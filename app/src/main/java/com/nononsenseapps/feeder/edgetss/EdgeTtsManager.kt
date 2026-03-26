package com.nononsenseapps.feeder.edgetss

import android.content.Context
import android.media.MediaPlayer
import android.os.Handler
import android.os.Looper
import android.os.SystemClock
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import java.io.File
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

sealed class TtsEvent {
    data class WordHighlight(val globalStart: Int, val globalEnd: Int, val progress: Float) : TtsEvent()
    object Done : TtsEvent()
    data class EngineError(val message: String) : TtsEvent()
}

class EdgeTtsManager(context: Context) {

    private val appContext = context.applicationContext
    private val engine = EdgeTtsEngine()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main)
    private val mainHandler = Handler(Looper.getMainLooper())

    private val _events = MutableSharedFlow<TtsEvent>(extraBufferCapacity = 64)
    val events: SharedFlow<TtsEvent> = _events.asSharedFlow()

    var voice: String = "en-US-AriaNeural"
    private var speed: Float = 1.0f

    private var speakJob: Job? = null
    private var currentPlayer: MediaPlayer? = null
    private var isPaused: Boolean = false

    private var pausePositionMs: Int = 0
    private var pendingBoundaries: List<WordBoundary> = emptyList()
    private var pendingChunkOffset: Int = 0
    private var pendingTotalLength: Int = 0
    private var highlightStartTime: Long = 0L

    fun speak(text: String) = speakFrom(text, 0)

    fun speakFrom(text: String, startCharOffset: Int) {
        stop()
        isPaused = false
        speakJob = scope.launch {
            runSpeechPipeline(text, startCharOffset)
        }
    }

    fun pause() {
        isPaused = true
        mainHandler.removeCallbacksAndMessages(null)
        currentPlayer?.let { mp ->
            if (mp.isPlaying) {
                pausePositionMs = mp.currentPosition
                mp.pause()
            }
        }
    }

    fun resume() {
        if (!isPaused) return
        isPaused = false
        currentPlayer?.let { mp ->
            rescheduleHighlights(mp.currentPosition)
            mp.start()
        }
    }

    fun stop() {
        isPaused = false
        speakJob?.cancel()
        speakJob = null
        mainHandler.removeCallbacksAndMessages(null)
        currentPlayer?.let {
            runCatching { it.stop(); it.release() }
        }
        currentPlayer = null
    }

    fun setSpeed(newSpeed: Float) {
        speed = newSpeed
    }

    fun shutdown() {
        stop()
        scope.launch { /* drain */ }.cancel()
    }

    // ── Pipeline ─────────────────────────────────────────────────────────────

    private suspend fun runSpeechPipeline(text: String, startCharOffset: Int = 0) {
        val sentences = splitSentences(text)
        val offsets   = buildOffsets(sentences)
        val total     = text.length

        val startIdx = offsets.indexOfLast { it <= startCharOffset }.coerceAtLeast(0)

        val channel = Channel<IndexedValue<TtsChunk>>(capacity = 2)

        val producer = scope.launch(Dispatchers.IO) {
            sentences.drop(startIdx).forEachIndexed { i, sentence ->
                if (!isActive) return@forEachIndexed
                runCatching {
                    val chunk = engine.synthesize(sentence, voice, speedToRate(speed))
                    channel.send(IndexedValue(startIdx + i, chunk))
                }.onFailure { e ->
                    _events.tryEmit(TtsEvent.EngineError(e.message ?: "Synthesis failed"))
                }
            }
            channel.close()
        }

        try {
            for (indexed in channel) {
                if (!currentCoroutineContext().isActive) break
                val (i, chunk) = indexed
                if (chunk.audioBytes.isEmpty()) continue
                val file = withContext(Dispatchers.IO) { writeTempMp3(chunk.audioBytes) }
                playChunk(file, chunk.wordBoundaries, offsets[i], total)
            }
            if (currentCoroutineContext().isActive) _events.emit(TtsEvent.Done)
        } finally {
            producer.cancel()
        }
    }

    private suspend fun playChunk(
        file: File,
        boundaries: List<WordBoundary>,
        chunkOffset: Int,
        totalLength: Int,
    ) {
        val mp = MediaPlayer()
        currentPlayer = mp
        pendingBoundaries  = boundaries
        pendingChunkOffset = chunkOffset
        pendingTotalLength = totalLength

        try {
            mp.setDataSource(file.absolutePath)
            mp.prepare()

            highlightStartTime = SystemClock.elapsedRealtime()
            scheduleHighlights(boundaries, chunkOffset, totalLength, 0)

            suspendCancellableCoroutine<Unit> { cont ->
                mp.setOnCompletionListener { cont.resume(Unit) }
                mp.setOnErrorListener { _, _, _ ->
                    cont.resumeWithException(Exception("MediaPlayer error"))
                    true
                }
                mp.start()
                cont.invokeOnCancellation {
                    mainHandler.removeCallbacksAndMessages(null)
                    runCatching { mp.stop() }
                }
            }
        } finally {
            runCatching { mp.release() }
            currentPlayer = null
            mainHandler.removeCallbacksAndMessages(null)
            withContext(Dispatchers.IO) { file.delete() }
        }
    }

    private fun scheduleHighlights(
        boundaries: List<WordBoundary>,
        chunkOffset: Int,
        totalLength: Int,
        skipMs: Int,
    ) {
        mainHandler.removeCallbacksAndMessages(null)
        val baseTime = SystemClock.elapsedRealtime() - skipMs
        boundaries.forEach { b ->
            val fireMs = b.offsetTicks / 10_000L
            if (fireMs < skipMs) return@forEach
            val delay = (baseTime + fireMs) - SystemClock.elapsedRealtime()
            mainHandler.postDelayed({
                val globalStart = chunkOffset + b.textOffset
                val globalEnd   = globalStart + b.text.length
                val progress    = if (totalLength > 0) globalStart.toFloat() / totalLength else 0f
                _events.tryEmit(TtsEvent.WordHighlight(globalStart, globalEnd, progress))
            }, delay.coerceAtLeast(0))
        }
    }

    private fun rescheduleHighlights(currentPositionMs: Int) {
        scheduleHighlights(pendingBoundaries, pendingChunkOffset, pendingTotalLength, currentPositionMs)
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun writeTempMp3(bytes: ByteArray): File {
        val file = File(appContext.cacheDir, "edgetss_${System.nanoTime()}.mp3")
        file.writeBytes(bytes)
        return file
    }

    private fun splitSentences(text: String, maxLen: Int = 300): List<String> {
        val result = mutableListOf<String>()
        val current = StringBuilder()

        fun flush() {
            val s = current.toString().trim()
            if (s.isNotBlank()) result.add(s)
            current.clear()
        }

        text.split("\n").filter { it.isNotBlank() }.forEach { para ->
            para.split(Regex("(?<=[.!?।॥])\\s+")).forEach { sentence ->
                if (current.isNotEmpty() && current.length + sentence.length + 1 > maxLen) flush()
                if (current.isNotEmpty()) current.append(" ")
                current.append(sentence)
            }
            flush()
        }
        flush()
        return result.ifEmpty { listOf(text.take(maxLen)) }
    }

    private fun buildOffsets(sentences: List<String>): List<Int> {
        val offsets = mutableListOf<Int>()
        var pos = 0
        for (s in sentences) { offsets.add(pos); pos += s.length + 1 }
        return offsets
    }

    private fun speedToRate(s: Float) = when {
        s <= 0.75f -> "-25%"
        s <= 1.0f  -> "+0%"
        s <= 1.25f -> "+25%"
        s <= 1.5f  -> "+50%"
        else       -> "+100%"
    }
}
