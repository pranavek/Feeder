package com.nononsenseapps.feeder.edgetss

import kotlinx.coroutines.suspendCancellableCoroutine
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import org.json.JSONObject
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.TimeZone
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

data class WordBoundary(
    val text: String,
    val offsetTicks: Long,    // 100-nanosecond ticks from start of audio
    val durationTicks: Long,
    val textOffset: Int = 0,  // char offset within the sentence (filled in locally)
)

data class TtsChunk(
    val audioBytes: ByteArray,
    val wordBoundaries: List<WordBoundary>,
)

class EdgeTtsEngine {

    private val client = OkHttpClient.Builder()
        .readTimeout(0, TimeUnit.MILLISECONDS)   // no read timeout — stream stays open
        .connectTimeout(15, TimeUnit.SECONDS)
        .build()

    suspend fun synthesize(
        sentenceText: String,
        voice: String = "en-US-AriaNeural",
        rate: String = "+0%",
        pitch: String = "+0Hz",
    ): TtsChunk = suspendCancellableCoroutine { cont ->

        val connectionId = uuid()
        val requestId   = uuid()
        val timestamp   = nowIso()
        val secMsGec    = computeSecMsGec()
        val muid        = UUID.randomUUID().toString().replace("-", "").uppercase()

        val wsUrl = "wss://speech.platform.bing.com/consumer/speech/synthesize/readaloud/edge/v1" +
            "?TrustedClientToken=6A5AA1D4EAFF4E9FB37E23D68491D6F4" +
            "&ConnectionId=$connectionId" +
            "&Sec-MS-GEC=$secMsGec" +
            "&Sec-MS-GEC-Version=$CHROMIUM_VERSION"

        val request = Request.Builder()
            .url(wsUrl)
            .header("Origin", "chrome-extension://jdiccldimpdaibmpdkjnbmckianbfold")
            .header(
                "User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
            )
            .header("Pragma", "no-cache")
            .header("Cache-Control", "no-cache")
            .header("Cookie", "muid=$muid;")
            .build()

        val audioChunks  = mutableListOf<ByteArray>()
        val rawBoundaries = mutableListOf<WordBoundary>()

        var webSocket: WebSocket? = null
        webSocket = client.newWebSocket(request, object : WebSocketListener() {

            override fun onOpen(ws: WebSocket, response: Response) {
                ws.send(buildSpeechConfig(timestamp))
                ws.send(buildSsml(requestId, timestamp, sentenceText, voice, rate, pitch))
            }

            override fun onMessage(ws: WebSocket, msg: String) {
                when {
                    "Path:turn.end" in msg -> {
                        val audio = audioChunks.fold(ByteArray(0)) { acc, b -> acc + b }
                        val boundaries = assignTextOffsets(rawBoundaries, sentenceText)
                        ws.close(1000, null)
                        if (!cont.isCompleted) cont.resume(TtsChunk(audio, boundaries))
                    }
                    "Path:audio.metadata" in msg -> {
                        parseWordBoundaries(msg)?.let { rawBoundaries.addAll(it) }
                    }
                }
            }

            override fun onMessage(ws: WebSocket, bytes: ByteString) {
                // Binary frame: [2-byte header length][header][mp3 audio]
                val data = bytes.toByteArray()
                if (data.size < 2) return
                val headerLen = ((data[0].toInt() and 0xFF) shl 8) or (data[1].toInt() and 0xFF)
                val audioStart = headerLen + 2
                if (audioStart < data.size) {
                    audioChunks.add(data.copyOfRange(audioStart, data.size))
                }
            }

            override fun onFailure(ws: WebSocket, t: Throwable, response: Response?) {
                if (!cont.isCompleted) cont.resumeWithException(
                    Exception("Edge TTS error: ${t.message}"),
                )
            }
        })

        cont.invokeOnCancellation { webSocket?.cancel() }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private fun buildSpeechConfig(timestamp: String) =
        "X-Timestamp:$timestamp\r\n" +
            "Content-Type:application/json; charset=utf-8\r\n" +
            "Path:speech.config\r\n\r\n" +
            """{"context":{"synthesis":{"audio":{"metadataoptions":{"sentenceBoundaryEnabled":"false","wordBoundaryEnabled":"true"},"outputFormat":"audio-24khz-48kbitrate-mono-mp3"}}}}"""

    private fun buildSsml(
        requestId: String,
        timestamp: String,
        text: String,
        voice: String,
        rate: String,
        pitch: String,
    ): String {
        val escaped = text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\"", "&quot;")
            .replace("'", "&apos;")
        val xmlLang = voice.split("-").take(2).joinToString("-").ifEmpty { "en-US" }
        return "X-RequestId:$requestId\r\n" +
            "Content-Type:application/ssml+xml\r\n" +
            "X-Timestamp:$timestamp\r\n" +
            "Path:ssml\r\n\r\n" +
            "<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' xml:lang='$xmlLang'>" +
            "<voice name='$voice'><prosody rate='$rate' pitch='$pitch'>$escaped</prosody></voice>" +
            "</speak>"
    }

    private fun parseWordBoundaries(message: String): List<WordBoundary>? = runCatching {
        val jsonStr = message.substringAfter("\r\n\r\n")
        val metadata = JSONObject(jsonStr).getJSONArray("Metadata")
        (0 until metadata.length()).mapNotNull { i ->
            val item = metadata.getJSONObject(i)
            if (item.getString("Type") != "WordBoundary") return@mapNotNull null
            val data    = item.getJSONObject("Data")
            val textObj = data.getJSONObject("text")
            WordBoundary(
                text          = textObj.getString("Text"),
                offsetTicks   = data.getLong("Offset"),
                durationTicks = data.getLong("Duration"),
            )
        }
    }.getOrNull()

    private fun assignTextOffsets(
        boundaries: List<WordBoundary>,
        sentence: String,
    ): List<WordBoundary> {
        var searchFrom = 0
        return boundaries.map { b ->
            val idx = sentence.indexOf(b.text, searchFrom)
            if (idx >= 0) {
                searchFrom = idx + b.text.length
                b.copy(textOffset = idx)
            } else b
        }
    }

    private fun computeSecMsGec(): String {
        val winTicks = System.currentTimeMillis() * 10_000L + 116_444_736_000_000_000L
        val rounded = winTicks - (winTicks % 3_000_000_000L)
        val input = "${rounded}6A5AA1D4EAFF4E9FB37E23D68491D6F4"
        return MessageDigest.getInstance("SHA-256")
            .digest(input.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02X".format(it) }
    }

    private fun uuid() = UUID.randomUUID().toString().replace("-", "").uppercase()

    private fun nowIso(): String {
        val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US)
        sdf.timeZone = TimeZone.getTimeZone("UTC")
        return sdf.format(Date())
    }

    companion object {
        private const val CHROMIUM_VERSION = "1-143.0.3650.75"
    }
}
