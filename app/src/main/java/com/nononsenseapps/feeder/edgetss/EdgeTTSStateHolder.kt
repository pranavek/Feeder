package com.nononsenseapps.feeder.edgetss

import android.content.Context
import androidx.compose.ui.text.AnnotatedString
import com.nononsenseapps.feeder.model.AppSetting
import com.nononsenseapps.feeder.model.ForcedAuto
import com.nononsenseapps.feeder.model.ForcedLocale
import com.nononsenseapps.feeder.model.LocaleOverride
import com.nononsenseapps.feeder.model.PlaybackStatus
import com.nononsenseapps.feeder.model.TTSStateHolder
import com.nononsenseapps.feeder.model.detectLocaleFromText
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import java.util.Locale

/**
 * Drop-in replacement for [TTSStateHolder] that uses Microsoft Edge TTS via WebSocket
 * instead of Android's native TextToSpeech API.
 *
 * All new logic is isolated in the [edgetss] package. The only upstream changes required
 * are adding `open` to [TTSStateHolder] and its three state-flow properties.
 */
class EdgeTTSStateHolder(
    context: Context,
    coroutineScope: CoroutineScope,
) : TTSStateHolder(context, coroutineScope) {

    private val ttsManager = EdgeTtsManager(context)

    // ── Overridden state flows ────────────────────────────────────────────────

    private val _edgeTtsState = MutableStateFlow(PlaybackStatus.STOPPED)
    override val ttsState: StateFlow<PlaybackStatus> = _edgeTtsState.asStateFlow()

    private val _edgeLanguage = MutableStateFlow<LocaleOverride>(AppSetting)
    override val language: StateFlow<LocaleOverride> = _edgeLanguage.asStateFlow()

    private val _edgeAvailableLanguages = MutableStateFlow<List<Locale>>(
        EDGE_VOICE_MAP.keys.map { Locale.forLanguageTag(it) },
    )
    override val availableLanguages: StateFlow<List<Locale>> = _edgeAvailableLanguages.asStateFlow()

    // ── Internal state ────────────────────────────────────────────────────────

    private var currentText: String = ""

    init {
        // Observe Edge TTS events to keep playback status in sync
        coroutineScope.launch {
            ttsManager.events.collect { event ->
                when (event) {
                    is TtsEvent.Done -> _edgeTtsState.value = PlaybackStatus.STOPPED
                    is TtsEvent.EngineError -> _edgeTtsState.value = PlaybackStatus.STOPPED
                    else -> { /* WordHighlight events ignored */ }
                }
            }
        }
    }

    // ── Public API overrides ──────────────────────────────────────────────────

    override fun tts(
        textArray: List<AnnotatedString>,
        useDetectLanguage: Boolean,
    ) {
        currentText = textArray.joinToString("\n") { it.text }

        // Select voice based on language preference
        val langOverride = _edgeLanguage.value
        ttsManager.voice = when {
            langOverride is ForcedLocale -> voiceForLocale(langOverride.locale)
            useDetectLanguage || langOverride is AppSetting || langOverride is ForcedAuto -> {
                val detected = context.detectLocaleFromText(currentText).firstOrNull()?.locale
                detected?.let { voiceForLocale(it) } ?: DEFAULT_VOICE
            }
            else -> DEFAULT_VOICE
        }

        _edgeTtsState.value = PlaybackStatus.PLAYING
        ttsManager.speak(currentText)
    }

    override fun pause() {
        ttsManager.pause()
        _edgeTtsState.update { if (it == PlaybackStatus.PLAYING) PlaybackStatus.PAUSED else it }
    }

    override fun stop() {
        ttsManager.stop()
        _edgeTtsState.value = PlaybackStatus.STOPPED
    }

    override fun skipNext() {
        // Stop current playback; user can tap "Read aloud" again to restart
        ttsManager.stop()
        _edgeTtsState.value = PlaybackStatus.STOPPED
    }

    override fun setLanguage(lang: LocaleOverride) {
        _edgeLanguage.value = lang
        if (lang is ForcedLocale) {
            ttsManager.voice = voiceForLocale(lang.locale)
        }
    }

    override fun shutdown() {
        ttsManager.shutdown()
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun voiceForLocale(locale: Locale): String {
        val lang = locale.language    // e.g. "en", "ml"
        val region = locale.country   // e.g. "US", "IN"
        val fullTag = if (region.isNotEmpty()) "$lang-$region" else lang
        // Exact match first, then any voice for that language
        return EDGE_VOICE_MAP[fullTag]
            ?: EDGE_VOICE_MAP.entries.firstOrNull { (k, _) -> k.startsWith("$lang-") }?.value
            ?: DEFAULT_VOICE
    }

    companion object {
        private const val DEFAULT_VOICE = "en-US-MichelleNeural"

        /** Locale tag → default Edge TTS voice name */
        val EDGE_VOICE_MAP: LinkedHashMap<String, String> = linkedMapOf(
            "en-US" to "en-US-MichelleNeural",
            "en-GB" to "en-GB-SoniaNeural",
            "en-AU" to "en-AU-NatashaNeural",
            "en-IN" to "en-IN-NeerjaNeural",
            "ml-IN" to "ml-IN-SobhanaNeural",
            "hi-IN" to "hi-IN-SwaraNeural",
            "ta-IN" to "ta-IN-PallaviNeural",
            "te-IN" to "te-IN-ShrutiNeural",
            "kn-IN" to "kn-IN-SapnaNeural",
        )
    }
}
