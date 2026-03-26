package com.nononsenseapps.feeder.edgetss

import android.content.Context
import android.os.Build
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
            // User explicitly chose a locale — always respect it
            langOverride is ForcedLocale -> voiceForLocale(langOverride.locale)

            useDetectLanguage || langOverride is AppSetting || langOverride is ForcedAuto -> {
                // 1st: Unicode script counting — offline, 100% accurate for Indic scripts.
                //      Many Indian sites declare lang="en" even for vernacular content, so
                //      script detection is more reliable than the HTML lang attribute.
                val scriptLang = detectLangFromScript(currentText)
                if (scriptLang != null) {
                    LANG_DEFAULT_VOICE[scriptLang] ?: DEFAULT_VOICE
                } else if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                    // 2nd: Android TextClassifier — distinguishes Latin-script languages
                    //      (English vs French vs Spanish etc.) where Unicode is ambiguous.
                    context.detectLocaleFromText(currentText).firstOrNull()?.locale
                        ?.let { voiceForLocale(it) } ?: DEFAULT_VOICE
                } else {
                    DEFAULT_VOICE
                }
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

    /**
     * Count Unicode code points per Indic script in the first 500 chars.
     * Each Indic script occupies a unique Unicode block, making this 100% accurate
     * and fully offline — no API or model required.
     * Returns the 2-letter BCP 47 language code (e.g. "ml", "hi") or null for Latin/other text.
     *
     * Ported from the readoutloud project's ReadabilityExtractor.detectScriptLang().
     */
    private fun detectLangFromScript(text: String): String? {
        val sample = text.take(500)
        val counts = mapOf(
            "ml" to sample.count { it in '\u0D00'..'\u0D7F' },  // Malayalam
            "hi" to sample.count { it in '\u0900'..'\u097F' },  // Devanagari (Hindi/Marathi)
            "ta" to sample.count { it in '\u0B80'..'\u0BFF' },  // Tamil
            "te" to sample.count { it in '\u0C00'..'\u0C7F' },  // Telugu
            "kn" to sample.count { it in '\u0C80'..'\u0CFF' },  // Kannada
        )
        val best = counts.maxByOrNull { it.value } ?: return null
        return if (best.value > 10) best.key else null
    }

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

        /** 2-letter lang code → best default voice (used by Unicode script detection path) */
        private val LANG_DEFAULT_VOICE: Map<String, String> = mapOf(
            "ml" to "ml-IN-SobhanaNeural",
            "hi" to "hi-IN-SwaraNeural",
            "ta" to "ta-IN-PallaviNeural",
            "te" to "te-IN-ShrutiNeural",
            "kn" to "kn-IN-SapnaNeural",
        )

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
