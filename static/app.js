/* =========================================================
   LocalAI Chat — app.js
   No external dependencies. Uses fetch + ReadableStream for
   POST-based SSE (EventSource only supports GET).
   ========================================================= */

(function () {
  "use strict";

  // Detect popup context EARLY to prevent MSAL popup blocking
  const isInPopup = window.opener !== null && window.opener !== undefined;
  const isChildWindow = window.self !== window.top;
  console.log("[Boot] Popup detection: isInPopup=", isInPopup, "isChildWindow=", isChildWindow);

  // ---- State -------------------------------------------------------
  const state = {
    conversationId: null,
    /** messages displayed in the current session (for saving) */
    messages: [],
    abortController: null,
    streaming: false,
    authEnabled: false,
    authReady: false,
    msalApp: null,
    account: null,
    accessToken: null,
    apiScope: null,
  };

  // ---- DOM refs ----------------------------------------------------
  const messagesEl   = document.getElementById("messages");
  const promptInput  = document.getElementById("prompt-input");
  const btnSend      = document.getElementById("btn-send");
  const btnMic       = document.getElementById("btn-mic");
  const btnVizFullscreen = document.getElementById("btn-viz-fullscreen");
  const btnTts       = document.getElementById("btn-tts");
  const btnVoicePreview = document.getElementById("btn-voice-preview");
  const sidebarPulse = document.getElementById("sidebar-pulse");
  const fullscreenViz = document.getElementById("viz-fullscreen");
  const fullscreenPulse = document.getElementById("fullscreen-pulse");
  const voiceStartGate = document.getElementById("voice-start-gate");
  const btnVoiceStart = document.getElementById("btn-voice-start");
  const btnCancel    = document.getElementById("btn-cancel");
  const btnNewChat   = document.getElementById("btn-new-chat");
  const btnSaveConv  = document.getElementById("btn-save-conv");
  const convList     = document.getElementById("conv-list");
  const statusLlama  = document.getElementById("status-llama");
  const statusMcp    = document.getElementById("status-mcp");
  const authBar      = document.getElementById("auth-bar");
  const authUser     = document.getElementById("auth-user");
  const btnLogin     = document.getElementById("btn-login");
  const btnLogout    = document.getElementById("btn-logout");

  const toolValvesController = window.createToolValvesController
    ? window.createToolValvesController({ apiFetch })
    : null;

  // Settings inputs
  const sTemperature  = document.getElementById("s-temperature");
  const sTopP         = document.getElementById("s-top_p");
  const sMaxTokens    = document.getElementById("s-max_tokens");
  const sSystemPrompt = document.getElementById("s-system_prompt");
  const sVoice        = document.getElementById("s-voice");
  const sVoiceRate    = document.getElementById("s-voice-rate");
  const sVoicePitch   = document.getElementById("s-voice-pitch");
  const sReactiveOrb  = document.getElementById("s-reactive-orb");
  const sVizFullscreenDefault = document.getElementById("s-viz-fullscreen-default");

  const ui = {
    preferFullscreenViz: false,
    voiceStartRequired: false,
  };

  const VOICE_START_KEY = "chat_voice_started_once";

  // Speech recognition (browser-native, phase 1)
  const SpeechRecognitionCtor = window.SpeechRecognition || window.webkitSpeechRecognition;
  const speech = {
    supported: Boolean(SpeechRecognitionCtor),
    recognition: null,
    listening: false,
    userStopped: false,
    blockedReason: "",
    probePending: false,
    lastProbeResult: "",
    awaitingWake: true,
    monitoringTts: false,
    pausedForTts: false,
    resumeAfterTts: false,
    baseText: "",
    interimText: "",
    wakeUser: "",
    keepArmed: false,
    lastWakeGreeting: "",
    wakeGreetingUntil: 0,
  };

  const tts = {
    supported: typeof window !== "undefined" && "speechSynthesis" in window,
    enabled: false,
    voiceURI: "",
    rate: 0.95,
    pitch: 1.0,
    micReactiveOrb: true,
    speaking: false,
    pendingUtterances: 0,
    boundaryPulseTimer: null,
    streamBuffer: "",
    gestureUnlocked: false,
    pendingWakeGreeting: "",
    unlockListenersInstalled: false,
  };

  const audioMonitor = {
    stream: null,
    context: null,
    source: null,
    analyser: null,
    data: null,
    rafId: null,
    active: false,
    smoothed: 0,
  };

  // ---- Persistence helpers (localStorage) --------------------------
  function loadSettings() {
    try {
      const saved = JSON.parse(localStorage.getItem("chat_settings") || "{}");
      if (saved.temperature  !== undefined) sTemperature.value  = saved.temperature;
      if (saved.top_p        !== undefined) sTopP.value         = saved.top_p;
      if (saved.max_tokens   !== undefined) sMaxTokens.value    = saved.max_tokens;
      if (saved.system_prompt !== undefined) sSystemPrompt.value = saved.system_prompt;
      if (typeof saved.open_viz_fullscreen === "boolean") {
        ui.preferFullscreenViz = saved.open_viz_fullscreen;
      }
    } catch (_) {}
    if (sVizFullscreenDefault) {
      sVizFullscreenDefault.checked = ui.preferFullscreenViz;
    }
  }

  function saveSettings() {
    localStorage.setItem("chat_settings", JSON.stringify({
      temperature:   parseFloat(sTemperature.value),
      top_p:         parseFloat(sTopP.value),
      max_tokens:    parseInt(sMaxTokens.value, 10),
      system_prompt: sSystemPrompt.value,
      open_viz_fullscreen: ui.preferFullscreenViz,
    }));
  }

  [sTemperature, sTopP, sMaxTokens, sSystemPrompt].forEach(el =>
    el.addEventListener("change", saveSettings)
  );

  function setPulseIntensity(value) {
    if (!sidebarPulse && !fullscreenPulse) return;
    const clamped = Math.max(0, Math.min(1, Number(value) || 0));
    [sidebarPulse, fullscreenPulse].forEach((pulseEl) => {
      if (!pulseEl) return;
      pulseEl.style.setProperty("--pulse-intensity", clamped.toFixed(3));
    });
  }

  function updateVizFullscreenButtonState() {
    if (!btnVizFullscreen) return;
    const active = Boolean(fullscreenViz && fullscreenViz.classList.contains("active"));
    btnVizFullscreen.textContent = active ? "Exit Visual" : "Full Screen Visual";
    btnVizFullscreen.setAttribute("aria-pressed", active ? "true" : "false");
    btnVizFullscreen.title = active
      ? "Exit full screen visualization"
      : "Open full screen visualization";
  }

  function hasVoiceStartedOnce() {
    try {
      return localStorage.getItem(VOICE_START_KEY) === "true";
    } catch (_) {
      return false;
    }
  }

  function setVoiceStartedOnce() {
    try {
      localStorage.setItem(VOICE_START_KEY, "true");
    } catch (_) {}
  }

  function showVoiceStartGate() {
    if (!voiceStartGate) return;
    voiceStartGate.classList.remove("hidden");
    voiceStartGate.setAttribute("aria-hidden", "false");
    if (btnVoiceStart) {
      setTimeout(() => {
        btnVoiceStart.focus();
      }, 30);
    }
  }

  function hideVoiceStartGate() {
    if (!voiceStartGate) return;
    voiceStartGate.classList.add("hidden");
    voiceStartGate.setAttribute("aria-hidden", "true");
  }

  function completeVoiceStart(source = "voice_start") {
    markTtsGestureUnlocked(source);
    tts.enabled = true;
    localStorage.setItem("chat_tts_enabled", "true");
    updateTtsButtonState();
    setVoiceStartedOnce();
    ui.voiceStartRequired = false;
    hideVoiceStartGate();

    speech.userStopped = false;
    speech.interimText = "";
    speech.baseText = "";
    startSpeechRecognition();
    updateSidebarPulse();
    logSpeechDebug("voice_start_completed", {
      text: source,
      final: true,
    });
  }

  function updatePulseElementState(pulseEl) {
    if (!pulseEl) return;
    pulseEl.classList.remove("pulse-idle", "pulse-listening", "pulse-thinking", "pulse-speaking");

    if (state.streaming) {
      pulseEl.classList.add("pulse-thinking");
      return;
    }

    if (tts.speaking) {
      pulseEl.classList.add("pulse-speaking");
      return;
    }

    if (speech.listening || speech.monitoringTts) {
      pulseEl.classList.add("pulse-listening");
      return;
    }

    pulseEl.classList.add("pulse-idle");
  }

  async function enterVizFullscreen(options = {}) {
    const allowOverlayFallback = options.allowOverlayFallback !== false;
    if (!fullscreenViz) return;
    fullscreenViz.classList.add("active");
    fullscreenViz.setAttribute("aria-hidden", "false");
    updateSidebarPulse();
    updateVizFullscreenButtonState();

    if (document.fullscreenElement === fullscreenViz) return;
    if (!fullscreenViz.requestFullscreen) return;

    try {
      await fullscreenViz.requestFullscreen();
    } catch (err) {
      console.warn("[Viz] Unable to enter fullscreen:", err);
      if (!allowOverlayFallback) {
        fullscreenViz.classList.remove("active");
        fullscreenViz.setAttribute("aria-hidden", "true");
      }
      updateVizFullscreenButtonState();
    }
  }

  async function exitVizFullscreen() {
    if (!fullscreenViz) return;
    if (document.fullscreenElement === fullscreenViz && document.exitFullscreen) {
      try {
        await document.exitFullscreen();
      } catch (err) {
        console.warn("[Viz] Unable to exit fullscreen:", err);
      }
    }
    fullscreenViz.classList.remove("active");
    setTimeout(() => {
      if (!fullscreenViz.classList.contains("active")) {
        fullscreenViz.setAttribute("aria-hidden", "true");
      }
    }, 240);
    updateVizFullscreenButtonState();
  }

  function updateSidebarPulse() {
    if (!sidebarPulse && !fullscreenPulse) return;
    updatePulseElementState(sidebarPulse);
    updatePulseElementState(fullscreenPulse);
    if (!state.streaming && !tts.speaking && !speech.listening && !speech.monitoringTts) {
      setPulseIntensity(0);
    }
  }

  function clearBoundaryPulse() {
    if (tts.boundaryPulseTimer) {
      clearTimeout(tts.boundaryPulseTimer);
      tts.boundaryPulseTimer = null;
    }
  }

  function pulseOnBoundary() {
    if (!tts.speaking || audioMonitor.active) return;
    const burst = 0.68 + (Math.random() * 0.32);
    setPulseIntensity(burst);
    clearBoundaryPulse();
    tts.boundaryPulseTimer = setTimeout(() => {
      if (tts.speaking && !audioMonitor.active) {
        setPulseIntensity(0.34);
      }
      tts.boundaryPulseTimer = null;
    }, 90);
  }

  function stopSpeechIntensityMonitor() {
    clearBoundaryPulse();

    if (audioMonitor.rafId !== null) {
      cancelAnimationFrame(audioMonitor.rafId);
      audioMonitor.rafId = null;
    }

    if (audioMonitor.stream) {
      for (const track of audioMonitor.stream.getTracks()) {
        track.stop();
      }
    }

    if (audioMonitor.context) {
      audioMonitor.context.close().catch(() => {});
    }

    audioMonitor.stream = null;
    audioMonitor.context = null;
    audioMonitor.source = null;
    audioMonitor.analyser = null;
    audioMonitor.data = null;
    audioMonitor.active = false;
    audioMonitor.smoothed = 0;

    speech.monitoringTts = false;
    setMicButtonState();
    if (tts.speaking) {
      setPulseIntensity(0.12);
    } else {
      setPulseIntensity(0);
    }
  }

  function pauseRecognitionForTts() {
    if (!speech.recognition) return;
    if (!speech.listening) {
      speech.pausedForTts = false;
      speech.resumeAfterTts = false;
      return;
    }

    speech.resumeAfterTts = true;
    speech.pausedForTts = true;
    speech.userStopped = true;
    logSpeechDebug("recognition_paused_for_tts", {
      text: "paused_for_tts",
      final: true,
    });

    try {
      speech.recognition.stop();
    } catch (_) {}
    setMicButtonState();
  }

  function resumeRecognitionAfterTts() {
    if (!speech.pausedForTts) return;

    const shouldResume = speech.resumeAfterTts;
    speech.pausedForTts = false;
    speech.resumeAfterTts = false;
    if (!shouldResume) return;

    speech.userStopped = false;
    speech.awaitingWake = !isEntraSignedIn();
    logSpeechDebug("recognition_resumed_after_tts", {
      text: "resumed_after_tts",
      final: true,
    });

    try {
      speech.recognition.start();
    } catch (_) {
      startSpeechRecognition();
    }
  }

  async function startSpeechIntensityMonitor() {
    if (!tts.micReactiveOrb) return;
    if (!tts.speaking || audioMonitor.active) return;
    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) return;

    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
        },
        video: false,
      });

      const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
      if (!AudioContextCtor) {
        for (const track of stream.getTracks()) {
          track.stop();
        }
        return;
      }

      const context = new AudioContextCtor();
      const source = context.createMediaStreamSource(stream);
      const analyser = context.createAnalyser();
      analyser.fftSize = 512;
      analyser.smoothingTimeConstant = 0.8;
      const data = new Uint8Array(analyser.fftSize);
      source.connect(analyser);

      audioMonitor.stream = stream;
      audioMonitor.context = context;
      audioMonitor.source = source;
      audioMonitor.analyser = analyser;
      audioMonitor.data = data;
      audioMonitor.active = true;

      speech.monitoringTts = true;
      setMicButtonState();

      const tick = () => {
        if (!audioMonitor.active || !audioMonitor.analyser || !audioMonitor.data) return;

        audioMonitor.analyser.getByteTimeDomainData(audioMonitor.data);
        let sum = 0;
        for (let i = 0; i < audioMonitor.data.length; i += 1) {
          const centered = (audioMonitor.data[i] - 128) / 128;
          sum += centered * centered;
        }

        const rms = Math.sqrt(sum / audioMonitor.data.length);
        const boosted = Math.max(0, Math.min(1, (rms - 0.01) * 16));
        audioMonitor.smoothed = (audioMonitor.smoothed * 0.56) + (boosted * 0.44);
        setPulseIntensity(audioMonitor.smoothed);

        audioMonitor.rafId = requestAnimationFrame(tick);
      };

      tick();
    } catch (err) {
      console.warn("[Speech] Mic monitor unavailable during TTS:", err);
      speech.monitoringTts = false;
      setMicButtonState();
    }
  }

  function updateTtsButtonState() {
    if (!btnTts) return;
    const canUse = tts.supported;
    btnTts.disabled = !canUse;
    btnTts.classList.toggle("enabled", tts.enabled);
    btnTts.setAttribute("aria-pressed", tts.enabled ? "true" : "false");
    btnTts.textContent = tts.enabled ? "Voice On" : "Voice Off";
    btnTts.title = canUse
      ? (tts.enabled ? "Disable read-aloud" : "Enable read-aloud")
      : "Voice output not supported in this browser";
  }

  function saveTtsSettings() {
    try {
      localStorage.setItem("chat_tts_settings", JSON.stringify({
        voiceURI: tts.voiceURI || "",
        rate: tts.rate,
        pitch: tts.pitch,
        micReactiveOrb: tts.micReactiveOrb,
      }));
    } catch (_) {}
  }

  function loadTtsSettings() {
    try {
      const saved = JSON.parse(localStorage.getItem("chat_tts_settings") || "{}");
      if (typeof saved.voiceURI === "string") tts.voiceURI = saved.voiceURI;
      if (typeof saved.rate === "number") tts.rate = Math.min(1.25, Math.max(0.7, saved.rate));
      if (typeof saved.pitch === "number") tts.pitch = Math.min(1.2, Math.max(0.8, saved.pitch));
      if (typeof saved.micReactiveOrb === "boolean") tts.micReactiveOrb = saved.micReactiveOrb;
    } catch (_) {}

    if (sVoiceRate) sVoiceRate.value = String(tts.rate);
    if (sVoicePitch) sVoicePitch.value = String(tts.pitch);
    if (sReactiveOrb) sReactiveOrb.checked = tts.micReactiveOrb;
  }

  function scoreVoiceQuality(voice, lang) {
    const name = String(voice?.name || "").toLowerCase();
    const vlang = String(voice?.lang || "").toLowerCase();
    const langLower = String(lang || "en-US").toLowerCase();
    const langBase = langLower.split("-")[0];

    let score = 0;
    if (vlang === langLower) score += 40;
    else if (vlang.startsWith(langBase)) score += 25;

    if (voice?.localService) score += 8;
    if (voice?.default) score += 4;

    if (/(neural|natural|wavenet|studio|enhanced|premium|siri)/.test(name)) score += 30;
    if (/(desktop|espeak|mbrola|compact)/.test(name)) score -= 25;

    return score;
  }

  function getPreferredVoice() {
    const lang = navigator.language || "en-US";
    const voices = window.speechSynthesis.getVoices() || [];
    if (!voices.length) return null;

    if (tts.voiceURI) {
      const selected = voices.find(v => v.voiceURI === tts.voiceURI);
      if (selected) return selected;
    }

    let best = voices[0];
    let bestScore = scoreVoiceQuality(best, lang);
    for (const voice of voices) {
      const score = scoreVoiceQuality(voice, lang);
      if (score > bestScore) {
        best = voice;
        bestScore = score;
      }
    }
    return best;
  }

  function getLocalPreferredVoice(excludeVoiceURI = "") {
    const lang = navigator.language || "en-US";
    const voices = window.speechSynthesis.getVoices() || [];
    const localVoices = voices.filter((v) => v.localService && v.voiceURI !== excludeVoiceURI);
    if (!localVoices.length) return null;

    let best = localVoices[0];
    let bestScore = scoreVoiceQuality(best, lang);
    for (const voice of localVoices) {
      const score = scoreVoiceQuality(voice, lang);
      if (score > bestScore) {
        best = voice;
        bestScore = score;
      }
    }
    return best;
  }

  function markTtsGestureUnlocked(source = "unknown") {
    if (!tts.supported) return;
    if (tts.gestureUnlocked) return;
    tts.gestureUnlocked = true;
    logSpeechDebug("tts_gesture_unlocked", {
      text: source,
      final: true,
    });

    if (tts.pendingWakeGreeting) {
      const queued = tts.pendingWakeGreeting;
      tts.pendingWakeGreeting = "";
      setTimeout(() => {
        speakWakeGreeting(queued, { fromGesture: true });
      }, 40);
    }
  }

  function installTtsGestureUnlockListeners() {
    if (!tts.supported || tts.unlockListenersInstalled) return;
    tts.unlockListenersInstalled = true;

    const handler = (event) => {
      const source = event && event.type ? String(event.type) : "gesture";
      markTtsGestureUnlocked(source);
      document.removeEventListener("pointerdown", handler, true);
      document.removeEventListener("keydown", handler, true);
      document.removeEventListener("touchstart", handler, true);
    };

    document.addEventListener("pointerdown", handler, true);
    document.addEventListener("keydown", handler, true);
    document.addEventListener("touchstart", handler, true);
  }

  function populateVoiceOptions() {
    if (!sVoice || !tts.supported) return;
    const voices = window.speechSynthesis.getVoices() || [];
    const preferred = getPreferredVoice();

    if (!tts.voiceURI && preferred) {
      tts.voiceURI = preferred.voiceURI;
    }

    sVoice.innerHTML = "";
    if (!voices.length) {
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = "Loading voices...";
      sVoice.appendChild(opt);
      sVoice.disabled = true;
      return;
    }

    sVoice.disabled = false;
    for (const voice of voices) {
      const opt = document.createElement("option");
      opt.value = voice.voiceURI;
      const tags = [];
      if (voice.default) tags.push("default");
      if (voice.localService) tags.push("local");
      const suffix = tags.length ? ` (${tags.join(", ")})` : "";
      opt.textContent = `${voice.name} - ${voice.lang}${suffix}`;
      sVoice.appendChild(opt);
    }

    if (tts.voiceURI && voices.some(v => v.voiceURI === tts.voiceURI)) {
      sVoice.value = tts.voiceURI;
    } else if (preferred) {
      tts.voiceURI = preferred.voiceURI;
      sVoice.value = preferred.voiceURI;
      saveTtsSettings();
    }
  }

  function splitForSpeech(text, maxChunkLength = 220) {
    const normalized = String(text || "").replace(/\s+/g, " ").trim();
    if (!normalized) return [];
    if (normalized.length <= maxChunkLength) return [normalized];

    const parts = [];
    let current = "";
    const sentences = normalized.split(/(?<=[.!?])\s+/);
    for (const sentence of sentences) {
      if (!sentence) continue;
      if (!current) {
        current = sentence;
        continue;
      }
      if ((current + " " + sentence).length <= maxChunkLength) {
        current += " " + sentence;
      } else {
        parts.push(current);
        current = sentence;
      }
    }
    if (current) parts.push(current);
    return parts;
  }

  function normalizeForSpeech(text) {
    return String(text || "")
      .replace(/^Image:\s*\/api\/files\/\S+$/gim, "")
      .replace(/!\[[^\]]*\]\(([^)]+)\)/g, "")
      .replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1")
      .replace(/`{1,3}([^`]+)`{1,3}/g, "$1")
      .replace(/\*\*([^*]+)\*\*/g, "$1")
      .replace(/__([^_]+)__/g, "$1")
      .replace(/\*([^*]+)\*/g, "$1")
      .replace(/_([^_]+)_/g, "$1")
      .replace(/~~([^~]+)~~/g, "$1")
      .replace(/^#+\s+/gm, "")
      .replace(/^>\s+/gm, "")
      .replace(/^[-*+]\s+/gm, "")
      .replace(/^\d+\.\s+/gm, "")
      .replace(/\s+/g, " ")
      .trim();
  }

  function cancelSpeechOutput() {
    if (!tts.supported) return;
    tts.pendingUtterances = 0;
    tts.speaking = false;
    tts.streamBuffer = "";
    stopSpeechIntensityMonitor();
    updateSidebarPulse();
    window.speechSynthesis.cancel();
    resumeRecognitionAfterTts();
  }

  function createSpeechUtterance(text, preferredVoice) {
    const lang = navigator.language || "en-US";
    const utterance = new SpeechSynthesisUtterance(text);
    utterance.lang = preferredVoice?.lang || lang;
    utterance.rate = tts.rate;
    utterance.pitch = tts.pitch;
    if (preferredVoice) utterance.voice = preferredVoice;

    utterance.onstart = () => {
      pauseRecognitionForTts();
      tts.speaking = true;
      setPulseIntensity(0.12);
      // Keep microphone off during assistant speech to avoid echo loops.
      stopSpeechIntensityMonitor();
      updateSidebarPulse();
    };

    utterance.onboundary = () => {
      pulseOnBoundary();
    };

    const markDone = () => {
      tts.pendingUtterances = Math.max(0, tts.pendingUtterances - 1);
      if (tts.pendingUtterances === 0) {
        tts.speaking = false;
        stopSpeechIntensityMonitor();
        resumeRecognitionAfterTts();
        updateSidebarPulse();
      }
    };
    utterance.onend = markDone;
    utterance.onerror = markDone;

    return utterance;
  }

  function queueSpeechChunk(chunkText, preferredVoice) {
    const chunk = normalizeForSpeech(chunkText);
    if (!chunk) return;
    tts.pendingUtterances += 1;
    window.speechSynthesis.speak(createSpeechUtterance(chunk, preferredVoice));
  }

  function drainSpeechStreamBuffer(preferredVoice, force = false) {
    if (!tts.streamBuffer) return;

    while (true) {
      const text = tts.streamBuffer;
      let cut = -1;

      const punctuationMatch = text.match(/^[\s\S]{50,}?[.!?](?:\s|$)/);
      if (punctuationMatch) {
        cut = punctuationMatch[0].length;
      }

      if (cut < 0 && text.length >= 180) {
        const lastSpace = text.lastIndexOf(" ", 180);
        cut = lastSpace >= 90 ? lastSpace : 180;
      }

      if (force && cut < 0 && text.trim()) {
        cut = text.length;
      }

      if (cut < 0) break;

      const chunk = text.slice(0, cut);
      tts.streamBuffer = text.slice(cut);
      queueSpeechChunk(chunk, preferredVoice);
    }
  }

  function enqueueStreamingSpeech(text) {
    if (!tts.supported || !tts.enabled) return;
    if (!text) return;

    tts.streamBuffer += text;
    drainSpeechStreamBuffer(getPreferredVoice(), false);
  }

  function flushStreamingSpeech() {
    if (!tts.supported || !tts.enabled) return;
    drainSpeechStreamBuffer(getPreferredVoice(), true);
  }

  function speakText(text) {
    if (!tts.supported || !tts.enabled) return;
    const message = normalizeForSpeech(text);
    if (!message) return;

    cancelSpeechOutput();
    const preferredVoice = getPreferredVoice();

    const chunks = splitForSpeech(message);
    if (!chunks.length) return;
    for (const chunk of chunks) {
      queueSpeechChunk(chunk, preferredVoice);
    }
  }

  function speakWakeGreeting(text, options = {}) {
    if (!tts.supported || !tts.enabled) {
      logSpeechDebug("wake_tts_skip", {
        text: !tts.supported ? "tts_not_supported" : "tts_disabled",
        final: true,
      });
      return;
    }
    const message = normalizeForSpeech(text);
    if (!message) {
      logSpeechDebug("wake_tts_skip", {
        text: "empty_wake_greeting",
        final: true,
      });
      return;
    }

    const fromGesture = Boolean(options.fromGesture);

    const synth = window.speechSynthesis;
    const initialVoice = getLocalPreferredVoice() || getPreferredVoice();

    function queueWakeUtterance(voice, attempt = 1) {
      const voiceName = voice?.name || "default";
      const voiceLang = voice?.lang || navigator.language || "en-US";

      // Reserve one slot so pulse state and monitor behavior stay consistent.
      tts.pendingUtterances = 1;
      const utterance = createSpeechUtterance(message, voice);

      const prevStart = utterance.onstart;
      const prevEnd = utterance.onend;
      const prevError = utterance.onerror;

      utterance.onstart = (ev) => {
        if (typeof prevStart === "function") prevStart(ev);
        logSpeechDebug("wake_tts_start", {
          text: message,
          final: true,
          extra: {
            attempt,
            voice: voiceName,
            lang: voiceLang,
            paused: Boolean(synth?.paused),
            speaking: Boolean(synth?.speaking),
            pending: Boolean(synth?.pending),
          },
        });
      };

      utterance.onend = (ev) => {
        if (typeof prevEnd === "function") prevEnd(ev);
        // After wake greeting, keep mic armed (ready for prompt) rather than
        // returning to wake-listening mode. keepArmed prevents recognition.onstart
        // from resetting awaitingWake back to true if it fires again.
        speech.keepArmed = true;
        speech.awaitingWake = false;
        setMicButtonState();
        logSpeechDebug("wake_tts_end", {
          text: message,
          final: true,
          extra: { attempt, voice: voiceName, lang: voiceLang },
        });
      };

      utterance.onerror = (ev) => {
        if (typeof prevError === "function") prevError(ev);
        const code = ev && ev.error ? String(ev.error) : "unknown";
        logSpeechDebug("wake_tts_error", {
          text: message,
          final: true,
          extra: {
            attempt,
            error: code,
            voice: voiceName,
            lang: voiceLang,
          },
        });

        // Browser policy can reject TTS until user activation occurs.
        if (code === "not-allowed") {
          if (!fromGesture) {
            tts.pendingWakeGreeting = message;
            logSpeechDebug("wake_tts_waiting_for_gesture", {
              text: "Wake greeting queued. Click anywhere once to allow voice output.",
              final: true,
              extra: {
                attempt,
                voice: voiceName,
                lang: voiceLang,
              },
            });
            return;
          }

          logSpeechDebug("wake_tts_blocked_after_gesture", {
            text: "Wake greeting still blocked after user gesture. Browser speech output is restricted.",
            final: true,
            extra: {
              attempt,
              voice: voiceName,
              lang: voiceLang,
            },
          });
        }
      };

      try {
        synth.speak(utterance);
        logSpeechDebug("wake_tts_queued", {
          text: message,
          final: true,
          extra: {
            attempt,
            voice: voiceName,
            lang: voiceLang,
            paused: Boolean(synth?.paused),
            speaking: Boolean(synth?.speaking),
            pending: Boolean(synth?.pending),
          },
        });
      } catch (err) {
        tts.pendingUtterances = 0;
        tts.speaking = false;
        stopSpeechIntensityMonitor();
        updateSidebarPulse();
        logSpeechDebug("wake_tts_error", {
          text: message,
          final: true,
          extra: {
            attempt,
            error: err && err.message ? String(err.message) : "speak_failed",
            voice: voiceName,
            lang: voiceLang,
          },
        });
      }
    }

    try {
      if (typeof synth.resume === "function") {
        synth.resume();
      }
      synth.cancel();
      setTimeout(() => {
        queueWakeUtterance(initialVoice, 1);
      }, 50);
    } catch (err) {
      tts.pendingUtterances = 0;
      tts.speaking = false;
      stopSpeechIntensityMonitor();
      updateSidebarPulse();
      logSpeechDebug("wake_tts_error", {
        text: message,
        final: true,
        extra: {
          error: err && err.message ? String(err.message) : "wake_tts_failed",
          voice: initialVoice?.name || "default",
          lang: initialVoice?.lang || navigator.language || "en-US",
        },
      });
    }
  }

  function initTextToSpeech() {
    if (!btnTts) return;

    loadTtsSettings();

    try {
      tts.enabled = localStorage.getItem("chat_tts_enabled") === "true";
    } catch (_) {
      tts.enabled = false;
    }

    if (!tts.supported) {
      btnTts.style.display = "none";
      return;
    }

    installTtsGestureUnlockListeners();

    btnTts.addEventListener("click", () => {
      markTtsGestureUnlocked("voice_button_click");
      tts.enabled = !tts.enabled;
      if (!tts.enabled) {
        cancelSpeechOutput();
      }
      localStorage.setItem("chat_tts_enabled", tts.enabled ? "true" : "false");
      updateTtsButtonState();
      updateSidebarPulse();
    });

    if (sVoice) {
      sVoice.addEventListener("change", () => {
        tts.voiceURI = sVoice.value || "";
        saveTtsSettings();
      });
    }

    if (sVoiceRate) {
      sVoiceRate.addEventListener("change", () => {
        const n = parseFloat(sVoiceRate.value);
        if (!Number.isNaN(n)) {
          tts.rate = Math.min(1.25, Math.max(0.7, n));
          sVoiceRate.value = String(tts.rate);
          saveTtsSettings();
        }
      });
    }

    if (sVoicePitch) {
      sVoicePitch.addEventListener("change", () => {
        const n = parseFloat(sVoicePitch.value);
        if (!Number.isNaN(n)) {
          tts.pitch = Math.min(1.2, Math.max(0.8, n));
          sVoicePitch.value = String(tts.pitch);
          saveTtsSettings();
        }
      });
    }

    if (sReactiveOrb) {
      sReactiveOrb.addEventListener("change", () => {
        tts.micReactiveOrb = Boolean(sReactiveOrb.checked);
        if (!tts.micReactiveOrb) {
          stopSpeechIntensityMonitor();
        } else if (tts.speaking) {
          startSpeechIntensityMonitor();
        }
        saveTtsSettings();
      });
    }

    if (btnVoicePreview) {
      btnVoicePreview.addEventListener("click", () => {
        markTtsGestureUnlocked("voice_preview_click");
        const wasEnabled = tts.enabled;
        tts.enabled = true;
        speakText("Voice preview. This is how assistant replies will sound.");
        tts.enabled = wasEnabled;
        updateSidebarPulse();
      });
    }

    window.speechSynthesis.onvoiceschanged = () => {
      populateVoiceOptions();
      updateTtsButtonState();
    };

    populateVoiceOptions();
    updateTtsButtonState();
    updateSidebarPulse();
  }

  function appendTranscript(base, addition) {
    const left = String(base || "");
    const right = String(addition || "").trim();
    if (!right) return left;
    if (!left) return right;
    const needsSpace = !/\s$/.test(left) && !/^[,.;:!?\s]/.test(right);
    return left + (needsSpace ? " " : "") + right;
  }

  function trimTranscriptWindow(text, maxWords = 12) {
    const words = String(text || "").trim().split(/\s+/).filter(Boolean);
    if (words.length <= maxWords) return words.join(" ");
    return words.slice(-maxWords).join(" ");
  }

  function normalizeSpeechText(text) {
    return String(text || "")
      .toLowerCase()
      .replace(/[.,!?;:]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function logSpeechDebug(event, details = {}) {
    const text = String(details.text || "").slice(0, 4096);
    if (!text) return;

    const payload = {
      event,
      text,
      final: details.final ?? null,
      listening: speech.listening,
      awaiting_wake: speech.awaitingWake,
      auth_mode: isEntraSignedIn() ? "entra" : "wake",
      conversation_id: state.conversationId,
      extra: details.extra || null,
    };

    apiFetch("/api/debug/speech-log", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }).catch((err) => {
      console.warn("[Speech] Debug log write failed:", err);
    });
  }

  async function probeSpeechMicrophone() {
    if (speech.probePending) return;
    speech.probePending = true;

    let stream = null;
    let context = null;
    try {
      if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
        logSpeechDebug("mic_probe_error", {
          text: "getusermedia_unavailable",
        });
        return;
      }

      const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
      if (!AudioContextCtor) {
        logSpeechDebug("mic_probe_error", {
          text: "audio_context_unavailable",
        });
        return;
      }

      stream = await navigator.mediaDevices.getUserMedia({
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
        },
        video: false,
      });

      context = new AudioContextCtor();
      const source = context.createMediaStreamSource(stream);
      const analyser = context.createAnalyser();
      analyser.fftSize = 512;
      const data = new Uint8Array(analyser.fftSize);
      source.connect(analyser);

      await new Promise((resolve) => setTimeout(resolve, 220));

      analyser.getByteTimeDomainData(data);
      let sum = 0;
      for (let i = 0; i < data.length; i += 1) {
        const centered = (data[i] - 128) / 128;
        sum += centered * centered;
      }

      const rms = Math.sqrt(sum / data.length);
      const signalDetected = rms > 0.02;
      speech.lastProbeResult = signalDetected ? `signal:${rms.toFixed(4)}` : `silent:${rms.toFixed(4)}`;
      logSpeechDebug(signalDetected ? "mic_probe_signal" : "mic_probe_silent", {
        text: signalDetected ? `rms:${rms.toFixed(4)}` : `silent:${rms.toFixed(4)}`,
        extra: { rms: Number(rms.toFixed(4)) },
      });
    } catch (err) {
      speech.lastProbeResult = err && err.name ? `error:${String(err.name)}` : "error:mic_probe_failed";
      logSpeechDebug("mic_probe_error", {
        text: err && err.name ? String(err.name) : "mic_probe_failed",
        extra: { message: err && err.message ? String(err.message) : "unknown" },
      });
    } finally {
      if (stream) {
        for (const track of stream.getTracks()) {
          track.stop();
        }
      }
      if (context) {
        context.close().catch(() => {});
      }
      speech.probePending = false;
    }
  }

  function getWakeUserCandidates() {
    const set = new Set();
    const displayName = state.account ? getDisplayName() : "";
    const username = state.account?.username || "";
    if (displayName) {
      set.add(displayName);
      const firstName = displayName.split(/\s+/)[0];
      if (firstName) set.add(firstName);
    }
    if (username) {
      const localPart = username.split("@")[0];
      if (localPart) {
        set.add(localPart);
        set.add(localPart.replace(/[._]/g, " "));
      }
    }
    if (speech.wakeUser) set.add(speech.wakeUser);
    return Array.from(set).map(v => normalizeSpeechText(v)).filter(Boolean);
  }

  function extractWakePayload(transcript) {
    const raw = String(transcript || "").trim();
    if (!raw) return null;

    const wakeLead = /\bhey\s*,?\s*(l+ama|astro\s*-?\s*l+ama)\b/i;
    const wakeMatch = raw.match(wakeLead);
    if (!wakeMatch || wakeMatch.index === undefined) return null;

    let tail = raw.slice(wakeMatch.index + wakeMatch[0].length).trim();
    tail = tail.replace(/^[\s,!.:;-]+/, "");
    // New wake phrase rule: "Hey Llama" is sufficient.
    // If additional words are spoken in the same utterance, treat them as payload.
    if (!tail) {
      return {
        user: "",
        payload: "",
        full: raw,
      };
    }

    // Keep compatibility with naturally spoken "it's <name> ..." variants
    // by dropping the optional leading contraction before payload parsing.
    tail = tail.replace(/^it'?s\s+/i, "").trim();
    if (!tail) {
      return {
        user: "",
        payload: "",
        full: raw,
      };
    }

    const candidates = getWakeUserCandidates();
    const normalizedTail = normalizeSpeechText(tail);

    for (const candidate of candidates) {
      if (normalizedTail === candidate || normalizedTail.startsWith(candidate + " ")) {
        speech.wakeUser = candidate;
        const candidateWordCount = candidate.split(" ").length;
        const words = tail.split(/\s+/);
        const remainder = words.slice(candidateWordCount).join(" ").trim();
        return {
          user: tail.split(/\s+/).slice(0, candidateWordCount).join(" ").trim(),
          payload: remainder,
          full: raw,
        };
      }
    }

    const fallbackWords = tail.split(/\s+/);
    const guessedUser = fallbackWords[0] || "";
    const payload = fallbackWords.slice(1).join(" ").trim();
    if (!guessedUser) {
      return {
        user: "",
        payload: tail,
        full: raw,
      };
    }
    // For "Hey Llama <prompt...>", treat the full tail as payload rather than
    // interpreting the first word as a required username.
    return {
      user: "",
      payload: tail,
      full: raw,
    };
  }

  function shouldStopWakeListening(transcript) {
    const normalized = normalizeSpeechText(transcript).replace(/ll+ama/g, "llama").replace(/astro\s*-?\s*llama/g, "llama");
    return normalized.includes("that'll do llama")
      || normalized.includes("thatll do llama")
      || normalized.includes("that will do llama");
  }

  function isEntraSignedIn() {
    return Boolean(state.authEnabled && state.account);
  }

  function setMicButtonState() {
    if (!btnMic) return;
    const micActive = speech.listening || speech.monitoringTts;
    const disabled = !speech.supported || speech.monitoringTts || tts.speaking;
    const entraDirectMode = isEntraSignedIn();
    btnMic.disabled = disabled;
    btnMic.classList.toggle("listening", micActive);
    btnMic.classList.toggle("blocked", !micActive && Boolean(speech.blockedReason));
    btnMic.setAttribute("aria-pressed", micActive ? "true" : "false");
    if (tts.speaking && speech.pausedForTts) {
      btnMic.textContent = "Mic Off";
      btnMic.title = "Microphone is paused while assistant speech is playing.";
    } else if (speech.listening && speech.awaitingWake) {
      btnMic.textContent = "Listening";
      btnMic.title = "Listening for questions";
    } else if (speech.listening && entraDirectMode) {
      btnMic.textContent = "Mic Direct";
      btnMic.title = "Entra mode: speech is sent directly (say: That'll do Llama to stop)";
    } else if (speech.listening) {
      btnMic.textContent = "Mic Armed";
      btnMic.title = "Wake phrase accepted. Speak your prompt now.";
    } else if (speech.monitoringTts) {
      btnMic.textContent = "Mic Live";
      btnMic.title = "Mic monitor active while speech is playing";
    } else if (speech.blockedReason === "network") {
      btnMic.textContent = "Mic Error";
      btnMic.title = speech.lastProbeResult.startsWith("silent:")
        ? "Speech recognition failed with a network error and microphone input appears silent. Check the selected input device and browser speech service."
        : "Speech recognition failed with a network error. Click to retry after checking browser speech/network availability.";
    } else if (speech.blockedReason === "not-allowed" || speech.blockedReason === "service-not-allowed") {
      btnMic.textContent = "Mic Blocked";
      btnMic.title = "Microphone or speech recognition permission was denied. Click to retry after allowing access.";
    } else {
      btnMic.textContent = "Mic";
      btnMic.title = entraDirectMode
        ? "Start direct voice input (Entra mode)"
        : "Start wake-phrase voice input";
    }
    updateSidebarPulse();
  }

  function emitWakeGreeting(nameHint = "") {
    const rawName = String(nameHint || "").trim();
    const displayName = rawName || (state.account ? getDisplayName() : "");
    const greeting = displayName
      ? `Hi ${displayName}, I am AstroLlama. Ask me anything about astronomy.`
      : "Hi, I am AstroLlama. Ask me anything about astronomy.";
    speech.lastWakeGreeting = greeting;
    speech.wakeGreetingUntil = Date.now() + 12_000;

    appendMessage("assistant", greeting).catch((err) => {
      console.warn("[Speech] Failed to render wake greeting:", err);
    });
    state.messages.push({ role: "assistant", content: greeting });
    if (tts.supported && tts.enabled) {
      speakWakeGreeting(greeting);
    }
    logSpeechDebug("speech_wake_greeting", {
      text: greeting,
      final: true,
      extra: {
        tts_enabled: tts.enabled,
        tts_supported: tts.supported,
        synth_paused: Boolean(window.speechSynthesis && window.speechSynthesis.paused),
        synth_speaking: Boolean(window.speechSynthesis && window.speechSynthesis.speaking),
        synth_pending: Boolean(window.speechSynthesis && window.speechSynthesis.pending),
      },
    });
  }

  function looksLikeWakeGreetingEcho(text) {
    const normalized = normalizeSpeechText(text);
    if (!normalized) return false;

    if (normalized.includes("call me llama")) return true;
    if (normalized.startsWith("hi i am") && normalized.includes("llama")) return true;

    const greetingMarkers = [
      "i am astrollama",
      "personally",
      "astrollama",
      "call me llama",
      "anything about astronomy",
    ];
    const markerMatches = greetingMarkers.filter(m => normalized.includes(m)).length;
    if (markerMatches >= 1) return true;

    const lastGreeting = normalizeSpeechText(speech.lastWakeGreeting || "");
    if (!lastGreeting) return false;

    // Treat as echo when substantial overlap with the most recent wake greeting.
    const words = normalized.split(" ");
    const overlap = words.filter(w => w.length > 2 && lastGreeting.includes(w)).length;
    return overlap >= 3;
  }

  function applyVizFullscreenPreference() {
    if (ui.preferFullscreenViz) {
      enterVizFullscreen({ allowOverlayFallback: true });
    } else if (fullscreenViz && fullscreenViz.classList.contains("active")) {
      exitVizFullscreen();
    }
  }

  function startSpeechRecognition() {
    if (!speech.recognition || speech.listening) return;
    speech.userStopped = false;
    speech.blockedReason = "";
    speech.awaitingWake = true;
    try {
      speech.recognition.start();
    } catch (err) {
      console.warn("[Speech] Unable to start speech recognition:", err);
      setMicButtonState();
    }
  }

  function stopSpeechRecognition() {
    if (speech.recognition && speech.listening) {
      speech.userStopped = true;
      speech.awaitingWake = true;
      logSpeechDebug("recognition_stop_requested", {
        text: "stop_requested",
      });
      speech.recognition.stop();
    }
  }

  function initSpeechRecognition() {
    if (!btnMic) return;
    if (!speech.supported) {
      btnMic.style.display = "none";
      return;
    }

    const recognition = new SpeechRecognitionCtor();
    recognition.lang = navigator.language || "en-US";
    recognition.continuous = true;
    recognition.interimResults = true;

    recognition.onstart = () => {
      speech.listening = true;
      speech.blockedReason = "";
      // Don't reset to wake-listening if we're intentionally staying armed
      // after the wake greeting (keepArmed is set by speakWakeGreeting onend).
      if (!speech.keepArmed) {
        speech.awaitingWake = !isEntraSignedIn();
      }
      logSpeechDebug("recognition_start", {
        text: "listening",
      });
      setMicButtonState();
    };

    recognition.onresult = (event) => {
      let finalText = "";
      let interimText = "";

      for (let i = event.resultIndex; i < event.results.length; i += 1) {
        const phrase = event.results[i][0]?.transcript || "";
        if (event.results[i].isFinal) {
          finalText += phrase;
        } else {
          interimText += phrase;
        }
      }

      if (interimText.trim()) {
        logSpeechDebug("speech_interim", {
          text: interimText,
          final: false,
        });
      }

      if (!finalText) return;

      logSpeechDebug("speech_final", {
        text: finalText,
        final: true,
      });

      const combinedText = appendTranscript(speech.baseText, finalText);
      speech.baseText = trimTranscriptWindow(combinedText);

      if (!speech.awaitingWake && !isEntraSignedIn()) {
        const armedText = String(finalText || "").trim();
        speech.baseText = "";
        if (!armedText || state.streaming) {
          setMicButtonState();
          return;
        }

        if (shouldStopWakeListening(armedText)) {
          logSpeechDebug("speech_stop_phrase", {
            text: armedText,
            final: true,
          });
          speech.interimText = "";
          speech.userStopped = true;
          stopSpeechRecognition();
          promptInput.value = "";
          setMicButtonState();
          return;
        }

        if (looksLikeWakeGreetingEcho(armedText)) {
          logSpeechDebug("speech_echo_ignored", {
            text: armedText,
            final: true,
          });
          speech.awaitingWake = false;
          setMicButtonState();
          return;
        }

        const repeatedWake = extractWakePayload(armedText);
        const repeatedWakeOnly = Boolean(
          repeatedWake && !String(repeatedWake.payload || "").trim()
        );
        if (repeatedWakeOnly) {
          speech.awaitingWake = false;
          logSpeechDebug("speech_wake_repeated", {
            text: armedText,
            final: true,
          });
          setMicButtonState();
          return;
        }

        speech.keepArmed = true;
        speech.awaitingWake = false;

        logSpeechDebug("speech_submit_after_wake", {
          text: armedText,
          final: true,
        });
        promptInput.value = armedText;
        sendMessage({ fromVoice: true }).catch(err => {
          console.warn("[Speech] Post-wake voice send failed:", err);
        });
        setMicButtonState();
        return;
      }

      if (shouldStopWakeListening(speech.baseText)) {
        logSpeechDebug("speech_stop_phrase", {
          text: speech.baseText,
          final: true,
        });
        speech.baseText = "";
        speech.interimText = "";
        speech.userStopped = true;
        stopSpeechRecognition();
        promptInput.value = "";
        setMicButtonState();
        return;
      }

      if (isEntraSignedIn()) {
        speech.awaitingWake = false;
        const directText = String(finalText || "").trim();
        speech.baseText = "";
        if (!directText || state.streaming) {
          setMicButtonState();
          return;
        }

        logSpeechDebug("speech_submit_direct", {
          text: directText,
          final: true,
        });
        promptInput.value = directText;
        sendMessage({ fromVoice: true }).catch(err => {
          console.warn("[Speech] Entra voice send failed:", err);
        });
        setMicButtonState();
        return;
      }

      const wake = extractWakePayload(speech.baseText);
      if (!wake) {
        logSpeechDebug("speech_wake_miss", {
          text: speech.baseText,
          final: true,
        });
        speech.keepArmed = false;
        speech.awaitingWake = true;
        setMicButtonState();
        return;
      }

      speech.baseText = "";
      const textToSend = String(wake.payload || "").trim();
      if (!textToSend) {
        speech.awaitingWake = false;
        logSpeechDebug("speech_wake_armed", {
          text: wake.user || "wake_armed",
          final: true,
        });
        emitWakeGreeting(wake.user || "");
        setMicButtonState();
        return;
      }
      speech.keepArmed = true;
      speech.awaitingWake = false;
      if (state.streaming) {
        setMicButtonState();
        return;
      }

      logSpeechDebug("speech_submit_wake", {
        text: textToSend,
        final: true,
        extra: { user: wake.user },
      });
      promptInput.value = textToSend;
      sendMessage({ fromVoice: true }).catch(err => {
        console.warn("[Speech] Voice-triggered send failed:", err);
      });
      setMicButtonState();
    };

    recognition.onerror = (event) => {
      console.warn("[Speech] Recognition error:", event.error);
      speech.interimText = "";
      const errorCode = event.error || "unknown";
      if (speech.blockedReason !== errorCode) {
        logSpeechDebug("recognition_error", {
          text: errorCode,
          extra: { error: errorCode },
        });
      }
      if (errorCode === "network") {
        speech.blockedReason = errorCode;
        speech.userStopped = true;
        probeSpeechMicrophone().catch(() => {});
      }
      if (errorCode === "not-allowed" || errorCode === "service-not-allowed") {
        speech.blockedReason = errorCode;
        speech.userStopped = true;
      }
    };

    recognition.onend = () => {
      speech.listening = false;
      speech.interimText = "";
      logSpeechDebug("recognition_end", {
        text: speech.baseText || "ended",
        final: true,
      });
      speech.baseText = "";
      // Auto-restart if the user hasn't explicitly stopped (browser ended it on its own)
      if (!speech.userStopped) {
        try {
          speech.recognition.start();
          return;
        } catch (_) {
          // If restart fails, fall through and reset state
        }
      }
      speech.userStopped = false;
      setMicButtonState();
    };

    speech.recognition = recognition;

    btnMic.addEventListener("click", () => {
      if (ui.voiceStartRequired) {
        completeVoiceStart("mic_button_click");
        return;
      }
      if (speech.listening) {
        stopSpeechRecognition();
        return;
      }

      speech.userStopped = false;
      speech.interimText = "";
      speech.baseText = "";
      startSpeechRecognition();
    });

    setMicButtonState();
    if (!ui.voiceStartRequired) {
      startSpeechRecognition();
    }
  }

  // ---- Auth + API wrapper ----------------------------------------
  function loadScript(src) {
    return new Promise((resolve, reject) => {
      const existing = document.querySelector(`script[src="${src}"]`);
      if (existing) {
        // If MSAL is already present, this source has effectively loaded.
        if (window.msal || existing.dataset.loaded === "true") {
          resolve();
          return;
        }

        // An existing tag that already failed will not emit events again; reject fast
        // so the caller can try fallback sources.
        if (existing.dataset.failed === "true") {
          reject(new Error(`Failed to load script: ${src}`));
          return;
        }

        existing.addEventListener("load", () => {
          existing.dataset.loaded = "true";
          resolve();
        }, { once: true });
        existing.addEventListener("error", () => {
          existing.dataset.failed = "true";
          reject(new Error(`Failed to load script: ${src}`));
        }, { once: true });
        return;
      }

      const script = document.createElement("script");
      script.src = src;
      script.async = true;
      script.addEventListener("load", () => {
        script.dataset.loaded = "true";
        resolve();
      }, { once: true });
      script.addEventListener("error", () => {
        script.dataset.failed = "true";
        reject(new Error(`Failed to load script: ${src}`));
      }, { once: true });
      document.head.appendChild(script);
    });
  }

  async function ensureMsalAvailable() {
    if (window.msal) {
      console.log("[Auth] MSAL already loaded in window");
      return;
    }
    console.log("[Auth] MSAL not found, attempting to load...");

    const sources = [
      // Try jsdelivr first (more reliable for user networks)
      "https://cdn.jsdelivr.net/npm/@azure/msal-browser@2.38.3/lib/msal-browser.min.js",
      // Fallback to Microsoft's official CDN
      "https://alcdn.msauth.net/browser/2.38.3/js/msal-browser.min.js",
    ];

    let lastError = null;
    for (const src of sources) {
      try {
        console.log(`[Auth] Attempting to load from: ${src}`);
        await loadScript(src);
        if (window.msal) {
          console.log("[Auth] ✅ MSAL loaded successfully from:", src);
          console.log("[Auth] window.msal.PublicClientApplication available:", typeof window.msal.PublicClientApplication);
          return;
        } else {
          console.warn(`[Auth] Script loaded but window.msal not found`);
        }
      } catch (err) {
        lastError = err;
        console.warn(`[Auth] ❌ Failed to load MSAL from ${src}:`, err.message);
      }
    }

    throw new Error(`Cannot load MSAL from any CDN. Last error: ${lastError?.message || "unknown"}. Check network access and browser console for CORS/CSP issues.`);
  }

  async function initAuth() {
    console.log("[Auth] initAuth() called");
    const r = await fetch("/api/auth/config");
    if (!r.ok) {
      console.error("[Auth] Failed to fetch auth config", r.status);
      throw new Error("Failed to load auth config");
    }

    const cfg = await r.json();
    console.log("[Auth] Auth config retrieved:", cfg);
    state.authEnabled = Boolean(cfg.enabled);
    state.apiScope = cfg.api_scope || null;
    const redirectUri = cfg.redirect_uri || window.location.origin;
    console.log("[Auth] Using redirect URI:", redirectUri);
    console.log("[Auth] Current window.location.href:", window.location.href);

    if (!state.authEnabled) {
      console.log("[Auth] Auth disabled via config");
      state.authReady = true;
      authBar.style.display = "none";
      return;
    }

    console.log("[Auth] Auth enabled, initializing MSAL...");
    const tenantId = cfg.tenant_id;
    const spaClientId = cfg.spa_client_id;
    console.log("[Auth] Environment values - Tenant:", tenantId, "Client:", spaClientId, "Scope:", state.apiScope);

    if (!tenantId || !spaClientId || !state.apiScope) {
      console.error("[Auth] Missing Entra config:", { tenantId, spaClientId, apiScope: state.apiScope });
      throw new Error("Entra auth is enabled but missing tenant/client/scope settings");
    }

    await ensureMsalAvailable();
    console.log("[Auth] MSAL ready, creating PublicClientApplication...");

    state.msalApp = new msal.PublicClientApplication({
      auth: {
        clientId: spaClientId,
        authority: `https://login.microsoftonline.com/${tenantId}`,
        redirectUri: redirectUri,
      },
      cache: {
        cacheLocation: "localStorage",
        storeAuthStateInCookie: false,
      },
    });
    console.log("[Auth] PublicClientApplication created with config:", {
      clientId: spaClientId,
      authority: `https://login.microsoftonline.com/${tenantId}`,
      redirectUri: redirectUri,
      currentUrl: window.location.href,
      urlSearchParams: window.location.search
    });

    console.log("[Auth] Attempting handleRedirectPromise...");
    try {
      const redirectResult = await state.msalApp.handleRedirectPromise();
      if (redirectResult) {
        console.log("[Auth] ✅ Redirect promise resolved with auth result:", {
          accessToken: redirectResult.accessToken ? "✓ present" : "✗ missing",
          expiresOn: redirectResult.expiresOn,
          account: redirectResult.account?.username || "unknown"
        });
        state.account = redirectResult.account;
        state.msalApp.setActiveAccount(state.account);
        state.accessToken = redirectResult.accessToken;
        updateSystemPromptWithUserName(); // ← Add user name to system prompt on redirect
        
        // If we're in a popup and got a redirect result, close the popup
        // MSAL should handle this, but we ensure it happens
        if (window.opener) {
          console.log("[Auth] Auth completed in popup context. Closing popup...");
          setTimeout(() => {
            window.close();
          }, 100);
          // Return early; don't continue with UI init in popup
          return;
        }
        
        setTimeout(() => { 
          console.log("[Auth] Auth flow completed, ready for requests"); 
        }, 500);
      } else {
        console.log("[Auth] handleRedirectPromise returned null (no redirect context, normal flow)");
      }
    } catch (error) {
      console.error("[Auth] ❌ handleRedirectPromise error:", error.message, error);
    }

    const accounts = state.msalApp.getAllAccounts();
    console.log("[Auth] Existing accounts after redirect check:", accounts.length > 0 ? accounts.map(a => a.username) : "none");
    if (accounts.length > 0 && !state.account) {
      state.account = accounts[0];
      state.msalApp.setActiveAccount(state.account);
      console.log("[Auth] Using first account:", state.account.username);
      updateSystemPromptWithUserName(); // ← Add user name to system prompt
      await ensureToken();
    }

    state.authReady = true;
    console.log("[Auth] Authentication initialization complete, authReady=true");
    console.log("[Auth] Auth initialization complete");
    updateAuthUi();
  }

  // Minimal MSAL init for popup redirect handling only
  // This function ONLY initializes MSAL, handles redirect, and closes the popup
  // It is used ONLY when we detect we are inside the auth redirect popup
  async function initMsalForRedirect() {
    console.log("[Auth/Redirect] Initializing minimal MSAL for redirect handling in popup...");
    
    // Fetch auth config first since we're in a popup and may not have it
    let cfg;
    try {
      const r = await fetch("/api/auth/config");
      if (!r.ok) throw new Error("Failed to fetch auth config");
      cfg = await r.json();
      console.log("[Auth/Redirect] Auth config retrieved:", cfg);
    } catch (err) {
      console.error("[Auth/Redirect] Failed to fetch auth config:", err);
      window.close();
      return;
    }
    
    const redirectUri = cfg.redirect_uri || window.location.origin;
    const tenantId = cfg.tenant_id;
    const spaClientId = cfg.spa_client_id;
    
    if (!tenantId || !spaClientId) {
      console.error("[Auth/Redirect] Missing Entra config in popup");
      window.close();
      return;
    }
    
    console.log("[Auth/Redirect] Creating minimal PublicClientApplication...");
    await ensureMsalAvailable();
    
    const msalApp = new msal.PublicClientApplication({
      auth: {
        clientId: spaClientId,
        authority: `https://login.microsoftonline.com/${tenantId}`,
        redirectUri: redirectUri,
      },
      cache: {
        cacheLocation: "localStorage",
        storeAuthStateInCookie: false,
      },
    });
    
    console.log("[Auth/Redirect] Calling handleRedirectPromise() - waiting for auth redirect...");
    console.log("[Auth/Redirect] Current URL:", window.location.href);
    console.log("[Auth/Redirect] Redirect URI:", redirectUri);
    
    try {
      // handleRedirectPromise() will wait for the redirect and resolve when auth code is received
      const redirectResult = await msalApp.handleRedirectPromise();
      
      if (redirectResult) {
        console.log("[Auth/Redirect] ✅ Redirect completed, token received!");
        console.log("[Auth/Redirect] Account:", redirectResult.account?.username);
        console.log("[Auth/Redirect] Token present:", !!redirectResult.accessToken);
        console.log("[Auth/Redirect] Scopes:", redirectResult.scopes);
      } else {
        console.log("[Auth/Redirect] handleRedirectPromise returned null (no active redirect)");
      }
    } catch (err) {
      console.error("[Auth/Redirect] handleRedirectPromise error:", err);
    }
    
    // Give localStorage time to sync, then close
    console.log("[Auth/Redirect] ✅ Waiting 500ms for state to sync, then closing popup...");
    await new Promise(resolve => setTimeout(resolve, 500));
    console.log("[Auth/Redirect] Closing popup now.");
    window.close();
  }

  async function ensureToken() {
    console.log("[Auth] ensureToken() called");
    if (!state.authEnabled) return null;
    if (!state.account) throw new Error("Sign-in required");

    const request = {
      scopes: [state.apiScope],
      account: state.account,
    };
    console.log("[Auth] Token request with scopes:", request.scopes);
    console.log("[Auth] Account:", state.account.username);

    try {
      console.log("[Auth] Attempting silent token acquisition...");
      const result = await state.msalApp.acquireTokenSilent(request);
      console.log("[Auth] ✅ Token acquired silently");
      console.log("[Auth] Token scope:", result.scopes);
      console.log("[Auth] Token expires:", result.expiresOn);
      state.accessToken = result.accessToken;
      console.log("[Auth] Returning token:", state.accessToken.substring(0, 50) + "...");
      return state.accessToken;
    } catch (err) {
      console.log("[Auth] ⚠ Silent token failed:", err.message);
      console.log("[Auth] Attempting popup token acquisition...");
      try {
        const result = await state.msalApp.acquireTokenPopup(request);
        console.log("[Auth] ✅ Token acquired via popup");
        console.log("[Auth] Token scope:", result.scopes);
        console.log("[Auth] Token expires:", result.expiresOn);
        state.accessToken = result.accessToken;
        state.account = result.account;
        state.msalApp.setActiveAccount(state.account);
        updateAuthUi();
        return state.accessToken;
      } catch (popupErr) {
        console.error("[Auth] ❌ Popup token acquisition failed:", popupErr.message);
        throw popupErr;
      }
    }
  }

  // Update system prompt with user name when they sign in
  function getDisplayName() {
    const claims = state.account?.idTokenClaims || {};

    // Helper: convert UPN-style string to "First Last"
    const formatUpn = (s) => s.split("@")[0].replace(/[._]/g, " ").replace(/\b\w/g, c => c.toUpperCase());

    // Prefer given_name + family_name from token claims (most reliable for proper names)
    if (claims.given_name || claims.family_name) {
      return [claims.given_name, claims.family_name].filter(Boolean).join(" ");
    }
    // Use display name if it looks like a real name (contains a space), otherwise format it
    if (claims.name) {
      return claims.name.includes(" ") ? claims.name : formatUpn(claims.name);
    }
    // Last resort: derive from UPN / username
    const upn = state.account?.username || "User";
    return formatUpn(upn);
  }

  function updateSystemPromptWithUserName() {
    if (!state.account) return;
    
    const displayName = getDisplayName();
    
    console.log("[Auth] Updating system prompt with user name:", displayName);
    
    // Get current system prompt
    let currentPrompt = sSystemPrompt.value.trim();
    
    // Remove any existing user name prefix from previous logins
    const userPrefixPattern = /^User name: .+?\n\n/;
    currentPrompt = currentPrompt.replace(userPrefixPattern, "");
    
    // Add new user name prefix
    const userPrefix = `User name: ${displayName}\n\n`;
    sSystemPrompt.value = userPrefix + currentPrompt;
    
    // Save to localStorage
    saveSettings();
  }
  
  // Clear user name from system prompt on logout
  function clearUserNameFromSystemPrompt() {
    console.log("[Auth] Clearing user name from system prompt");
    
    let currentPrompt = sSystemPrompt.value.trim();
    
    // Remove user name prefix
    const userPrefixPattern = /^User name: .+?\n\n/;
    currentPrompt = currentPrompt.replace(userPrefixPattern, "");
    
    sSystemPrompt.value = currentPrompt;
    
    // Save to localStorage
    saveSettings();
  }
  
  function updateAuthUi() {
    console.log("[Auth] updateAuthUi() called");
    if (!state.authEnabled) return;
    const signedIn = Boolean(state.account);
    console.log("[Auth] Signed in:", signedIn);
    authUser.textContent = signedIn
      ? getDisplayName()
      : "Not signed in";
    btnLogin.disabled = signedIn;
    btnLogout.disabled = !signedIn;
    btnSend.disabled = !signedIn || state.streaming;
    promptInput.disabled = !signedIn;
    setMicButtonState();
  }

  async function signIn() {
    console.log("[Auth] signIn() called", {
      authEnabled: state.authEnabled,
      msalReady: !!state.msalApp,
      currentUrl: window.location.href,
      isPopup: window.opener ? "YES (popup window)" : "NO (main window)"
    });
    
    // Guard: prevent nested popups. If we're currently IN a popup, don't open another one
    if (window.opener) {
      console.log("[Auth] ⚠️ signIn() called from within a popup. Ignoring to prevent nested popup.");
      console.log("[Auth] The redirect handler should be processing this authentication.");
      return;
    }
    
    if (!state.authEnabled || !state.msalApp) {
      console.error("[Auth] Cannot sign in: auth not ready");
      alert("Authentication not ready");
      return;
    }
    
    const loginRequest = { scopes: [state.apiScope] };
    console.log("[Auth] Initiating login popup with request:", loginRequest);
    
    try {
      console.log("[Auth] Calling msalApp.loginPopup()...");
      const result = await state.msalApp.loginPopup(loginRequest);
      console.log("[Auth] ✅ loginPopup completed successfully");
      console.log("[Auth] Result account:", result.account?.username);
      console.log("[Auth] Result accessToken:", result.accessToken ? "✓ present" : "✗ missing");
      state.account = result.account;
      state.msalApp.setActiveAccount(state.account);
      console.log("[Auth] Account set in state, calling ensureToken...");
      updateSystemPromptWithUserName(); // ← Add user name to system prompt
      await ensureToken();
      updateAuthUi();
      console.log("[Auth] Sign in flow complete");
      await pollStatus();
      await loadConvList();
    } catch (error) {
      console.error("[Auth] ❌ Sign in failed:", error.message);
      console.error("[Auth] Error details:", error);
      alert("Sign in failed: " + error.message);
    }
  }

  async function signOut() {
    console.log("[Auth] signOut() called");
    if (!state.authEnabled || !state.msalApp || !state.account) return;
    console.log("[Auth] Logging out...");
    await state.msalApp.logoutPopup({ account: state.account });
    console.log("[Auth] Logout successful");
    state.account = null;
    state.accessToken = null;
    state.conversationId = null;
    state.messages = [];
    clearUserNameFromSystemPrompt(); // ← Clear user name from system prompt
    messagesEl.innerHTML = "";
    convList.innerHTML = "";
    updateAuthUi();
    setChipStatus(statusLlama, false, "llama");
    setChipStatus(statusMcp, false, "mcp");
  }

  async function apiFetch(url, options = {}) {
    const opts = { ...options };
    opts.headers = { ...(opts.headers || {}) };

    if (state.authEnabled) {
      if (!state.authReady) throw new Error("Authentication is not ready");
      const token = await ensureToken();
      opts.headers.Authorization = `Bearer ${token}`;
      console.log(`[API] Sending ${options.method || 'GET'} ${url} with auth header`);
      console.log(`[API] Bearer token (first 50 chars):`, token.substring(0, 50) + "...");
    }

    const response = await fetch(url, opts);
    
    console.log(`[API] ${options.method || 'GET'} ${url} → ${response.status}`);
    if (!response.ok && state.authEnabled) {
      console.warn(`[API] Response not OK, status: ${response.status}`);
      if (response.status === 401) {
        console.log("[API] Got 401, clearing token and updating UI");
        state.accessToken = null;
        updateAuthUi();
      }
    }

    return response;
  }

  // ---- Status polling ----------------------------------------------
  async function pollStatus() {
    try {
      const r = await apiFetch("/api/status");
      if (!r.ok) return;
      const data = await r.json();
      setChipStatus(statusLlama, data.llama_server === "ok", "llama");
      setChipStatus(statusMcp,   data.mcp_server   === "ok", "mcp");
    } catch (_) {
      setChipStatus(statusLlama, false, "llama");
      setChipStatus(statusMcp,   false, "mcp");
    }
  }

  function setChipStatus(el, ok, label) {
    el.className = "status-chip " + (ok ? "ok" : "error");
    el.textContent = label + " \u25CF";
  }

  // ---- Conversation list -------------------------------------------
  async function loadConvList() {
    try {
      const r = await apiFetch("/api/conversations");
      if (!r.ok) return;
      const list = await r.json();
      convList.innerHTML = "";
      list.forEach(conv => {
        const li = document.createElement("li");
        if (conv.id === state.conversationId) li.classList.add("active");

        const nameSpan = document.createElement("span");
        nameSpan.className = "conv-name";
        nameSpan.textContent = conv.name || "Untitled";

        const delBtn = document.createElement("button");
        delBtn.className = "del-btn";
        delBtn.textContent = "\u2715";
        delBtn.title = "Delete";
        delBtn.addEventListener("click", (e) => {
          e.stopPropagation();
          deleteConv(conv.id);
        });

        li.appendChild(nameSpan);
        li.appendChild(delBtn);
        li.addEventListener("click", () => loadConv(conv.id));
        convList.appendChild(li);
      });
    } catch (_) {}
  }

  async function loadConv(id) {
    try {
      const r = await apiFetch(`/api/conversations/${id}`);
      if (!r.ok) return;
      const conv = await r.json();
      state.conversationId = id;
      state.messages = conv.messages || [];
      messagesEl.innerHTML = "";
      for (const msg of state.messages) {
        if (msg.role === "system") continue;
        await appendMessage(msg.role === "user" ? "user" : "assistant", msg.content || "");
      }
      loadConvList();
    } catch (_) {}
  }

  async function deleteConv(id) {
    if (!confirm("Delete this conversation?")) return;
    await apiFetch(`/api/conversations/${id}`, { method: "DELETE" });
    if (state.conversationId === id) newChat();
    loadConvList();
  }

  // ---- New chat ----------------------------------------------------
  function newChat() {
    state.conversationId = null;
    state.messages = [];
    messagesEl.innerHTML = "";
    loadConvList();
  }

  // ---- Save conversation -------------------------------------------
  async function saveConv() {
    if (!state.conversationId || state.messages.length === 0) {
      alert("Nothing to save yet.");
      return;
    }
    const name = prompt("Conversation name:", "My conversation") || "Untitled";
    const body = {
      conversation_id: state.conversationId,
      name,
      messages: state.messages,
      settings: {
        temperature:   parseFloat(sTemperature.value),
        top_p:         parseFloat(sTopP.value),
        max_tokens:    parseInt(sMaxTokens.value, 10),
        system_prompt: sSystemPrompt.value,
      },
    };
    const r = await apiFetch("/api/conversations", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (r.ok) loadConvList();
    else alert("Failed to save conversation.");
  }

  // ---- Message rendering -------------------------------------------
  async function appendMessage(role, text) {
    const div = document.createElement("div");
    div.className = "msg " + role;
    const label = document.createElement("div");
    label.className = "msg-label";
    label.textContent = role === "user" ? "You" : "Assistant";
    div.appendChild(label);
    const content = document.createElement("div");
    content.className = "msg-content";
    if (role === "assistant") {
      content.innerHTML = await renderHighlighted(text);
    } else {
      const p = document.createElement("p");
      p.textContent = text;
      content.appendChild(p);
    }
    div.appendChild(content);
    messagesEl.appendChild(div);
    scrollToBottom();
    return div;
  }

  function appendThinking() {
    const div = document.createElement("div");
    div.className = "msg assistant";
    const label = document.createElement("div");
    label.className = "msg-label";
    label.textContent = "Assistant";
    div.appendChild(label);
    const thinking = document.createElement("div");
    thinking.className = "thinking";
    thinking.innerHTML = "<span></span><span></span><span></span>";
    div.appendChild(thinking);
    messagesEl.appendChild(div);
    scrollToBottom();
    return div;
  }

  function scrollToBottom() {
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }

  function parseSummarizeLink(href) {
    if (!href || !href.startsWith("astrollama://summarize")) return null;
    const qIndex = href.indexOf("?");
    if (qIndex === -1) return null;
    const query = href.slice(qIndex + 1);
    const params = new URLSearchParams(query);
    const title = (params.get("title") || "").trim();
    const paperId = (params.get("paper_id") || "").trim();
    if (!title) return null;
    return { title, paperId };
  }

  // ---- Tool call details block (inside an assistant message) -------
  function ensureToolDetails(msgEl) {
    let details = msgEl.querySelector(".tool-details");
    if (!details) {
      details = document.createElement("details");
      details.className = "tool-details";
      const summary = document.createElement("summary");
      summary.textContent = "Tool calls";
      details.appendChild(summary);
      msgEl.appendChild(details);
    }
    return details;
  }

  function addToolStart(msgEl, name, args) {
    const details = ensureToolDetails(msgEl);
    const entry = document.createElement("div");
    entry.className = "tool-entry";
    entry.dataset.toolName = name;
    entry.innerHTML = `<div class="tool-name">\u2699\ufe0f ${escHtml(name)}</div>` +
      `<div class="tool-args">Args: ${escHtml(JSON.stringify(args, null, 2))}</div>`;
    details.appendChild(entry);
    details.open = false;
  }

  async function addToolResult(msgEl, name, result) {
    const details = ensureToolDetails(msgEl);
    const entry = [...details.querySelectorAll(".tool-entry")]
      .reverse()
      .find(e => e.dataset.toolName === name);
    const resultText = String(result || "").trim();

    if (entry) {
      entry.dataset.done = "true";
      const existing = entry.querySelector(".tool-result");
      if (existing) existing.remove();
      if (resultText) {
        const resultDiv = document.createElement("div");
        resultDiv.className = "tool-result";
        resultDiv.innerHTML = await renderHighlighted(resultText);
        entry.appendChild(resultDiv);
      }
      details.open = true;
      return;
    }

    const div = document.createElement("div");
    div.className = "tool-entry";
    div.dataset.toolName = name;
    div.dataset.done = "true";
    div.innerHTML = `<div class="tool-name">⚙️ ${escHtml(name)}</div>`;
    if (resultText) {
      const resultDiv = document.createElement("div");
      resultDiv.className = "tool-result";
      resultDiv.innerHTML = await renderHighlighted(resultText);
      div.appendChild(resultDiv);
    }
    details.appendChild(div);
    details.open = true;
  }

  function addToolDownload(msgEl, name, url, size) {
    const details = ensureToolDetails(msgEl);
    const entry = [...details.querySelectorAll(".tool-entry")]
      .reverse()
      .find(e => e.dataset.toolName === name);
    const sizeStr = size > 1048576
      ? (size / 1048576).toFixed(1) + " MB"
      : (size / 1024).toFixed(1) + " KB";
    const linkHtml = `<div class="tool-download">💾 Result too large for inline display (${escHtml(sizeStr)}) — ` +
      `<a href="${escHtml(url)}" download>Download file</a></div>`;
    if (entry) {
      entry.dataset.done = "true";
      entry.insertAdjacentHTML("beforeend", linkHtml);
    } else {
      const div = document.createElement("div");
      div.className = "tool-entry";
      div.dataset.toolName = name;
      div.dataset.done = "true";
      div.innerHTML = `<div class="tool-name">⚙️ ${escHtml(name)}</div>` + linkHtml;
      details.appendChild(div);
    }
  }

  function addToolImage(msgEl, name, url) {
    // Render the image directly in the message bubble (outside the collapsed
    // tool-details panel) so it is always visible.
    const existing = msgEl.querySelector(`.tool-image[data-tool-name="${CSS.escape(name)}"]`);
    if (existing) return; // don't duplicate
    const wrapper = document.createElement("div");
    wrapper.className = "tool-image map-thumbnail-wrap";
    wrapper.dataset.toolName = name;
    wrapper.innerHTML =
      `<a href="${escHtml(url)}" target="_blank" rel="noopener noreferrer">` +
      `<img src="${escHtml(url)}" class="map-thumbnail" alt="Star map">` +
      `</a>`;
    msgEl.appendChild(wrapper);
  }

  function addToolError(msgEl, name, error) {
    const details = ensureToolDetails(msgEl);
    const errDiv = document.createElement("div");
    errDiv.className = "tool-entry";
    errDiv.innerHTML = `<div class="tool-name">\u274c ${escHtml(name)}</div>` +
      `<div class="tool-error">${escHtml(error)}</div>`;
    details.appendChild(errDiv);
  }

  function escHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  // ---- Pygments syntax highlighting via backend API ---------------

  /** Fetch the Pygments CSS once and inject it into <head>. */
  async function initHighlightStyles() {
    try {
      const r = await apiFetch("/api/highlight/styles");
      if (!r.ok) return;
      const css = await r.text();
      const style = document.createElement("style");
      style.id = "pygments-styles";
      style.textContent = css;
      document.head.appendChild(style);
    } catch (_) {}
  }

  /**
   * Send *text* to the backend highlight endpoint and return an HTML string.
   * Falls back to client-side <pre><code> rendering on error.
   */
  async function renderHighlighted(text) {
    try {
      const r = await apiFetch("/api/highlight", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text }),
      });
      if (r.ok) {
        const data = await r.json();
        return data.html;
      }
      console.warn("[Highlight] API returned", r.status, "— using client-side fallback");
    } catch (err) {
      console.warn("[Highlight] API call failed:", err);
    }
    // Client-side fallback: use marked.js for full markdown rendering
    const rendered = marked.parse(text);
    return rendered.replace(/<a\b(?![^>]*\btarget=)([^>]*)>/gi, '<a$1 target="_blank" rel="noopener noreferrer">');
  }

  // ---- Send message ------------------------------------------------
  async function sendMessage(options = {}) {
    const fromVoice = Boolean(options.fromVoice);
    if (fromVoice) {
      speech.baseText = "";
      speech.interimText = "";
    }
    cancelSpeechOutput();

    const text = promptInput.value.trim();
    if (!text || state.streaming) return;

    // Set streaming flag immediately to prevent double-send
    state.streaming = true;
    btnSend.disabled = true;
    btnCancel.disabled = false;
    setMicButtonState();
    updateSidebarPulse();

    promptInput.value = "";
    promptInput.style.height = "";

    // Display user message immediately
    appendMessage("user", text);
    state.messages.push({ role: "user", content: text });

    // Show thinking indicator
    const aiBubble = appendThinking();

    state.abortController = new AbortController();

    const requestBody = {
      conversation_id: state.conversationId,
      message: text,
      settings: {
        temperature:   parseFloat(sTemperature.value),
        top_p:         parseFloat(sTopP.value),
        max_tokens:    parseInt(sMaxTokens.value, 10),
        system_prompt: sSystemPrompt.value || null,
      },
    };

    let contentEl = null;
    let assistantText = "";

    try {
      const response = await apiFetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(requestBody),
        signal: state.abortController.signal,
      });

      if (!response.ok) {
        throw new Error(`Server error: ${response.status}`);
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop(); // last partial line

        for (const line of lines) {
          if (!line.startsWith("data:")) continue;
          const dataStr = line.slice(5).trim();
          if (!dataStr) continue;

          let event;
          try { event = JSON.parse(dataStr); } catch (_) { continue; }

          switch (event.type) {
            case "conversation_id":
              state.conversationId = event.conversation_id;
              loadConvList();
              break;

            case "token":
              if (!contentEl) {
                // Replace thinking indicator with real content
                const thinking = aiBubble.querySelector(".thinking");
                if (thinking) thinking.remove();
                contentEl = document.createElement("div");
                contentEl.className = "msg-content";
                aiBubble.appendChild(contentEl);
              }
              assistantText += event.text;
              enqueueStreamingSpeech(event.text);
              contentEl.textContent = assistantText;
              scrollToBottom();
              break;

            case "tool_start":
              addToolStart(aiBubble, event.name, event.args);
              scrollToBottom();
              break;

            case "tool_result":
              await addToolResult(aiBubble, event.name, event.result);
              scrollToBottom();
              break;

            case "tool_download":
              addToolDownload(aiBubble, event.name, event.url, event.size);
              scrollToBottom();
              break;

            case "tool_image":
              addToolImage(aiBubble, event.name, event.url);
              scrollToBottom();
              break;

            case "tool_error":
              addToolError(aiBubble, event.name, event.error);
              scrollToBottom();
              break;

            case "error": {
              const errDiv = document.createElement("div");
              errDiv.className = "msg error";
              const lbl = document.createElement("div");
              lbl.className = "msg-label";
              lbl.textContent = "Error";
              errDiv.appendChild(lbl);
              const eContent = document.createElement("div");
              eContent.textContent = event.message;
              errDiv.appendChild(eContent);
              // Replace thinking bubble with error
              aiBubble.replaceWith(errDiv);
              scrollToBottom();
              break;
            }

            case "done":
              flushStreamingSpeech();
              if (assistantText) {
                state.messages.push({ role: "assistant", content: assistantText });
                if (contentEl) {
                  // Strip bare "Image: /api/files/..." lines and markdown images
                  // pointing at /api/files/ before rendering — addToolImage already
                  // rendered those as thumbnails in the bubble.
                  // The full text is preserved in state.messages so conversation
                  // history replays render them as thumbnails via renderHighlighted.
                  const textForRender = assistantText
                    .replace(/^Image:\s*\/api\/files\/\S+$/gim, "")
                    .replace(/!\[[^\]]*\]\(\/api\/files\/[^)]+\)/gi, "")
                    .trim();
                  contentEl.innerHTML = textForRender
                    ? await renderHighlighted(textForRender)
                    : "";
                }
                // Re-append all tool-image thumbnails so they always appear
                // after the text content, regardless of SSE event order.
                aiBubble.querySelectorAll(".tool-image").forEach(
                  el => aiBubble.appendChild(el)
                );
                scrollToBottom();
              }
              break;
          }
        }
      }
    } catch (err) {
      if (err.name !== "AbortError") {
        // Show error in the bubble
        const thinking = aiBubble.querySelector(".thinking");
        if (thinking) thinking.remove();
        const errEl = document.createElement("div");
        errEl.className = "tool-error";
        errEl.textContent = "Connection error: " + err.message;
        aiBubble.appendChild(errEl);
        scrollToBottom();
      } else {
        // Cancelled — clean up thinking if still shown
        const thinking = aiBubble.querySelector(".thinking");
        if (thinking) {
          aiBubble.querySelector(".msg-label").textContent = "Assistant (cancelled)";
          thinking.remove();
          const cancelNote = document.createElement("div");
          cancelNote.className = "msg-content";
          cancelNote.style.opacity = "0.5";
          cancelNote.textContent = assistantText || "(cancelled)";
          aiBubble.appendChild(cancelNote);
        }
      }
    } finally {
      state.streaming = false;
      state.abortController = null;
      btnSend.disabled = false;
      btnCancel.disabled = true;
      setMicButtonState();
      updateSidebarPulse();
    }
  }

  // ---- Event listeners --------------------------------------------
  btnSend.addEventListener("click", sendMessage);

  if (btnVoiceStart) {
    btnVoiceStart.addEventListener("click", () => {
      completeVoiceStart("voice_start_button");
    });
  }

  promptInput.addEventListener("keydown", (e) => {
    if (ui.voiceStartRequired) {
      completeVoiceStart("keyboard_input");
    }
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  });

  messagesEl.addEventListener("click", (e) => {
    const target = e.target instanceof Element ? e.target.closest("a") : null;
    if (!target) return;

    const href = target.getAttribute("href") || "";
    const parsed = parseSummarizeLink(href);
    if (!parsed) return;

    e.preventDefault();
    if (state.streaming) return;

    const prompt = parsed.paperId
      ? `Retrieve and summarize the arXiv paper "${parsed.title}" (arXiv:${parsed.paperId}) using load_paper_html_text.`
      : `Retrieve and summarize the paper "${parsed.title}" using load_paper_html_text.`;
    promptInput.value = prompt;
    promptInput.dispatchEvent(new Event("input", { bubbles: true }));
    sendMessage();
  });

  btnCancel.addEventListener("click", () => {
    if (state.abortController) state.abortController.abort();
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && toolValvesController && toolValvesController.isOpen()) {
      toolValvesController.close();
      return;
    }
    if (e.key === "Escape" && fullscreenViz && fullscreenViz.classList.contains("active")) {
      exitVizFullscreen();
      return;
    }
    if (e.key === "Escape" && state.abortController) {
      state.abortController.abort();
    }
  });

  if (btnVizFullscreen) {
    btnVizFullscreen.addEventListener("click", () => {
      const active = Boolean(fullscreenViz && fullscreenViz.classList.contains("active"));
      if (active) {
        exitVizFullscreen();
      } else {
        enterVizFullscreen();
      }
    });
  }

  if (toolValvesController) {
    toolValvesController.bind();
  }

  if (sVizFullscreenDefault) {
    sVizFullscreenDefault.addEventListener("change", () => {
      ui.preferFullscreenViz = Boolean(sVizFullscreenDefault.checked);
      saveSettings();
      applyVizFullscreenPreference();
    });
  }

  document.addEventListener("fullscreenchange", () => {
    const active = Boolean(document.fullscreenElement && fullscreenViz && document.fullscreenElement === fullscreenViz);
    if (!fullscreenViz) return;
    fullscreenViz.classList.toggle("active", active);
    fullscreenViz.setAttribute("aria-hidden", active ? "false" : "true");
    updateVizFullscreenButtonState();
  });

  btnNewChat.addEventListener("click", newChat);
  btnSaveConv.addEventListener("click", saveConv);
  btnLogin.addEventListener("click", () => {
    console.log("[UI] Sign in button clicked");
    signIn().catch(err => {
      console.error("[UI] Sign in error:", err);
      alert(err.message || "Sign in failed");
    });
  });
  btnLogout.addEventListener("click", () => {
    console.log("[UI] Sign out button clicked");
    signOut().catch(err => {
      console.error("[UI] Sign out error:", err);
      alert(err.message || "Sign out failed");
    });
  });

  // ---- Init -------------------------------------------------------
  async function init() {
    console.log("[Init] Starting app initialization...");
    console.log("[Init] Environment info:", {
      isPopup: isInPopup,
      isChildWindow: isChildWindow,
      currentUrl: window.location.href,
      hasRedirectCode: window.location.search.includes("code=") ? "YES" : "NO",
      hasState: window.location.search.includes("state=") ? "YES" : "NO",
      hasError: window.location.search.includes("error=") ? "YES" : "NO",
    });
    
    // CRITICAL: If we're in a popup, ONLY handle redirect and close.
    // Do NOT initialize MSAL, UI, or any other app functionality.
    if (isInPopup) {
      console.log("[Init] 🔴 POPUP DETECTED: Skipping app initialization, only handling auth redirect...");
      try {
        loadSettings(); // Load settings first so we have MSAL config
        
        // Initialize minimal MSAL just for redirect handling
        await initMsalForRedirect();
        
        console.log("[Init] Popup should have closed after redirect handling.");
      } catch (err) {
        console.error("[Init] Error in popup redirect handling:", err);
        window.close();
      }
      return;
    }
    
    // Main window initialization
    loadSettings();
    ui.voiceStartRequired = Boolean((speech.supported || tts.supported) && !hasVoiceStartedOnce());
    if (ui.voiceStartRequired) {
      showVoiceStartGate();
    } else {
      hideVoiceStartGate();
    }
    applyVizFullscreenPreference();
    initTextToSpeech();
    initSpeechRecognition();
    await initHighlightStyles();

    try {
      await initAuth();
    } catch (err) {
      console.error("[Init] Auth initialization failed:", err);
      authUser.textContent = "Authentication setup failed";
      btnLogin.disabled = true;
      btnLogout.disabled = true;
      btnSend.disabled = true;
      promptInput.disabled = true;
      alert(err.message || "Authentication setup failed");
      return;
    }

    console.log("[Init] Auth initialized successfully");
    updateAuthUi();
    updateVizFullscreenButtonState();
    await loadConvList();
    await pollStatus();
    if (toolValvesController) {
      try {
        await toolValvesController.preload(true);
      } catch (err) {
        console.warn("[Tools] Valves menu unavailable:", err);
      }
    }
    setInterval(pollStatus, 10_000);
  }

  console.log("[Boot] Script loaded, calling init()...");
  init();
})();
