# How We Implemented Real-Time Voice Input and Streaming Text-to-Speech in AstroLlama

Most chat apps treat voice as an afterthought: click a mic, dump text, and read everything at the end. We wanted something more natural.

In AstroLlama, voice had to feel like a live conversation:
- Speak naturally into the mic with interim transcription while you talk.
- Hear answers as they stream, not after the full response completes.
- Keep the interface stateful and legible, even while input, generation, and output overlap.

This post walks through exactly how we implemented it in a browser-first way using native Web APIs and a small amount of state management.

## The Design Constraints

We set four constraints early:

1. No heavy frontend framework requirement for voice features.
2. Browser-native speech APIs first, with graceful fallback.
3. Streaming output from server to UI, with speech chunking in near real-time.
4. Strong UX feedback so users always know what the system is doing.

The result was a single client-side pipeline built around three parallel systems:
- Speech recognition (input)
- Streaming chat transport (generation)
- Speech synthesis (output)

## Voice Input: Continuous Recognition with Interim Results

For speech recognition we used the browser implementation exposed as SpeechRecognition (or webkitSpeechRecognition where needed).

Core setup:
- continuous = true
- interimResults = true
- recognition language defaults to navigator.language

Why interim results matter: users can see words appear before each phrase is finalized, which makes dictation feel responsive.

We maintain two buffers:
- baseText: finalized transcript
- interimText: temporary in-progress phrase

When onresult fires, we split final and interim fragments, append finals into baseText, and render baseText + interimText into the prompt field.

This avoids a common bug where finalizing one phrase accidentally duplicates or overwrites prior text.

### Input Stability Pattern

Before sending a message, we stop recognition and clear interim state. This prevents the recognizer from racing with message submission and mutating the prompt after send begins.

In practice that sequence is:
- stop recognition
- clear listening flags
- lock relevant controls while streaming

This tiny ordering detail eliminated most voice-related edge glitches.

## Voice Output: Web Speech API with Streaming-Aware Chunking

For text-to-speech we used speechSynthesis and SpeechSynthesisUtterance.

The interesting part is not the API call itself. The important part is handling partial model output while text is still streaming from the server.

### Step 1: Normalize Text for Speech

Model responses include markdown and tool artifacts. Great for reading, bad for speech.

We normalize text before speaking by stripping or converting:
- markdown links to visible text
- inline code markers
- emphasis markers
- list markers
- heading and quote prefixes
- image placeholders

That keeps spoken output conversational instead of reading punctuation syntax aloud.

### Step 2: Maintain a Streaming Speech Buffer

Incoming token text is appended to a tts streamBuffer.

We do not speak every token immediately. Instead, we drain the buffer into chunks using heuristics:
- Prefer sentence boundaries after punctuation once enough content accumulates.
- If no punctuation yet, cut around a target length near word boundaries.
- On final done event, force-flush the remainder.

This produces smoother cadence than token-by-token speech and avoids excessive utterance fragmentation.

### Step 3: Queue Utterances and Track Pending Count

Each chunk becomes a SpeechSynthesisUtterance with selected voice, rate, and pitch.

We track pendingUtterances to know when speaking truly finishes:
- increment before enqueue
- decrement on onend or onerror
- when pending reaches zero, mark speaking false and reset visual/audio monitors

That prevents UI state from getting stuck in speaking mode after cancellations or synthesis errors.

## Voice Selection and Persistence

Different browsers expose wildly different voice inventories.

We implemented a voice scoring function that prefers:
- exact language match first
- local/default voices as tie-breakers
- premium/neural naming hints when present
- penalties for low-quality synthetic voice families

User preferences are persisted in localStorage:
- selected voice URI
- rate
- pitch
- whether reactive orb monitoring is enabled
- global TTS enabled state

This means users only configure voice once.

## Streaming Transport and TTS Synchronization

AstroLlama receives model output via fetch plus ReadableStream and SSE-style data lines.

For each token event:
- append token to assistant text in the bubble
- pass token text into enqueueStreamingSpeech

On done event:
- flush remaining speech buffer
- render final rich markdown
- persist assistant message

This pairing lets users read and hear the answer as it forms.

## Reactive Orb: Visualizing Listening, Thinking, and Speaking

A good voice interface needs a clear status display.

We used a pulsing orb with explicit mode classes:
- pulse-listening
- pulse-thinking
- pulse-speaking
- idle

The orb supports two speech-activity modes:

1. Boundary pulse mode
When a speech utterance boundary event fires, we trigger a short pulse burst.

2. Mic-reactive mode
If enabled, we capture microphone input with getUserMedia, run an AnalyserNode over time-domain data, compute RMS energy, smooth it, and map to intensity.

This makes the orb feel tied to live audio energy rather than canned animation.

Important detail: we cleanly tear down MediaStream tracks and AudioContext on stop to avoid leaked mic sessions.

## Control-State Rules That Keep UX Predictable

Voice interfaces fail fast when controls are ambiguous. We used strict rules:

- Mic button disabled while model streaming.
- Mic button also disabled during mic-reactive TTS monitoring.
- TTS toggle immediately cancels active speech when turned off.
- Cancel action aborts network stream and gracefully finalizes UI state.

These rules reduce accidental overlap between capture and playback.

## Accessibility and Fallbacks

Not all browsers support all voice APIs.

We feature-detect both systems independently:
- hide mic controls if SpeechRecognition is unavailable
- hide or disable TTS controls if speechSynthesis is unavailable

Buttons update aria-pressed and descriptive titles so state is screen-reader friendly.

## What Worked Best

Three implementation choices made the biggest difference:

1. Streaming-first speech buffer
Chunking on punctuation and length gave natural pacing without long waits.

2. Text normalization before speaking
Removing markdown syntax dramatically improved comprehension.

3. Explicit state machine behavior
Tracking listening, streaming, speaking, and pending utterances prevented contradictory UI states.

## Production Lessons

If you are adding voice to an existing chat app, these are the practical takeaways:

- Treat speech recognition, model streaming, and speech synthesis as separate but synchronized pipelines.
- Never bind speech directly to raw token cadence.
- Build cancellation paths first, then add polish.
- Keep visual status feedback tightly coupled to actual runtime state.
- Persist user voice preferences so the feature feels personal, not experimental.

## Closing

Voice UX is often judged in the first ten seconds. If dictation lags, speech sounds robotic, or state is unclear, users disengage.

By combining native browser speech APIs with a streaming-aware chunking layer and strict UI state management, AstroLlama delivers a voice experience that feels immediate and conversational without requiring heavy frontend dependencies.

If you are building local-first AI tools, this pattern is a strong baseline: keep the stack simple, stream everything, and make the interface tell the truth about what the system is doing.
