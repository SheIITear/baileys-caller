/**
 * Stream feeder.
 *
 * Push-based uplink for live audio (e.g. a realtime voice model). Callers
 * push Float32 PCM of any length at the capture sample rate; the feeder
 * re-chunks it to the WASM frame size and meters one chunk per frame
 * interval, sending silence on underflow so the RTP clock never stalls.
 *
 * Same cadence contract as AudioFeeder, without ffmpeg.
 */
/** Cap on buffered audio (~20 s at 16 kHz / 320-frame chunks). */
const MAX_QUEUED_CHUNKS = 1000;
/**
 * Chunks to hold before playback starts, and again after the queue runs dry
 * (~120 ms at 16 kHz). A realtime model delivers audio in bursts; without a
 * small jitter buffer every gap between bursts becomes an audible break in
 * the middle of a word.
 */
const JITTER_CHUNKS = 6;
/** Never stall longer than this waiting for the buffer to fill. */
const MAX_REBUFFER_MS = 400;
export class StreamFeeder {
    #queue = [];
    #partial = null;
    #partialLen = 0;
    #emitTimer = null;
    #nextEmitAtMs = 0;
    #running = false;
    #chunkSamples = 320;
    #chunkIntervalMs = 20;
    #onChunk = null;
    droppedChunks = 0;
    underflowChunks = 0;
    chunksEmitted = 0;
    rebufferCount = 0;
    /** True while waiting for the jitter buffer to fill; silence goes out. */
    #buffering = true;
    #bufferingSinceMs = 0;
    /** Chunks waiting to be sent (each is one frame interval of audio). */
    get queuedChunks() {
        return this.#queue.length;
    }
    /**
     * Starts metering. Safe to call after `push()`: audio pushed before the
     * WASM opens capture is kept and sent first.
     */
    start = (sampleRate, channels, framesPerChunk, onChunk) => {
        if (this.#running)
            return;
        const chunkSamples = framesPerChunk * channels;
        if (chunkSamples !== this.#chunkSamples) {
            // Re-chunk anything buffered under the old size.
            const buffered = this.#drainAll();
            this.#chunkSamples = chunkSamples;
            this.push(buffered);
        }
        this.#chunkIntervalMs = (framesPerChunk / sampleRate) * 1000;
        this.#onChunk = onChunk;
        this.#running = true;
        this.#nextEmitAtMs = 0;
        this.#buffering = true;
        this.#bufferingSinceMs = Date.now();
        this.#scheduleNext();
    };
    stop = () => {
        this.#running = false;
        if (this.#emitTimer) {
            clearTimeout(this.#emitTimer);
            this.#emitTimer = null;
        }
        this.#onChunk = null;
        this.#buffering = true;
        this.clear();
    };
    /** Queue PCM samples (mono Float32 in [-1, 1] at the capture rate). */
    push = (pcm) => {
        let offset = 0;
        while (offset < pcm.length) {
            if (!this.#partial) {
                this.#partial = new Float32Array(this.#chunkSamples);
                this.#partialLen = 0;
            }
            const take = Math.min(this.#chunkSamples - this.#partialLen, pcm.length - offset);
            this.#partial.set(pcm.subarray(offset, offset + take), this.#partialLen);
            this.#partialLen += take;
            offset += take;
            if (this.#partialLen === this.#chunkSamples) {
                if (this.#queue.length >= MAX_QUEUED_CHUNKS) {
                    this.droppedChunks += 1;
                }
                else {
                    this.#queue.push(this.#partial);
                }
                this.#partial = null;
                this.#partialLen = 0;
            }
        }
    };
    /** Pad and queue a trailing partial chunk, e.g. at the end of an utterance. */
    flush = () => {
        if (!this.#partial || this.#partialLen === 0)
            return;
        this.#partial.fill(0, this.#partialLen);
        this.#queue.push(this.#partial);
        this.#partial = null;
        this.#partialLen = 0;
    };
    /** Drop everything buffered — used for barge-in. */
    clear = () => {
        this.#queue = [];
        this.#partial = null;
        this.#partialLen = 0;
        // The next audio is a fresh utterance: buffer it before playing.
        this.#buffering = true;
        this.#bufferingSinceMs = Date.now();
    };
    #drainAll = () => {
        const total = this.#queue.length * this.#chunkSamples + this.#partialLen;
        const out = new Float32Array(total);
        let at = 0;
        for (const chunk of this.#queue) {
            out.set(chunk, at);
            at += chunk.length;
        }
        if (this.#partial)
            out.set(this.#partial.subarray(0, this.#partialLen), at);
        this.clear();
        return out;
    };
    #scheduleNext = () => {
        if (!this.#running)
            return;
        const now = Date.now();
        if (this.#nextEmitAtMs === 0)
            this.#nextEmitAtMs = now;
        const delayMs = Math.max(0, this.#nextEmitAtMs - now);
        this.#emitTimer = setTimeout(() => {
            this.#emitTimer = null;
            this.#flushOne();
            this.#nextEmitAtMs += this.#chunkIntervalMs;
            // After a long event-loop stall, resync instead of bursting to catch up.
            if (Date.now() - this.#nextEmitAtMs > 200)
                this.#nextEmitAtMs = Date.now();
            this.#scheduleNext();
        }, delayMs);
    };
    #startBuffering = () => {
        if (this.#buffering) {
            // Keep a clock running even if we were already waiting, or the
            // time-based escape hatch below never fires.
            if (this.#bufferingSinceMs === 0)
                this.#bufferingSinceMs = Date.now();
            return;
        }
        this.#buffering = true;
        this.#bufferingSinceMs = Date.now();
        this.rebufferCount += 1;
    };
    /** Silence while the buffer refills, so speech plays as one piece. */
    #shouldHoldForBuffer = () => {
        if (!this.#buffering)
            return false;
        if (this.#bufferingSinceMs === 0)
            this.#bufferingSinceMs = Date.now();
        const filled = this.#queue.length >= JITTER_CHUNKS;
        const waitedTooLong = Date.now() - this.#bufferingSinceMs > MAX_REBUFFER_MS;
        if (filled || waitedTooLong) {
            this.#buffering = false;
            this.#bufferingSinceMs = 0;
            return false;
        }
        return true;
    };
    #flushOne = () => {
        if (this.#shouldHoldForBuffer()) {
            this.#onChunk?.(new Float32Array(this.#chunkSamples));
            return;
        }
        let chunk = this.#queue.shift();
        if (!chunk) {
            chunk = new Float32Array(this.#chunkSamples);
            this.underflowChunks += 1;
            // Refill before resuming, instead of alternating speech and silence.
            this.#startBuffering();
        }
        this.chunksEmitted += 1;
        this.#onChunk?.(chunk);
    };
}
