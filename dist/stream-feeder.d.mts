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
export declare class StreamFeeder {
    #private;
    droppedChunks: number;
    underflowChunks: number;
    chunksEmitted: number;
    rebufferCount: number;
    /** Chunks waiting to be sent (each is one frame interval of audio). */
    get queuedChunks(): number;
    /**
     * Starts metering. Safe to call after `push()`: audio pushed before the
     * WASM opens capture is kept and sent first.
     */
    start: (sampleRate: number, channels: number, framesPerChunk: number, onChunk: (chunk: Float32Array) => void) => void;
    stop: () => void;
    /** Queue PCM samples (mono Float32 in [-1, 1] at the capture rate). */
    push: (pcm: Float32Array) => void;
    /** Pad and queue a trailing partial chunk, e.g. at the end of an utterance. */
    flush: () => void;
    /** Drop everything buffered — used for barge-in. */
    clear: () => void;
}
