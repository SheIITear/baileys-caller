/**
 * baileys-caller — WhatsApp voice calling for Node.js.
 *
 * Wraps WhatsApp Web's official VoIP WASM stack and routes signaling through
 * Baileys. Public surface:
 *
 *   const client = new VoipClient({ authDir })
 *   await client.connect()
 *   const call = await client.call("12345678901", { audioSource: "./hi.mp3" })
 *
 * @author ShellTear
 */
import { EventEmitter } from "node:events";
import { WasmEngine } from "./wasm-engine.mjs";
import { StreamFeeder } from "./stream-feeder.mjs";
import { CallState, type VoipSdkConfig } from "./types.mjs";
export type { VoipSdkConfig, CallOptions, CallEvents, AudioConfig } from "./types.mjs";
export { CallState } from "./types.mjs";
/** A live or recently-ended call. */
export declare class ActiveCall extends EventEmitter {
    #private;
    readonly callId: string;
    private readonly engine;
    /** @internal mirrors the source path for the audio feeder */
    _audioSource: string;
    /** @internal set when `audioSource` is `"stream"` */
    _stream: StreamFeeder | null;
    constructor(callId: string, engine: WasmEngine, durationMs: number);
    get state(): CallState;
    end: () => void;
    mute: (muted: boolean) => void;
    waitForEnd: () => Promise<string>;
    /**
     * Stream mode only (`audioSource: "stream"`): queue mono Float32 PCM at
     * 16 kHz for the uplink. Audio pushed before the call connects is kept.
     */
    pushAudio: (pcm: Float32Array) => void;
    /** Stream mode only: queue a trailing partial frame (end of an utterance). */
    flushAudio: () => void;
    /** Stream mode only: drop queued uplink audio, e.g. when the user barges in. */
    clearAudio: () => void;
    /** Stream mode only: chunks (20 ms each at 16 kHz) still waiting to be sent. */
    get queuedAudioChunks(): number;
    /** @internal — called by VoipClient on WASM call-state change */
    _updateState: (state: number) => void;
    /** @internal */
    _emitAudio: (pcm: Float32Array) => void;
    /** @internal */
    _forceEnd: (reason: string) => void;
}
/** Top-level client. Connects to WhatsApp and lets you place calls. */
export declare class VoipClient {
    #private;
    constructor(config?: VoipSdkConfig);
    /** True once the VoIP stack is up (after connect() or attach()). */
    get ready(): boolean;
    /** Connect to WhatsApp and bring up the WASM VoIP stack. */
    connect: () => Promise<void>;
    /**
     * Bring up the VoIP stack on a Baileys socket the caller already owns and
     * keeps alive (auth, reconnects, QR). The socket must be open. Never touches
     * process-wide handlers. Call detach() before the socket is replaced.
     */
    attach: (sock: any) => Promise<void>;
    /** Tear down the VoIP stack but leave an attached socket running. */
    detach: () => void;
    /** Place an outbound voice call. */
    call: (phoneNumber: string, opts?: {
        audioSource?: string;
        durationMs?: number;
    }) => Promise<ActiveCall>;
    /** Tear down the WhatsApp socket and release resources. */
    disconnect: () => void;
}
