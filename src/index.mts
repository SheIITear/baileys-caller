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
import { randomBytes, createHmac } from "node:crypto";
import { resolve } from "node:path";

import { WasmEngine } from "./wasm-engine.mjs";
import { RelayRtcTransport, type RelayListUpdatePayload } from "./relay-transport.mjs";
import { SignalingBridge } from "./signaling.mjs";
import { AudioFeeder } from "./audio-feeder.mjs";
import { StreamFeeder } from "./stream-feeder.mjs";
import { CallState, type VoipSdkConfig } from "./types.mjs";

export type { VoipSdkConfig, CallOptions, CallEvents, AudioConfig } from "./types.mjs";
export { CallState } from "./types.mjs";

const SHA256_LEN = 32;

/**
 * How long to wait for the stack to confirm a local hang-up before ending the
 * call ourselves. Without this, a hang-up the stack never reports would leave
 * `ended` unfired and `waitForEnd()` pending forever.
 */
const HANGUP_CONFIRM_MS = 2500;

const loadBaileys = async (): Promise<any> => {
  try {
    return await import("@whiskeysockets/baileys");
  } catch {
    throw new Error(
      "Could not import @whiskeysockets/baileys. Install it as a peer dependency.",
    );
  }
};

const toBareJid = (jid: string): string => {
  if (!jid) return jid;
  const at = jid.indexOf("@");
  if (at < 0) return jid;
  const user = jid.slice(0, at).split(":")[0];
  return `${user}@${jid.slice(at + 1)}`;
};

const computeHkdf = (
  key: Uint8Array,
  salt: Uint8Array | null,
  info: Uint8Array,
  length: number,
): Uint8Array => {
  const effectiveSalt = salt && salt.length > 0 ? Buffer.from(salt) : Buffer.alloc(SHA256_LEN, 0);
  const prk = createHmac("sha256", effectiveSalt).update(key).digest();
  const blocks = Math.ceil(length / SHA256_LEN);
  const okm = Buffer.alloc(blocks * SHA256_LEN);
  let prev = Buffer.alloc(0);
  for (let i = 1; i <= blocks; i += 1) {
    prev = createHmac("sha256", prk)
      .update(prev)
      .update(info)
      .update(Buffer.from([i]))
      .digest();
    prev.copy(okm, (i - 1) * SHA256_LEN);
  }
  return new Uint8Array(okm.buffer, okm.byteOffset, length);
};

const computeHmacSha256 = (data: Uint8Array, key: Uint8Array): Uint8Array => {
  const result = createHmac("sha256", Buffer.from(key)).update(data).digest();
  return new Uint8Array(result.buffer, result.byteOffset, result.byteLength);
};

const isCallReceiptNode = (node: any): boolean => {
  if (node?.tag !== "receipt") return false;
  const child = Array.isArray(node.content) ? node.content[0] : null;
  return !!(child?.attrs?.["call-id"] || child?.attrs?.call_id);
};

/** A live or recently-ended call. */
export class ActiveCall extends EventEmitter {
  #state: CallState = CallState.Idle;
  #endResolver!: (reason: string) => void;
  readonly #endPromise: Promise<string>;
  #endTimer: NodeJS.Timeout | null = null;
  #hangUpTimer: NodeJS.Timeout | null = null;
  #hangUpRequested = false;
  #ended = false;

  /** @internal mirrors the source path for the audio feeder */
  _audioSource: string = "silence";

  /** @internal set when `audioSource` is `"stream"` */
  _stream: StreamFeeder | null = null;

  constructor(
    public readonly callId: string,
    private readonly engine: WasmEngine,
    durationMs: number,
  ) {
    super();
    this.#endPromise = new Promise((res) => { this.#endResolver = res; });
    if (durationMs > 0) {
      this.#endTimer = setTimeout(() => this.end(), durationMs);
    }
  }

  get state(): CallState { return this.#state; }

  end = (): void => {
    if (this.#ended || this.#hangUpRequested) return;
    this.#hangUpRequested = true;
    if (this.#endTimer) { clearTimeout(this.#endTimer); this.#endTimer = null; }
    try { this.engine.endCall(0, true); } catch {}
    // Normally the stack reports Idle/Ending right after and _forceEnd runs;
    // this only covers the case where it never does.
    this.#hangUpTimer = setTimeout(() => this._forceEnd("hangup"), HANGUP_CONFIRM_MS);
    this.#hangUpTimer.unref?.();
  };

  mute = (muted: boolean): void => {
    try { this.engine.setMute(muted); } catch {}
  };

  waitForEnd = (): Promise<string> => this.#endPromise;

  /**
   * Stream mode only (`audioSource: "stream"`): queue mono Float32 PCM at
   * 16 kHz for the uplink. Audio pushed before the call connects is kept.
   */
  pushAudio = (pcm: Float32Array): void => {
    if (!this._stream) throw new Error('pushAudio requires audioSource: "stream"');
    this._stream.push(pcm);
  };

  /** Stream mode only: queue a trailing partial frame (end of an utterance). */
  flushAudio = (): void => { this._stream?.flush(); };

  /** Stream mode only: drop queued uplink audio, e.g. when the user barges in. */
  clearAudio = (): void => { this._stream?.clear(); };

  /** Stream mode only: chunks (20 ms each at 16 kHz) still waiting to be sent. */
  get queuedAudioChunks(): number { return this._stream?.queuedChunks ?? 0; }

  /** @internal — called by VoipClient on WASM call-state change */
  _updateState = (state: number): void => {
    this.#state = state as CallState;
    if (state === CallState.PreacceptReceived) this.emit("ringing");
    else if (state === CallState.Active) this.emit("connected");
    else if (state === CallState.Idle || state === CallState.Ending) {
      this._forceEnd("ended");
    }
  };

  /** @internal */
  _emitAudio = (pcm: Float32Array): void => { this.emit("audio", pcm); };

  /** @internal */
  _forceEnd = (reason: string): void => {
    if (this.#ended) return;
    this.#ended = true;
    if (this.#endTimer) { clearTimeout(this.#endTimer); this.#endTimer = null; }
    if (this.#hangUpTimer) { clearTimeout(this.#hangUpTimer); this.#hangUpTimer = null; }
    this.emit("ended", reason);
    this.#endResolver(reason);
  };
}

/** Top-level client. Connects to WhatsApp and lets you place calls. */
export class VoipClient {
  readonly #config: VoipSdkConfig;
  #engine: WasmEngine | null = null;
  #relay: RelayRtcTransport | null = null;
  #signaling: SignalingBridge | null = null;
  #sock: any = null;
  #activeCall: ActiveCall | null = null;
  #baileys: any = null;

  // Capture state populated when WASM negotiates audio params
  #capturePtr = 0;
  #captureChunkBytes = 0;
  #captureSampleRate = 16000;
  #captureChannels = 1;
  #captureFramesPerChunk = 320;
  #feeder: AudioFeeder | StreamFeeder | null = null;
  #wsHandlers: Array<[string, (node: any) => void]> = [];
  #ownsSocket = false;

  constructor(config: VoipSdkConfig = {}) {
    this.#config = config;
  }

  /** True once the VoIP stack is up (after connect() or attach()). */
  get ready(): boolean { return this.#engine !== null; }

  /** Connect to WhatsApp and bring up the WASM VoIP stack. */
  connect = async (): Promise<void> => {
    if (!this.#config.authDir) throw new Error("connect() requires authDir; use attach(sock) for an existing socket.");
    this.#ownsSocket = true;
    this.#baileys = await loadBaileys();
    const { useMultiFileAuthState, default: makeWASocket, DisconnectReason } = this.#baileys;
    const makeSocket: (opts: any) => any =
      makeWASocket ?? this.#baileys.makeWASocket ?? this.#baileys;

    const authDir = resolve(this.#config.authDir as string);
    const { state, saveCreds } = await useMultiFileAuthState(authDir);

    const silentLogger: any = {
      level: "silent",
      child: () => silentLogger,
      trace: () => {},
      debug: () => {},
      info: () => {},
      warn: () => {},
      error: () => {},
      fatal: () => {},
    };

    const createSocket = () => makeSocket({
      auth: state,
      emitOwnEvents: true,
      logger: silentLogger,
    });

    // Connect with auto-reconnect on the post-QR 515 stream-error path.
    await new Promise<void>((resolveOpen, rejectOpen) => {
      let opened = false;
      let retries = 0;
      const maxRetries = 5;

      const connectSocket = () => {
        this.#sock = createSocket();
        this.#sock.ev.on("creds.update", saveCreds);

        process.removeAllListeners("uncaughtException");
        process.on("uncaughtException", (err: any) => {
          const code = err?.output?.statusCode ?? err?.data?.attrs?.code;
          if ((code === 515 || code === "515") && !opened && retries < maxRetries) {
            retries += 1;
            setTimeout(connectSocket, 1500);
          } else if (!opened) {
            rejectOpen(err);
          }
        });

        this.#sock.ev.on("connection.update", (update: any) => {
          if (update.qr) {
            void import("qrcode-terminal")
              .then((qrt) => (qrt.default ?? qrt).generate(update.qr, { small: true }))
              .catch(() => {
                console.log("Scan this QR code in WhatsApp > Linked Devices:");
                console.log(update.qr);
              });
          }
          if (update.connection === "open") {
            opened = true;
            process.removeAllListeners("uncaughtException");
            resolveOpen();
            return;
          }
          if (update.connection === "close" && !opened) {
            const statusCode = update.lastDisconnect?.error?.output?.statusCode;
            const shouldReconnect =
              statusCode === 515 || statusCode === DisconnectReason?.restartRequired;
            if (shouldReconnect && retries < maxRetries) {
              retries += 1;
              setTimeout(connectSocket, 1000);
            } else {
              rejectOpen(update.lastDisconnect?.error ?? new Error("socket closed before open"));
            }
          }
        });
      };

      connectSocket();
    });

    await this.#bootstrap();
  };

  /**
   * Bring up the VoIP stack on a Baileys socket the caller already owns and
   * keeps alive (auth, reconnects, QR). The socket must be open. Never touches
   * process-wide handlers. Call detach() before the socket is replaced.
   */
  attach = async (sock: any): Promise<void> => {
    if (this.#engine) throw new Error("Already attached. Call detach() first.");
    if (!sock?.ws || !sock?.authState?.creds?.me?.id) {
      throw new Error("attach() needs an open, authenticated Baileys socket.");
    }
    this.#ownsSocket = false;
    this.#sock = sock;
    await this.#bootstrap();
  };

  /** Tear down the VoIP stack but leave an attached socket running. */
  detach = (): void => {
    this.#activeCall?._forceEnd("disconnect");
    this.#activeCall = null;
    this.#handleAudioCaptureStop();
    for (const [event, handler] of this.#wsHandlers) {
      try { this.#sock?.ws?.off?.(event, handler); } catch {}
    }
    this.#wsHandlers = [];
    this.#relay?.closeAll();
    this.#engine?.destroy();
    this.#engine = null;
    this.#relay = null;
    this.#signaling = null;
    if (!this.#ownsSocket) this.#sock = null;
  };

  #bootstrap = async (): Promise<void> => {
    this.#signaling = new SignalingBridge({ sock: this.#sock });
    await this.#signaling.init();

    this.#relay = new RelayRtcTransport({
      onTransportMessage: (data, ip, port) => this.#engine?.handleOnTransportMessage(data, ip, port),
      onIceRtt: (rttMs, ip, port) => this.#engine?.updateIceRtt(rttMs, ip, port),
    });

    this.#engine = new WasmEngine({
      pthreadPoolSize: this.#config.pthreadPoolSize,
      runtimePthreadPoolSize: this.#config.runtimePthreadPoolSize,
      callbacks: {
        onSignalingXmpp: (peerJid, callId, xmlPayload) =>
          this.#signaling!.sendSignaling(peerJid, callId, xmlPayload),
        onCallEvent: (eventType, eventData) => this.#handleCallEvent(eventType, eventData),
        sendDataToRelay: (data, ip, port) => this.#relay!.send(data, ip, port),
        onAudioCaptureInit: (config) => this.#handleAudioCaptureInit(config),
        onAudioCaptureStart: () => this.#handleAudioCaptureStart(),
        onAudioCaptureStop: () => this.#handleAudioCaptureStop(),
        onAudioPlaybackData: (audioData) => this.#activeCall?._emitAudio(audioData),
        cryptoHkdf: computeHkdf,
        hmacSha256: computeHmacSha256,
      },
    });

    await this.#engine.initialize();
    this.#signaling.attachEngine(this.#engine);

    const selfPnJid = this.#sock.authState.creds.me?.id;
    const selfLidJid = this.#sock.authState.creds.me?.lid;
    this.#engine.initVoipStack(selfPnJid, toBareJid(selfPnJid), selfLidJid);
    await this.#engine.waitForVoipStackReady();
    try { this.#engine.updateNetworkMedium(2, 0); } catch {}

    const onCall = (node: any) => {
      if (!this.#signaling || !this.#engine) return;
      this.#signaling.processIncomingCall(node, this.#engine, this.#activeCall?.callId ?? "");
    };
    const onReceipt = (node: any) => {
      if (!isCallReceiptNode(node) || !this.#signaling || !this.#engine) return;
      this.#signaling.processIncomingReceipt(node, this.#engine, this.#activeCall?.callId ?? "");
    };
    this.#sock.ws.on("CB:call", onCall);
    this.#sock.ws.on("CB:receipt", onReceipt);
    this.#wsHandlers = [["CB:call", onCall], ["CB:receipt", onReceipt]];
  };

  /** Place an outbound voice call. */
  call = async (
    phoneNumber: string,
    opts: { audioSource?: string; durationMs?: number } = {},
  ): Promise<ActiveCall> => {
    if (!this.#engine || !this.#signaling) throw new Error("Not connected. Call connect() first.");
    if (this.#activeCall) throw new Error("A call is already active.");

    const targetNumber = phoneNumber.replace(/\D/g, "");
    const targetPnJid = `${targetNumber}@s.whatsapp.net`;
    const durationMs = opts.durationMs ?? 120_000;
    const audioSource = opts.audioSource ?? "silence";

    const peerLid = await this.#signaling.resolveLid(targetPnJid);
    if (!peerLid) throw new Error(`Could not resolve LID for ${targetPnJid}`);

    for (const jid of [targetPnJid, peerLid]) {
      try { await this.#sock.presenceSubscribe(jid); } catch {}
    }
    await new Promise((r) => setTimeout(r, 750));

    const peerDeviceJids = await this.#signaling.discoverPeerDevices(peerLid);
    const deviceList = peerDeviceJids.length ? peerDeviceJids : [toBareJid(peerLid)];

    await this.#signaling.ensureSessionsForPeers(deviceList);

    await new Promise((r) => setTimeout(r, 500));
    await this.#signaling.issueTcToken(peerLid);
    const tcToken = await this.#signaling.ensureTcToken(peerLid, targetPnJid);

    const callId = ("00" + randomBytes(16).toString("hex").slice(2)).toUpperCase();

    const call = new ActiveCall(callId, this.#engine, durationMs);
    call._audioSource = audioSource;
    if (audioSource === "stream") call._stream = new StreamFeeder();
    this.#activeCall = call;
    call.once("ended", () => {
      if (this.#activeCall === call) this.#activeCall = null;
      // Each relay connection is a native WebRTC peer with its own threads and
      // buffers; left open they outlive the call until the next relay list.
      this.#relay?.closeConnections();
    });

    this.#engine.startCall({
      peerJid: peerLid,
      peerPn: targetPnJid,
      peerList: deviceList,
      callId,
      isVideo: false,
      isLidCall: true,
      isFromDialer: false,
      extraData: tcToken,
    });

    return call;
  };

  /** Tear down the WhatsApp socket and release resources. */
  disconnect = (): void => {
    this.detach();
    if (this.#ownsSocket) this.#sock?.end?.();
    this.#sock = null;
  };

  // ─── private ──────────────────────────────────────────────────────────────

  #handleCallEvent = (eventType: number, eventData?: string): void => {
    if (eventType === 16 && eventData) {
      try {
        const parsed = JSON.parse(eventData);
        const info = parsed.call_info ?? parsed.callInfo ?? {};
        const callState = Number(info.call_state ?? info.callState ?? 0);
        this.#activeCall?._updateState(callState);
      } catch {}
    } else if (eventType === 156 && eventData) {
      try {
        const update = JSON.parse(eventData) as RelayListUpdatePayload;
        this.#relay?.updateRelayList(update);
      } catch {}
    } else if (eventType === 2) {
      this.#activeCall?._forceEnd("remote_end");
    }
  };

  #handleAudioCaptureInit = (config: {
    sampleRate: number; channels: number; bitsPerSample: number; framesPerChunk: number;
  }): void => {
    if (!this.#engine) return;
    this.#captureSampleRate = config.sampleRate || 16000;
    this.#captureChannels = config.channels || 1;
    this.#captureFramesPerChunk = config.framesPerChunk || 320;
    const chunkSamples = this.#captureFramesPerChunk * this.#captureChannels;
    this.#captureChunkBytes = chunkSamples * Float32Array.BYTES_PER_ELEMENT;
    this.#capturePtr = this.#engine.malloc(this.#captureChunkBytes);
  };

  #handleAudioCaptureStart = (): void => {
    if (!this.#engine || !this.#capturePtr) return;
    const stream = this.#activeCall?._stream;
    if (stream) {
      stream.start(
        this.#captureSampleRate,
        this.#captureChannels,
        this.#captureFramesPerChunk,
        (chunk) => {
          if (this.#engine && this.#capturePtr) this.#engine.sendAudioData(chunk, this.#capturePtr);
        },
      );
      this.#feeder = stream;
      return;
    }
    const audioSource = this.#activeCall?._audioSource ?? "silence";
    this.#feeder = new AudioFeeder(
      this.#captureSampleRate,
      this.#captureChannels,
      this.#captureFramesPerChunk,
      (chunk) => {
        if (this.#engine && this.#capturePtr) this.#engine.sendAudioData(chunk, this.#capturePtr);
      },
      audioSource,
    );
    this.#feeder.start();
  };

  #handleAudioCaptureStop = (): void => {
    this.#feeder?.stop();
    this.#feeder = null;
    if (this.#engine && this.#capturePtr) {
      try { this.#engine.free(this.#capturePtr); } catch {}
      this.#capturePtr = 0;
    }
  };
}
