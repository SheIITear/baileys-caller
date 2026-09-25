/**
 * WhatsApp VoIP WASM engine.
 *
 * Loads the WhatsApp Web VoIP WASM stack inside a Node.js `vm.Context`,
 * spawns a 20-thread `worker_threads` pool to mirror the browser's pthread
 * model, and exposes a callback-based JS bridge. Audio-only (no video).
 *
 * @author ShellTear
 */
import * as vm from "node:vm";
import * as fs from "node:fs";
import * as path from "node:path";
import { randomFillSync } from "node:crypto";
import { Worker } from "node:worker_threads";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const CALL_WASM_AB_PROPS_JSON = process.env.CALL_WASM_AB_PROPS_JSON ?? "";
/** WhatsApp Web's own pool size; the default when `pthreadPoolSize` is not set. */
const DEFAULT_PTHREAD_POOL_SIZE = 20;
const VOIP_READY_TIMEOUT_MS = 15_000;

const parseJsonObjectEnv = (raw: string): Record<string, boolean | number | string> => {
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed;
  } catch {}
  return {};
};

export type WasmAudioConfig = {
  sampleRate: number;
  channels: number;
  bitsPerSample: number;
  framesPerChunk: number;
};

export type WasmEngineCallbacks = {
  onSignalingXmpp?: (peerJid: string, callId: string, xmlPayload: Uint8Array) => void;
  onCallEvent?: (eventType: number, eventData?: string) => void;
  onVoipReady?: () => void;
  sendDataToRelay?: (data: Uint8Array, ip: string, port: number) => number;
  onLog?: (level: string, message: string) => void;
  onAudioCaptureInit?: (config: WasmAudioConfig) => void;
  onAudioCaptureStart?: () => void;
  onAudioCaptureStop?: () => void;
  onAudioPlaybackInit?: (config: WasmAudioConfig) => void;
  onAudioPlaybackStart?: () => void;
  onAudioPlaybackStop?: () => void;
  onAudioPlaybackData?: (audioData: Float32Array) => void;
  cryptoHkdf?: (key: Uint8Array, salt: Uint8Array | null, info: Uint8Array, length: number) => Uint8Array;
  hmacSha256?: (data: Uint8Array, key: Uint8Array) => Uint8Array;
};

export type WasmEngineConfig = {
  resourcesPath?: string;
  wasmPath?: string;
  wasmBinary?: Uint8Array;
  loaderCode?: string;
  workerModulesCode?: string;
  loaderModuleName?: string;
  callbacks?: WasmEngineCallbacks;
  enableLogs?: boolean;
  /**
   * Workers this engine pre-loads for the `__NODE_PTHREAD` hooks. WhatsApp's
   * runtime starts its threads through `createDedicatedWebWorker` instead
   * (see `runtimePthreadPoolSize`), so these only cost memory: each is a V8
   * isolate holding the VoIP module (~17 MB). Defaults to 20; 0 is valid.
   */
  pthreadPoolSize?: number;
  /**
   * Workers WhatsApp's runtime starts up front for its pthreads (its
   * `pthreadPoolSizeOverride`, default 20). The runtime starts more on demand
   * when a thread is needed beyond these, so a smaller pool trades memory for
   * a slower first use. `workerStats().dedicated` shows how many exist.
   */
  runtimePthreadPoolSize?: number;
  options?: {
    heartbeatInterval?: number;
    lobbyTimeout?: number;
    maxParticipantsScreenShare?: number;
    maxGroupSizeLongRingtone?: number;
    logLevel?: number;
  };
};

const toByteArray = (input: any): Uint8Array => {
  if (!input) return new Uint8Array(0);
  if (input instanceof Uint8Array) return input;
  if (typeof input === "string") return new TextEncoder().encode(input);
  if (ArrayBuffer.isView(input)) return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (typeof input === "object" && typeof input.length === "number") {
    const arr = new Uint8Array(input.length);
    for (let i = 0; i < input.length; i += 1) arr[i] = input[i] ?? 0;
    return arr;
  }
  return new Uint8Array(0);
};

const filterWorkerStderr = (chunk: Buffer): void => {
  const line = chunk.toString().trim();
  if (line && !line.startsWith("voip:") && !line.startsWith("still waiting")) {
    process.stderr.write(chunk);
  }
};

const resolveWorkerScriptPath = (): string => {
  const compiled = path.join(__dirname, "worker-bootstrap.mjs");
  return fs.existsSync(compiled) ? compiled : path.join(__dirname, "worker-bootstrap.mts");
};

class NodeWorkerMessagePort {
  readonly #listeners = new Map<string, Set<(data: any) => void>>();
  readonly #worker: any;
  readonly fullyConnected: Promise<NodeWorkerMessagePort>;
  readonly name: string;
  workerID = 0;
  pthread_ptr = 0;

  readonly #onCallback: ((callbackName: string, data: any) => void) | undefined;

  /**
   * `onCallback` routes the worker's callbacks to the engine that owns it. Without
   * it they only reach the static listeners, which cannot tell engines apart.
   */
  constructor(worker: any, name = "WAWebVoipWebWasmWorker", onCallback?: (callbackName: string, data: any) => void) {
    this.#worker = worker;
    this.name = name;
    this.#onCallback = onCallback;
    this.fullyConnected = new Promise((resolve) => {
      const loadedHandler = (msg: any) => {
        if (msg && msg.cmd === "loaded") {
          this.workerID = msg.workerID ?? 0;
          resolve(this);
        }
      };
      this.addMessageListener("cmd", loadedHandler);
    });
    if (typeof worker.on === "function") {
      worker.on("message", (data: any) => this.#handleMessage(data));
      worker.on("error", () => {});
    } else if (typeof worker.addEventListener === "function") {
      worker.addEventListener("message", (ev: any) => this.#handleMessage(ev?.data ?? ev));
    }
  }

  postMessage = (msg: any, transferList?: any[]): void => {
    const out = msg && typeof msg === "object" && msg.cmd && !msg.type ? { ...msg, type: "cmd" } : msg;
    this.#worker.postMessage(out, transferList as any);
  };

  addMessageListener = (type: string, handler: (data: any) => void): typeof handler => {
    let set = this.#listeners.get(type);
    if (!set) { set = new Set(); this.#listeners.set(type, set); }
    set.add(handler);
    return handler;
  };

  removeMessageListener = (type: string, handler: (data: any) => void): boolean =>
    this.#listeners.get(type)?.delete(handler) ?? false;

  removeAllMessageListeners = (type?: string): void => {
    if (type) this.#listeners.get(type)?.clear();
    else this.#listeners.clear();
  };

  terminate = (): void => { this.#worker.terminate(); };
  close = (): void => {};
  isWrappingVirtualMessagePort = (): boolean => false;
  getWorker = (): Worker => this.#worker;

  #handleMessage = (data: any): void => {
    if (!data || typeof data !== "object") return;

    if (data.type === "callback" || data.type === "waWasmWorkerCompatibleCallback") {
      let callbackName: string;
      let callbackArgs: any;

      if (data.__name) {
        callbackName = data.__name;
        callbackArgs = {};
        for (const key in data) {
          if (key !== "type" && key !== "__name" && key !== "prototype" && key !== "args" && !key.startsWith("__")) {
            callbackArgs[key] = data[key];
          }
        }
      } else if (data.name) {
        callbackName = data.name;
        callbackArgs = data.args ?? {};
      } else if (data.payload?.name) {
        callbackName = data.payload.name;
        callbackArgs = data.payload.args ?? {};
      } else {
        return;
      }

      if (callbackName === "onSignalingXmpp" &&
          (!callbackArgs || Object.keys(callbackArgs).length === 0)) {
        callbackArgs = {
          peerJid: data.peerJid, callId: data.callId, xmlPayload: data.xmlPayload,
        };
      }

      let listenerData: any = callbackArgs;
      if (!callbackArgs || Object.keys(callbackArgs).length === 0 ||
          (Object.keys(callbackArgs).length === 1 && callbackArgs.prototype)) {
        listenerData = {};
        for (const key in data) {
          if (key !== "type" && key !== "__name" && key !== "prototype" && key !== "args" && !key.startsWith("__")) {
            listenerData[key] = data[key];
          }
        }
      } else if (callbackName === "sendDataToRelay") {
        listenerData = { ...callbackArgs };
        if (data.data !== undefined) listenerData.data = data.data;
        if (data.len !== undefined) listenerData.len = data.len;
        if (data.ip !== undefined) listenerData.ip = data.ip;
        if (data.port !== undefined) listenerData.port = data.port;
      } else if (callbackName === "onCallEvent") {
        listenerData = { ...callbackArgs };
        if (data.eventType !== undefined) listenerData.eventType = data.eventType;
        if (data.userData !== undefined) listenerData.userData = data.userData;
        if (data.eventDataJson !== undefined) listenerData.eventDataJson = data.eventDataJson;
      }

      this.#onCallback?.(callbackName, listenerData);
      WasmEngine.notifyGlobalCallbackListeners(callbackName, listenerData);
      return;
    }

    const dispatch = (key?: string): void => {
      if (!key) return;
      for (const handler of this.#listeners.get(key) ?? []) {
        try { handler(data); } catch {}
      }
    };
    dispatch(data.type);
    if (data.cmd !== data.type) dispatch(data.cmd);
  };
}

export class WasmEngine {
  /**
   * Listeners outside any engine, kept for callers that register their own.
   * The engine's callbacks are per instance (see #callbackListeners): a static
   * registration captured the first engine forever, so after a detach/attach
   * every event went to the destroyed engine and kept it alive.
   */
  static readonly #globalCallbackListeners = new Map<string, Set<(data: any) => void>>();

  static registerGlobalCallbackListener = (callbackName: string, handler: (data: any) => void): void => {
    const key = `callback:${callbackName}`;
    let set = WasmEngine.#globalCallbackListeners.get(key);
    if (!set) { set = new Set(); WasmEngine.#globalCallbackListeners.set(key, set); }
    set.add(handler);
  };

  static notifyGlobalCallbackListeners = (callbackName: string, data: any): void => {
    const set = WasmEngine.#globalCallbackListeners.get(`callback:${callbackName}`);
    if (!set) return;
    for (const handler of set) {
      try { handler(data); } catch {}
    }
  };

  readonly #config: Required<Pick<WasmEngineConfig, "resourcesPath" | "wasmPath" | "enableLogs">> & WasmEngineConfig;
  #instance: any = null;
  #initialized = false;
  readonly #moduleRegistry = new Map<string, { deps: string[]; factory: Function; exports?: any }>();
  #vmContext: vm.Context | null = null;

  readonly #callbackListeners = new Map<string, (data: any) => void>();

  #unusedWorkers: NodeWorkerMessagePort[] = [];
  #runningWorkers: NodeWorkerMessagePort[] = [];
  /** Workers the VoIP code spawns itself; not part of the pthread pool. */
  #dedicatedWorkers = new Set<Worker>();
  readonly #pthreads: Record<number, NodeWorkerMessagePort> = {};
  #nextWorkerID = 1;

  #wasmModule: WebAssembly.Module | null = null;
  #wasmMemory: WebAssembly.Memory | null = null;
  #removeRunDependencyCallback: ((dep: string) => void) | null = null;
  #workersLoadedCount = 0;

  #audioPlaybackLoopInterval: NodeJS.Timeout | null = null;
  #audioPlaybackBuffer: number | null = null;
  #isPlaybackActive = false;

  #voipStackInitialized = false;
  #voipStackInitPromise: Promise<void> | null = null;
  #voipReadyResolver: (() => void) | null = null;
  #voipReadyPromise: Promise<void> | null = null;

  #workerModulesCode = "";
  #loaderCode = "";

  constructor(config: WasmEngineConfig = {}) {
    const basePath = config.resourcesPath
      ? (path.isAbsolute(config.resourcesPath) ? config.resourcesPath : path.resolve(process.cwd(), config.resourcesPath))
      : path.resolve(__dirname, "..");
    const wasmPath = config.wasmPath
      ? (path.isAbsolute(config.wasmPath) ? config.wasmPath : path.resolve(process.cwd(), config.wasmPath))
      : path.join(basePath, "assets", "wasm", "whatsapp.wasm");

    this.#config = {
      ...config,
      wasmPath,
      resourcesPath: basePath,
      enableLogs: config.enableLogs ?? true,
      options: {
        heartbeatInterval: 30,
        lobbyTimeout: 1,
        maxParticipantsScreenShare: 32,
        maxGroupSizeLongRingtone: 32,
        ...config.options,
      },
    };
  }

  initialize = async (): Promise<void> => {
    if (this.#initialized) throw new Error("WasmEngine already initialized");

    const voipStorageDir = "/tmp/voip";
    try { if (!fs.existsSync(voipStorageDir)) fs.mkdirSync(voipStorageDir, { recursive: true }); } catch {}

    const loaderFile = path.join(this.#config.resourcesPath, "assets", "wasm", "loader.js");
    const workerFile = path.join(this.#config.resourcesPath, "assets", "wasm", "worker-modules.js");

    if (!this.#config.wasmBinary && !fs.existsSync(this.#config.wasmPath)) {
      throw new Error(`WASM file not found: ${this.#config.wasmPath}`);
    }

    const wasmBuffer = this.#config.wasmBinary
      ? Buffer.from(this.#config.wasmBinary)
      : fs.readFileSync(this.#config.wasmPath);

    const diskWorkerCode = fs.existsSync(workerFile) ? fs.readFileSync(workerFile, "utf8") : "";
    this.#workerModulesCode = this.#config.workerModulesCode ?? diskWorkerCode;
    const workerBundleHasLoader =
      typeof this.#workerModulesCode === "string" && /WAWebVoipWebWasmLoader/.test(this.#workerModulesCode);

    // Skip the on-disk loader.js if the worker bundle already has a loader —
    // the standalone loader.js is older and would clobber the freshly fetched
    // bindings inside worker-modules.js.
    this.#loaderCode = this.#config.loaderCode ??
      (workerBundleHasLoader ? "" : fs.existsSync(loaderFile) ? fs.readFileSync(loaderFile, "utf8") : "");

    if (!this.#loaderCode && !this.#workerModulesCode) {
      throw new Error("No loader/worker code available to initialize VoIP");
    }

    const memory = new WebAssembly.Memory({ initial: 256, maximum: 32768, shared: true });
    this.#wasmMemory = memory;
    this.#wasmModule = await WebAssembly.compile(wasmBuffer);
    this.#vmContext = this.#createVMContext(memory);

    const runModuleCode = (code: string) => { if (code) vm.runInContext(code, this.#vmContext!); };
    runModuleCode(this.#workerModulesCode);
    runModuleCode(this.#loaderCode);

    (this.#vmContext as any).WAWebVoipWebWasmWorkerResource = this.#requireModule("WAWebVoipWebWasmWorkerResource");

    const loaderModuleNames = [
      this.#config.loaderModuleName,
      "WAWebVoipWebWasmLoader",
      "WAWebVoipWebWasmLoader.worker",
      "WAWebVoipWebWasmLoader_ProdLab_internal.worker",
      "WAWebVoipWebWasmLoader_ProdLabvideo_internal.worker",
    ].filter((v, i, a): v is string => !!v && a.indexOf(v) === i);

    let wasmLoader: any = null;
    for (const moduleName of loaderModuleNames) {
      const candidate = this.#requireModule(moduleName);
      if (typeof candidate === "function") { wasmLoader = candidate; break; }
      if (typeof candidate?.default === "function") { wasmLoader = candidate.default; break; }
    }
    if (typeof wasmLoader !== "function") {
      throw new Error(`No compatible WASM loader found. Tried: ${loaderModuleNames.join(", ")}`);
    }

    this.#registerWorkerCallbacks();
    await this.#initPThreadPool();
    const workersLoadingPromise = this.#loadWasmModuleToAllWorkers();

    const readyPromise = wasmLoader({
      wasmBinary: wasmBuffer,
      wasmMemory: memory,
      locateFile: () => this.#config.wasmPath,
      onRuntimeInitialized: () => {},
      ...(this.#config.runtimePthreadPoolSize !== undefined
        ? { pthreadPoolSizeOverride: this.#config.runtimePthreadPoolSize }
        : {}),
    });

    const [instance] = await Promise.all([readyPromise, workersLoadingPromise]);
    this.#instance = instance;
    this.#initialized = true;
  };

  isInitialized = (): boolean => this.#initialized;

  /** Worker threads alive now: size `pthreadPoolSize` from the peak `running` on real calls. */
  workerStats = (): { pool: number; running: number; unused: number; dedicated: number } => ({
    pool: this.#poolSize,
    running: this.#runningWorkers.length,
    unused: this.#unusedWorkers.length,
    dedicated: this.#dedicatedWorkers.size,
  });

  get #poolSize(): number {
    const size = this.#config.pthreadPoolSize;
    return size !== undefined && Number.isInteger(size) && size >= 0 ? size : DEFAULT_PTHREAD_POOL_SIZE;
  }

  #dispatchWorkerCallback = (callbackName: string, data: any): void => {
    const handler = this.#callbackListeners.get(callbackName);
    if (!handler) return;
    try { handler(data); } catch {}
  };

  destroy = (): void => {
    this.#stopAudioPlaybackLoop();
    if (this.#instance && typeof this.#instance.endCall === "function") {
      try { this.#instance.endCall(0, false); } catch {}
    }
    for (const worker of [...this.#runningWorkers, ...this.#unusedWorkers]) {
      try { worker.terminate(); } catch {}
    }
    for (const worker of this.#dedicatedWorkers) {
      try { void worker.terminate(); } catch {}
    }
    this.#dedicatedWorkers.clear();
    this.#callbackListeners.clear();
    this.#releasePendingWaits();
    // The process-wide copy would otherwise keep this engine alive until the next one replaces it.
    const g = global as any;
    if (this.#globalWasmCallbacks && g.WhatsAppVoipWasmCallbacks === this.#globalWasmCallbacks) {
      delete g.WhatsAppVoipWasmCallbacks;
      delete g.WhatsAppVoipWasmWorkerCompatibleCallbacks;
    }
    this.#globalWasmCallbacks = null;
    this.#runningWorkers = [];
    this.#unusedWorkers = [];
    this.#instance = null;
    this.#vmContext = null;
    this.#moduleRegistry.clear();
    this.#wasmModule = null;
    this.#wasmMemory = null;
    this.#initialized = false;
  };

  initVoipStack = (selfJid: string, meUserJid: string, selfLid: string): void => {
    this.#ensureInitialized();
    if (this.#voipStackInitialized || this.#voipStackInitPromise) return;

    this.#voipStackInitPromise = new Promise<void>((resolveInit) => {
      this.#voipReadyPromise = new Promise<void>((readyResolve) => {
        this.#voipReadyResolver = () => {
          this.#voipStackInitialized = true;
          this.#voipReadyResolver = null;
          this.#voipReadyPromise = null;
          readyResolve();
        };
      });

      try {
        this.#applyDefaultAbProps();
        try {
          this.#instance.initVoipStack(selfJid, meUserJid, selfLid);
        } catch (modernErr: any) {
          if (modernErr?.name === "BindingError" &&
              (String(modernErr?.message ?? "").includes("expected 8 args") ||
               String(modernErr?.message ?? "").includes("takes 8"))) {
            this.#instance.initVoipStack(selfJid, meUserJid, selfLid, true, 5, 0, 8, 16);
          } else {
            throw modernErr;
          }
        }

        Promise.race([
          this.#voipReadyPromise,
          new Promise<void>((r) => setTimeout(() => {
            this.#voipStackInitialized = true; r();
          }, VOIP_READY_TIMEOUT_MS)),
        ]).finally(() => {
          this.#voipStackInitPromise = null;
          resolveInit();
        });
      } catch {
        this.#voipReadyResolver = null;
        this.#voipReadyPromise = null;
        this.#voipStackInitPromise = null;
        resolveInit();
      }
    });
  };

  waitForVoipStackReady = async (): Promise<void> => {
    if (this.#voipStackInitialized) return;
    if (this.#voipStackInitPromise) {
      await this.#voipStackInitPromise;
    } else {
      await new Promise((r) => setTimeout(r, 100));
    }
  };

  isVoipStackReady = (): boolean => this.#voipStackInitialized;

  startCall = (options: {
    peerJid: string; peerPn: string; peerList?: string[];
    callId: string; isVideo: boolean; isLidCall?: boolean;
    isFromDialer?: boolean; extraData?: Uint8Array;
  }): unknown => {
    this.#ensureInitialized();
    const peers = this.#makeStringList(options.peerList ?? [options.peerJid]);
    const tcToken = this.#createUint8List(options.extraData);
    const isLidCall = options.isLidCall ?? options.peerJid.includes("@lid");
    const isFromDialer = options.isFromDialer ?? false;
    const peerJid = String(options.peerJid);

    try {
      try {
        return this.#instance.startVoipCall(
          peerJid, peers, options.callId, options.isVideo,
          options.peerPn, isLidCall, isFromDialer, tcToken,
        );
      } catch (error: any) {
        if (error?.name !== "BindingError") throw error;
        return this.#instance.startVoipCall(
          peerJid, peers, options.callId, options.isVideo,
          options.peerPn, isFromDialer, tcToken,
        );
      }
    } finally {
      peers?.delete?.();
      tcToken?.delete?.();
    }
  };

  endCall = (reason = 0, sendTerminate = true): void => {
    this.#ensureInitialized();
    this.#instance.endCall(reason, sendTerminate);
  };

  setMute = (muted: boolean): number => {
    this.#ensureInitialized();
    return this.#instance.setCallMute(muted);
  };

  updateNetworkMedium = (networkMedium: number, networkMtu = 0): void => {
    this.#ensureInitialized();
    this.#instance.updateNetworkMedium?.(networkMedium, networkMtu);
  };

  handleSignalingOffer = (msg: {
    payload: string; peerPlatform?: number; peerAppVersion?: string;
    epochId?: string; timestamp?: string; isOffline?: boolean;
    isOfferNotContact?: boolean; peerJid: string; tcToken?: Uint8Array;
  }): void => {
    this.#ensureInitialized();
    const tcTokenList = this.#createUint8List(msg.tcToken);
    try {
      this.#instance.handleIncomingSignalingOffer(
        msg.payload,
        String(msg.peerPlatform ?? 0),
        String(msg.peerAppVersion ?? "0"),
        String(msg.epochId ?? "0"),
        String(msg.timestamp ?? "0"),
        msg.isOffline ?? false,
        msg.isOfferNotContact ?? false,
        String(msg.peerJid),
        tcTokenList,
      );
    } finally {
      tcTokenList?.delete?.();
    }
  };

  handleSignalingMessage = (msg: {
    payload: string; peerPlatform?: string | number; peerAppVersion?: string;
    epochId?: string; timestamp?: string; isOffline?: boolean;
    peerJid: string; tcToken?: Uint8Array;
  }): void => {
    this.#ensureInitialized();
    const tcTokenList = this.#createUint8List(msg.tcToken);
    try {
      this.#instance.handleIncomingSignalingMessage(
        msg.payload,
        String(msg.peerPlatform ?? "0"),
        String(msg.peerAppVersion ?? "0"),
        String(msg.epochId ?? "0"),
        String(msg.timestamp ?? "0"),
        msg.isOffline ?? false,
        String(msg.peerJid),
        tcTokenList,
      );
    } finally {
      tcTokenList?.delete?.();
    }
  };

  handleSignalingAck = (msg: {
    payload: string; ackError?: string; msgType?: string;
    peerJid?: string; extraData?: Uint8Array;
  }): void => {
    this.#ensureInitialized();
    const options = this.#createUint8List(msg.extraData);
    try {
      this.#instance.handleIncomingSignalingAck(
        msg.payload,
        String(msg.ackError ?? "0"),
        String(msg.msgType ?? ""),
        msg.peerJid ?? "",
        options,
      );
    } finally {
      options?.delete?.();
    }
  };

  handleSignalingReceipt = (msg: { payload: string; peerJid: string; tcToken?: Uint8Array }): void => {
    this.#ensureInitialized();
    const tcTokenList = this.#createUint8List(msg.tcToken);
    try {
      this.#instance.handleIncomingSignalingReceipt?.(msg.payload, msg.peerJid, tcTokenList);
    } finally {
      tcTokenList?.delete?.();
    }
  };

  handleOnTransportMessage = (data: Uint8Array, ip: string, port: number): void => {
    this.#ensureInitialized();
    if (typeof this.#instance.handleOnMessageFromHeap === "function") {
      const ptr = this.malloc(data.byteLength);
      if (!ptr) return;
      try {
        const heapU8 = this.#instance.GROWABLE_HEAP_U8?.() ?? this.#instance.HEAPU8;
        if (!heapU8) return;
        heapU8.set(data, ptr);
        this.#instance.handleOnMessageFromHeap(ptr, data.byteLength, ip, port);
      } finally {
        this.free(ptr);
      }
      return;
    }
    if (typeof this.#instance.handleOnMessage !== "function") return;
    const dataList = this.#createUint8List(data);
    try {
      this.#instance.handleOnMessage(dataList, ip, port);
    } finally {
      dataList?.delete?.();
    }
  };

  updateIceRtt = (rttMs: number, relayIp: string, relayPort: number): void => {
    this.#ensureInitialized();
    this.#instance.updateIceRtt?.(rttMs, relayIp, relayPort);
  };

  sendAudioData = (data: Float32Array, ptr: number): void => {
    this.#ensureInitialized();
    if (!data || data.length === 0 || !ptr) return;
    if (typeof this.#instance.onAudioDataFromJs !== "function") return;
    try {
      const heapF32 = this.#instance.GROWABLE_HEAP_F32?.();
      if (!heapF32) return;
      const index = Math.floor(ptr / 4);
      if (index < 0 || index + data.length > heapF32.length) return;
      heapF32.set(data, index);
      this.#instance.onAudioDataFromJs(ptr, data.length);
    } catch {}
  };

  malloc = (size: number): number => {
    this.#ensureInitialized();
    return this.#instance._malloc(size);
  };

  free = (ptr: number): void => {
    this.#ensureInitialized();
    this.#instance._free(ptr);
  };

  // ─── private ──────────────────────────────────────────────────────────────

  #ensureInitialized = (): void => {
    if (!this.#initialized || !this.#instance) {
      throw new Error("WasmEngine not initialized. Call initialize() first.");
    }
  };

  #makeStringList = (arr: string[]): any => {
    const list = new this.#instance.StringList();
    for (const v of arr) list.push_back(v);
    return list;
  };

  #createUint8List = (data?: Uint8Array): any => {
    if (!this.#instance?.Uint8List) return null;
    const list = new this.#instance.Uint8List();
    if (data) data.forEach((byte) => list.push_back(byte));
    return list;
  };

  #startAudioPlaybackLoop = (): void => {
    if (this.#audioPlaybackLoopInterval) return;
    this.#ensureInitialized();
    this.#isPlaybackActive = true;
    if (typeof this.#instance.requestAudioDataFromWasmVoip !== "function") return;

    const framesPerChunk = 320;
    const bufferSize = framesPerChunk * 4;
    try {
      const _malloc = this.#instance._malloc ?? this.#instance.malloc;
      if (!_malloc) return;
      this.#audioPlaybackBuffer = _malloc(bufferSize);
    } catch {
      return;
    }
    if (!this.#audioPlaybackBuffer || this.#audioPlaybackBuffer <= 0) return;

    this.#audioPlaybackLoopInterval = setInterval(() => {
      if (!this.#isPlaybackActive || !this.#instance || !this.#initialized) {
        this.#stopAudioPlaybackLoop();
        return;
      }
      try {
        this.#instance.requestAudioDataFromWasmVoip(this.#audioPlaybackBuffer!, bufferSize);
        const heapF32 = this.#instance.GROWABLE_HEAP_F32?.();
        if (!heapF32) return;
        const index = Math.floor(this.#audioPlaybackBuffer! / 4);
        const numFloats = Math.floor(bufferSize / 4);
        if (index < 0 || index + numFloats > heapF32.length) return;
        const audioData = new Float32Array(heapF32.buffer, heapF32.byteOffset + index * 4, numFloats);
        const hasNonZero = audioData.some((s) => Math.abs(s) > 0.0001);
        if (hasNonZero) this.#config.callbacks?.onAudioPlaybackData?.(audioData);
      } catch {}
    }, 16);
  };

  #stopAudioPlaybackLoop = (): void => {
    this.#isPlaybackActive = false;
    if (this.#audioPlaybackLoopInterval) {
      clearInterval(this.#audioPlaybackLoopInterval);
      this.#audioPlaybackLoopInterval = null;
    }
    if (this.#audioPlaybackBuffer && this.#audioPlaybackBuffer > 0) {
      try { this.#instance?._free?.(this.#audioPlaybackBuffer); } catch {}
      this.#audioPlaybackBuffer = null;
    }
  };

  #applyDefaultAbProps = (): void => {
    if (!this.#instance) return;
    const setInt = typeof this.#instance.setABPropInt === "function"
      ? (k: string, v: number): void => { this.#instance.setABPropInt(k, v); }
      : null;
    const setBool = typeof this.#instance.setABPropBool === "function"
      ? (k: string, v: boolean): void => { this.#instance.setABPropBool(k, v); }
      : null;
    const setString = typeof this.#instance.setABPropString === "function"
      ? (k: string, v: string): void => { this.#instance.setABPropString(k, v); }
      : null;
    if (!setInt && !setBool && !setString) return;

    const opts = this.#config.options ?? {};
    const intProps: Record<string, number> = {
      heartbeat_interval_s: opts.heartbeatInterval ?? 30,
      lobby_timeout_min: opts.lobbyTimeout ?? 1,
      max_num_participants_for_ss: opts.maxParticipantsScreenShare ?? 32,
      max_group_size_for_long_ringtone: opts.maxGroupSizeLongRingtone ?? 32,
      app_exit_reason_version: 1,
      log_level: opts.logLevel ?? 3,
      calling_rust_migration_bitmap: 0,
      calling_rust_migration_incoming_stanza_bitmap: 0,
      default_endpoint_thread_poll_timeout: 0,
      aigc_version: 0,
      call_admin_version: 0,
      vid_stream_pause_resume_jb_reset_threshold_ms: 0,
      // Opus: max bandwidth WB (16 kHz). FB (48 kHz) needs native audio device
      // hooks not available in this JS-only WASM context.
      opus_max_bandwidth: 1103, // OPUS_BANDWIDTH_WIDEBAND
    };
    const boolProps: Record<string, boolean> = {
      enable_av_downgrade: false,
      enable_new_user_action_stanza_for_raise_hand_sender: false,
      enable_webcodec_video_encode: false,
      enable_init_bwe_for_group_call: false,
      enable_ring_for_gc_on_offer_expire: false,
      allow_reporting_call_replayer_id: false,
      enable_offer_v2_upgrade: false,
      enable_silent_offer: false,
      voice_ai_conversation_starter_latency_tracking: false,
      enable_waiting_room_logging: false,
      attach_transport_rtx: false,
      ignore_joinable_terminate_on_expired_offer: false,
      enable_passthrough_video_decoder: false,
    };

    for (const [key, value] of Object.entries(intProps)) {
      if (setInt && Number.isFinite(value)) try { setInt(key, value); } catch {}
    }
    for (const [key, value] of Object.entries(boolProps)) {
      if (setBool) try { setBool(key, value); } catch {}
    }

    const overrideProps = parseJsonObjectEnv(CALL_WASM_AB_PROPS_JSON);
    for (const [key, value] of Object.entries(overrideProps)) {
      try {
        if (typeof value === "boolean" && setBool) setBool(key, value);
        else if (typeof value === "number" && setInt) setInt(key, value);
        else if (typeof value === "string" && setString) setString(key, value);
      } catch {}
    }
  };

  #allocateUnusedWorker = (): void => {
    const workerScriptPath = resolveWorkerScriptPath();
    if (!fs.existsSync(workerScriptPath)) return;
    try {
      const worker = new Worker(workerScriptPath, {
        stdout: true, stderr: true,
        workerData: {
          wasmPath: this.#config.wasmPath,
          wasmBinary: this.#config.wasmBinary,
          workerModulesCode: this.#workerModulesCode,
          loaderCode: this.#loaderCode,
          loaderModuleName: this.#config.loaderModuleName,
          resourcesPath: this.#config.resourcesPath,
          enableLogs: this.#config.enableLogs,
        },
      });
      const port = new NodeWorkerMessagePort(worker, "WAWebVoipWebWasmWorker", this.#dispatchWorkerCallback);
      worker.stdout?.on("data", () => {}); // suppress noisy worker stdout
      worker.stderr?.on("data", filterWorkerStderr);
      this.#unusedWorkers.push(port);
    } catch {}
  };

  #initPThreadPool = async (): Promise<void> => {
    for (let i = 0; i < this.#poolSize; i += 1) this.#allocateUnusedWorker();
  };

  #loadWasmModuleToWorker = (worker: NodeWorkerMessagePort): Promise<void> =>
    new Promise((resolve) => {
      const loadedHandler = (msg: any): void => {
        if (msg && msg.cmd === "loaded") {
          worker.removeMessageListener("cmd", loadedHandler);
          this.#workersLoadedCount += 1;
          if (this.#workersLoadedCount >= this.#poolSize && this.#removeRunDependencyCallback) {
            this.#removeRunDependencyCallback("loading-workers");
          }
          resolve();
        }
      };
      worker.addMessageListener("cmd", loadedHandler);
      worker.workerID = this.#nextWorkerID++;
      worker.postMessage({
        cmd: "load", type: "cmd",
        wasmMemory: this.#wasmMemory,
        wasmModule: this.#wasmModule,
        workerID: worker.workerID,
        handlers: [],
      });
    });

  #loadWasmModuleToAllWorkers = async (): Promise<void> => {
    this.#workersLoadedCount = 0;
    await Promise.all(this.#unusedWorkers.map((w) => this.#loadWasmModuleToWorker(w)));
  };

  #registerWorkerCallbacks = (): void => {
    const callbacks = this.#config.callbacks ?? {};
    const on = (callbackName: string, handler: (data: any) => void): void => {
      this.#callbackListeners.set(callbackName, handler);
    };

    on("loggingCallback", (data) => {
      this.#noteStackLog(data?.message);
      if (!this.#config.enableLogs) return;
      const level = data?.level;
      const msg = data?.message ?? "";
      const mapped = level === 1 ? "error" : level === 2 ? "warn" : level === 3 ? "log" : "debug";
      callbacks.onLog?.(mapped, msg);
    });

    if (callbacks.onAudioCaptureInit) {
      on("initCaptureDriverJS", (data) => {
        callbacks.onAudioCaptureInit!({
          sampleRate: data?.sample_rate ?? data?.sampleRate,
          channels: data?.channels,
          bitsPerSample: data?.bits_per_sample ?? data?.bitsPerSample,
          framesPerChunk: data?.frames_per_chunk ?? data?.framesPerChunk,
        });
      });
    }

    on("startCaptureJS", () => callbacks.onAudioCaptureStart?.());
    on("stopCaptureJS", () => callbacks.onAudioCaptureStop?.());

    if (callbacks.onAudioPlaybackInit) {
      on("initPlaybackDriverJS", (data) => {
        callbacks.onAudioPlaybackInit!({
          sampleRate: data?.sample_rate ?? data?.sampleRate,
          channels: data?.channels,
          bitsPerSample: data?.bits_per_sample ?? data?.bitsPerSample,
          framesPerChunk: data?.frames_per_chunk ?? data?.framesPerChunk,
        });
      });
    }

    on("startPlaybackJS", () => {
      callbacks.onAudioPlaybackStart?.();
      this.#startAudioPlaybackLoop();
    });
    on("stopPlaybackJS", () => {
      this.#stopAudioPlaybackLoop();
      callbacks.onAudioPlaybackStop?.();
    });

    if (callbacks.onSignalingXmpp) {
      on("onSignalingXmpp", (data) => {
        const peerJid = data.peerJid ?? data.args?.peerJid;
        const callId = data.callId ?? data.args?.callId;
        let xmlPayload = data.xmlPayload ?? data.args?.xmlPayload;
        if (Array.isArray(xmlPayload)) xmlPayload = new Uint8Array(xmlPayload);
        else if (xmlPayload && typeof xmlPayload === "object" &&
                 !(xmlPayload instanceof Uint8Array) && !Buffer.isBuffer(xmlPayload)) {
          xmlPayload = new Uint8Array(xmlPayload);
        }
        callbacks.onSignalingXmpp!(peerJid, callId, xmlPayload);
      });
    }

    if (callbacks.onCallEvent) {
      on("onCallEvent", (data) => {
        callbacks.onCallEvent!(data.eventType, data.eventDataJson);
      });
    }

    if (callbacks.sendDataToRelay) {
      on("sendDataToRelay", (data) => {
        let relayData = data.data ?? data.args?.data;
        const ip = data.ip ?? data.args?.ip;
        const portNum = data.port ?? data.args?.port;
        if (relayData instanceof Uint8Array) { /* ok */ }
        else if (Array.isArray(relayData)) relayData = new Uint8Array(relayData);
        else if (Buffer.isBuffer(relayData)) relayData = new Uint8Array(relayData);
        else if (relayData && typeof relayData === "object" && relayData.buffer) {
          relayData = new Uint8Array(
            relayData.buffer,
            relayData.byteOffset ?? 0,
            relayData.byteLength ?? relayData.length,
          );
        } else if (relayData instanceof ArrayBuffer) relayData = new Uint8Array(relayData);
        else return 0;
        if (!ip || !portNum) return 0;
        callbacks.sendDataToRelay!(relayData, ip, portNum);
        return relayData.byteLength;
      });
    }

  };

  /** Ports the VoIP code constructs itself, bound to this engine's callbacks. */
  get #ownedPortClass(): typeof NodeWorkerMessagePort {
    const dispatch = this.#dispatchWorkerCallback;
    return class extends NodeWorkerMessagePort {
      constructor(worker: any, name?: string) {
        super(worker, name, dispatch);
      }
    };
  }

  #requireModule = (name: string): any => {
    const preDefinedModules: Record<string, any> = {
      Promise,
      WAWebVoipWebWasmWorkerResource: {
        resourcePath: resolveWorkerScriptPath(),
        name: "WAWebVoipWebWasmWorker",
      },
      WorkerBundleResource: {
        createDedicatedWebWorker: (resource: any) => {
          const scriptPath = resource?.resourcePath && fs.existsSync(resource.resourcePath)
            ? resource.resourcePath
            : resolveWorkerScriptPath();
          const worker = new Worker(scriptPath, {
            stdout: true, stderr: true,
            workerData: {
              wasmPath: this.#config.wasmPath,
              wasmBinary: this.#config.wasmBinary,
              workerModulesCode: this.#workerModulesCode,
              loaderCode: this.#loaderCode,
              loaderModuleName: this.#config.loaderModuleName,
              resourcesPath: this.#config.resourcesPath,
              enableLogs: this.#config.enableLogs,
            },
          });
          worker.stdout?.on("data", () => {});
          worker.stderr?.on("data", filterWorkerStderr);
          this.#dedicatedWorkers.add(worker);
          worker.once("exit", () => this.#dedicatedWorkers.delete(worker));
          return worker;
        },
      },
      WorkerClient: { init: () => {} },
      WorkerMessagePort: {
        WorkerMessagePort: this.#ownedPortClass,
        CastWorkerMessagePort: (w: any) => w,
        WorkerSyncedMessagePort: this.#ownedPortClass,
      },
      bx: Object.assign((id: string | number) => String(id), { getURL: () => "" }),
      HasteSupportData: { handle: () => {} },
      ServiceWorkerDynamicModules: { handle: () => {} },
      WhatsAppWebServiceWorker: { default: true },
      WAWebLogger: { initializeWAWebLogger: () => {} },
      WAWebSw: { initHandlers: () => {} },
      WAWebWamRuntimeProvider: { setWamRuntime: () => {} },
      WAWebWamWorkerInterface: { commit: () => {}, set: () => {} },
      ServerJSDefine: { handleDefine: () => {} },
      ix: { add: () => {} },
      MetaConfigMap: { add: () => {} },
      QPLHasteSupportDataStorage: { default: { add: () => {}, get: () => null } },
      getFalcoLogPolicy_DO_NOT_USE: { add: () => {} },
      gkx: { add: () => {} },
      justknobx: { add: () => {} },
      qex: { add: () => {} },
    };

    if (preDefinedModules[name]) return preDefinedModules[name];
    const mod = this.#moduleRegistry.get(name);
    if (!mod) return {};
    if (mod.exports !== undefined) return mod.exports;

    const requireFn = this.#requireModule;
    const normalizeModuleResult = (value: any): any => {
      if (value && typeof value === "object" && "exports" in value && Object.keys(value).length === 1) {
        return (value as any).exports;
      }
      return value;
    };
    const importDefaultFn = (dep: string): any => {
      const v = requireFn(dep);
      return v && v.__esModule ? v.default : v;
    };
    const importAllFn = (dep: string): any => {
      const v = requireFn(dep);
      if (v == null) return { default: v };
      if (v.__esModule) return v;
      if (typeof v !== "object" && typeof v !== "function") return { default: v };
      const ns: Record<string, any> = {};
      for (const key of Object.keys(v)) ns[key] = v[key];
      ns.default = v;
      return ns;
    };

    const tryMetro = (): any => {
      const module = { exports: {} as any };
      mod.factory(this.#vmContext ?? globalThis, requireFn, importDefaultFn, importAllFn, null, module, module.exports);
      return normalizeModuleResult(module.exports);
    };
    const tryLegacy = (): any => {
      const exports: Record<string, any> = {};
      const module = { exports };
      const resolvedDeps = mod.deps.map((dep) => requireFn(dep));
      mod.factory(this.#vmContext ?? globalThis, requireFn, requireFn, requireFn, module, exports, ...resolvedDeps);
      return normalizeModuleResult(module.exports);
    };

    let result: any = {};
    let metroError: unknown = null;
    try { result = tryMetro(); } catch (e) { metroError = e; }
    if (metroError != null || (result && typeof result === "object" && Object.keys(result).length === 0)) {
      try {
        const legacyResult = tryLegacy();
        if (typeof legacyResult === "function" || (legacyResult && Object.keys(legacyResult).length > 0)) {
          result = legacyResult;
        }
      } catch {}
    }
    mod.exports = result;
    return result;
  };

  #createVMContext = (memory: WebAssembly.Memory): vm.Context => {
    const callbacks = this.#config.callbacks ?? {};

    const wasmCallbacks: Record<string, any> = {
      onVoipReady: (): void => {
        this.#voipReadyResolver?.();
        callbacks.onVoipReady?.();
      },
      onSignalingXmpp: (data: any) => callbacks.onSignalingXmpp?.(data?.peerJid, data?.callId, data?.xmlPayload),
      onCallEvent: (data: any) => callbacks.onCallEvent?.(data?.eventType, data?.eventDataJson),
      sendDataToRelay: (data: any) => callbacks.sendDataToRelay?.(data?.data, data?.ip, data?.port),
      loggingCallback: (data: any): void => {
        this.#noteStackLog(data?.message);
        if (!this.#config.enableLogs) return;
        const level = data?.level;
        const msg = data?.message ?? "";
        const mapped = level === 1 ? "error" : level === 2 ? "warn" : level === 3 ? "log" : "debug";
        callbacks.onLog?.(mapped, msg);
      },
      initCaptureDriverJS: (data: any): number => {
        callbacks.onAudioCaptureInit?.({
          sampleRate: data?.sample_rate, channels: data?.channels,
          bitsPerSample: data?.bits_per_sample, framesPerChunk: data?.frames_per_chunk,
        });
        return 0;
      },
      startCaptureJS: (): number => { callbacks.onAudioCaptureStart?.(); return 0; },
      stopCaptureJS: (): number => { callbacks.onAudioCaptureStop?.(); return 0; },
      initPlaybackDriverJS: (data: any): number => {
        callbacks.onAudioPlaybackInit?.({
          sampleRate: data?.sample_rate, channels: data?.channels,
          bitsPerSample: data?.bits_per_sample, framesPerChunk: data?.frames_per_chunk,
        });
        return 0;
      },
      startPlaybackJS: (): number => {
        callbacks.onAudioPlaybackStart?.();
        this.#startAudioPlaybackLoop();
        return 0;
      },
      stopPlaybackJS: (): number => {
        this.#stopAudioPlaybackLoop();
        callbacks.onAudioPlaybackStop?.();
        return 0;
      },
      startVideoCaptureJS: () => 0,
      stopVideoCaptureJS: () => 0,
      startDesktopCaptureJS: () => 0,
      stopDesktopCaptureJS: () => 0,
      dataChannelStateCallback: () => 0,
      getBrowserAudioProcessingStatus: () => 7,
      getBweModelPath: () => null,
      videoFrameConsumed: () => 0,
      cryptoHkdfExtractWithSaltAndExpand: (data: any): Uint8Array => {
        const key = toByteArray(data?.key_);
        const salt = data?.salt_ ? toByteArray(data.salt_) : new Uint8Array(0);
        const info = toByteArray(data?.info_);
        const length = data?.length ?? 32;
        return callbacks.cryptoHkdf?.(key, salt, info, length) ?? new Uint8Array(length);
      },
      hmacSha256KeyGenerator: (data: any): Uint8Array => {
        const hmacData = new Uint8Array(data?.data_ ?? []);
        const hmacKey = new Uint8Array(data?.key_ ?? []);
        return callbacks.hmacSha256?.(hmacData, hmacKey) ?? new Uint8Array(32);
      },
      isParticipantKnownContact: () => true,
      getPersistentDirectoryPath: (): string => {
        const dir = "/tmp/voip";
        try { if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true }); } catch {}
        return dir;
      },
    };

    const __d = (name: string, deps: string[], factory: Function): void => {
      this.#moduleRegistry.set(name, { deps, factory, exports: undefined });
    };

    const babelHelpers = {
      extends: Object.assign,
      inheritsLoose: (sub: any, sup: any): void => {
        sub.prototype = Object.create(sup.prototype);
        sub.prototype.constructor = sub;
        sub.__proto__ = sup;
      },
      objectWithoutPropertiesLoose: (source: any, excluded: string[]): Record<string, unknown> => {
        if (source == null) return {};
        const target: Record<string, unknown> = {};
        for (const key of Object.keys(source)) {
          if (excluded.indexOf(key) >= 0) continue;
          target[key] = source[key];
        }
        return target;
      },
      taggedTemplateLiteralLoose: (strings: TemplateStringsArray, raw?: string[]): TemplateStringsArray => {
        if (!raw) raw = (strings.slice(0) as unknown) as string[];
        (strings as any).raw = raw;
        return strings;
      },
      wrapNativeSuper: (Class: any) => Class,
    };

    const addRunDependency = (dep: string): void => {
      if (dep === "loading-workers" && this.#workersLoadedCount >= this.#poolSize) {
        setImmediate(() => this.#removeRunDependencyCallback?.(dep));
      }
    };
    const removeRunDependency = (_dep: string): void => {};
    this.#removeRunDependencyCallback = removeRunDependency;

    const webCrypto = {
      getRandomValues: <T extends ArrayBufferView>(arr: T): T => {
        if (!arr || !ArrayBuffer.isView(arr)) {
          throw new TypeError("crypto.getRandomValues expects a TypedArray");
        }
        const bytes = Buffer.from(arr.buffer, arr.byteOffset, arr.byteLength);
        randomFillSync(bytes);
        return arr;
      },
    };

    const selfObj: Record<string, any> = {
      __swData: { dynamic_data: { hsdp: {}, dynamic_modules: [] } },
      WhatsAppVoipWasmCallbacks: wasmCallbacks,
      WhatsAppVoipWasmWorkerCompatibleCallbacks: wasmCallbacks,
      crypto: webCrypto,
    };
    selfObj.self = selfObj;
    selfObj.window = selfObj;
    selfObj.globalThis = selfObj;

    if (typeof global !== "undefined") {
      (global as any).WhatsAppVoipWasmCallbacks = wasmCallbacks;
      (global as any).WhatsAppVoipWasmWorkerCompatibleCallbacks = wasmCallbacks;
      this.#globalWasmCallbacks = wasmCallbacks;
    }

    const context = vm.createContext({
      self: selfObj, globalThis: selfObj, global: selfObj, window: selfObj,
      console, setTimeout, setInterval, clearTimeout, clearInterval,
      queueMicrotask, performance, babelHelpers, __d,
      require: this.#requireModule,
      addRunDependency, removeRunDependency,
      WebAssembly, SharedArrayBuffer,
      Atomics: this.#createAtomicsWrapper(memory),
      Int8Array, Uint8Array, Int16Array, Uint16Array, Int32Array, Uint32Array,
      Float32Array, Float64Array, BigInt64Array, BigUint64Array,
      ArrayBuffer, DataView, Error, TypeError, RangeError, Promise,
      Map, Set, WeakMap, WeakSet, Symbol, Object, Array, String, Number,
      Boolean, Math, Date, JSON, RegExp, Function, Proxy, Reflect,
      crypto: webCrypto,
      WhatsAppVoipWasmCallbacks: wasmCallbacks,
      WhatsAppVoipWasmWorkerCompatibleCallbacks: wasmCallbacks,
      navigator: {
        userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        hardwareConcurrency: 4,
      },
      process: undefined,
      document: { currentScript: null },
      location: { href: "file:///wasm" },
      Worker: class { constructor() {} postMessage() {} terminate() {} addEventListener() {} },
      fetch: async () => { throw new Error("fetch not supported"); },
      XMLHttpRequest: class { open() {} send() {} setRequestHeader() {} },
      Blob: class { constructor() {} },
      URL: { createObjectURL: () => "blob:fake", revokeObjectURL: () => {} },
      Image: class { src = ""; onload = null; onerror = null; },
      Audio: class { src = ""; addEventListener() {} },
      __NODE_PTHREAD: {
        getUnusedWorker: (): any => {
          if (this.#unusedWorkers.length === 0) return null;
          const worker = this.#unusedWorkers.pop()!;
          this.#runningWorkers.push(worker);
          return worker;
        },
        returnWorkerToPool: (worker: any): void => {
          const idx = this.#runningWorkers.indexOf(worker);
          if (idx >= 0) {
            this.#runningWorkers.splice(idx, 1);
            this.#unusedWorkers.push(worker);
          }
        },
        spawnThread: (params: any): number => {
          const worker = this.#unusedWorkers.pop();
          if (!worker) return 6;
          this.#runningWorkers.push(worker);
          this.#pthreads[params.pthread_ptr] = worker;
          worker.pthread_ptr = params.pthread_ptr;
          const pthreadTable = this.#instance?.PThread?.pthreads;
          if (pthreadTable) pthreadTable[params.pthread_ptr] = worker;
          worker.postMessage({
            cmd: "run",
            start_routine: params.startRoutine,
            arg: params.arg,
            pthread_ptr: params.pthread_ptr,
          });
          return 0;
        },
        unusedWorkersCount: () => this.#unusedWorkers.length,
        runningWorkersCount: () => this.#runningWorkers.length,
      },
      __IS_NODE_PTHREAD_ENV: true,
    });

    context.self = context;
    context.globalThis = context;
    context.global = context;
    context.window = context;
    return context;
  };

  /**
   * Main-thread waits on shared memory, woken by the pthread workers. Once
   * destroy() terminates the workers nothing wakes them, and V8 keeps every
   * pending waitAsync promise as a GC root, so each one held the whole runtime
   * (module bytes, shared memory) of a destroyed engine forever. Wrapping them
   * lets destroy() cut that link and wake the wait itself.
   */
  #globalWasmCallbacks: Record<string, unknown> | null = null;

  /**
   * WhatsApp's runtime never calls onVoipReady, so waitForVoipStackReady used
   * to end only at its 15 s timeout. The stack is up once its call-event
   * thread starts (~20 ms after initVoipStack), which it logs.
   */
  #noteStackLog = (message: unknown): void => {
    if (this.#voipReadyResolver && typeof message === "string" && message.includes("call_event_proc started")) {
      this.#voipReadyResolver();
    }
  };

  readonly #pendingWaits = new Set<{ array: Int32Array; index: number; settle: ((v: string) => void) | null }>();

  #trackedWaitAsync = (typedArray: Int32Array, index: number, value: number, timeout?: number): any => {
    const result = (Atomics as any).waitAsync(typedArray, index, value, timeout);
    if (!result.async) return result;
    const wait = { array: typedArray, index, settle: null as ((v: string) => void) | null };
    const value$ = new Promise<string>((resolve) => { wait.settle = resolve; });
    this.#pendingWaits.add(wait);
    // The native promise only sees `wait`; dropping `settle` frees the runtime.
    (result.value as Promise<string>).then((v) => {
      this.#pendingWaits.delete(wait);
      wait.settle?.(v);
    });
    return { async: true, value: value$ };
  };

  #releasePendingWaits = (): void => {
    for (const wait of this.#pendingWaits) {
      wait.settle = null;
      try { Atomics.notify(wait.array, wait.index); } catch {}
    }
    this.#pendingWaits.clear();
  };

  #createAtomicsWrapper = (_memory: WebAssembly.Memory): typeof Atomics => {
    const atomicsWrapper = {
      add: Atomics.add.bind(Atomics),
      and: Atomics.and.bind(Atomics),
      compareExchange: Atomics.compareExchange.bind(Atomics),
      exchange: Atomics.exchange.bind(Atomics),
      isLockFree: Atomics.isLockFree.bind(Atomics),
      load: Atomics.load.bind(Atomics),
      or: Atomics.or.bind(Atomics),
      store: Atomics.store.bind(Atomics),
      sub: Atomics.sub.bind(Atomics),
      xor: Atomics.xor.bind(Atomics),
      notify: (typedArray: Int32Array, index: number, count?: number): number => {
        try { return Atomics.notify(typedArray, index, count); }
        catch (e: any) {
          if (e?.message?.includes("futex_wake") || e?.message?.includes("main_browser_thread")) return 0;
          throw e;
        }
      },
      waitAsync: (Atomics as any).waitAsync
        ? this.#trackedWaitAsync
        : (): { async: boolean; value: Promise<"ok"> } => ({ async: true, value: Promise.resolve("ok") }),
      wait: (typedArray: Int32Array, index: number, value: number, timeout?: number): "ok" | "not-equal" | "timed-out" => {
        const currentValue = Atomics.load(typedArray, index);
        if (currentValue !== value) return "not-equal";
        if (timeout !== undefined && timeout <= 0) return "timed-out";
        return "timed-out";
      },
      [Symbol.toStringTag]: "Atomics",
    };
    return atomicsWrapper as unknown as typeof Atomics;
  };
}

export default WasmEngine;
