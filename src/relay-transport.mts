/**
 * Relay transport.
 *
 * Tunnels UDP traffic to WhatsApp's edge relay servers via WebRTC data
 * channels (using `@roamhq/wrtc`). Mirrors the browser client's behavior:
 * pre-negotiated SCTP, custom DTLS fingerprint, ICE restart on idle.
 *
 * @author ShellTear
 */
import { appendFileSync } from "node:fs";

const RELAY_PROTO_UDP = 0;
const FAUX_WEB_CLIENT_RELAY_PORT = 3478;
const TRUE_WEB_CLIENT_RELAY_PORT = 3480;
const CONNECTION_TIMEOUT_MS = 20_000;
const ICE_RESTART_IDLE_THRESHOLD_MS = 10_000;
const ICE_RTT_POLL_MS = 1_000;
const MAX_BUFFER_SIZE = 256 * 1024;

const RELAY_PACKET_LOG_PATH = process.env.CALL_DUMP_RELAY_PACKETS_PATH ?? "";
const DISABLE_IPV6 = process.env.CALL_DISABLE_IPV6 !== "0";
const RELAY_PORT_MODE = process.env.CALL_RELAY_PORT_MODE === "web" ? "web" : "original";
const USE_ORIGINAL_RELAY_PORTS = RELAY_PORT_MODE === "original";

const RELAY_DTLS_FINGERPRINT =
  "F9:CA:0C:98:A3:CC:71:D6:42:CE:5A:E2:53:D2:15:20:D3:1B:BA:D8:57:A4:F0:AF:BE:0B:FB:F3:6B:0C:A0:68";

type RelayAddress = {
  protocol: number;
  ipv4?: string;
  ipv6?: string;
  port?: number;
  port_v6?: number;
};

type RelayDescriptor = {
  relay_id: number;
  relay_name: string;
  token_id: number;
  auth_token_id?: number;
  addresses: RelayAddress[];
};

export type RelayListUpdatePayload = {
  relay_key: string;
  relay_tokens: string[];
  auth_tokens?: string[];
  enable_edgeray_dtls_active_mode?: boolean;
  relays: RelayDescriptor[];
};

type RelayConnectionInfo = {
  id: string;
  relayId: number;
  ip: string;
  port: number;
  originalPort: number;
  isIPv6: boolean;
  token: string;
  authToken?: string;
  key: string;
  name: string;
  enableEdgerayDtlsActiveMode: boolean;
};

type RelayConnectionState = "none" | "connecting" | "open" | "closed" | "failed";

type RelayConnectionRuntime = {
  info: RelayConnectionInfo;
  state: RelayConnectionState;
  peerConnection: any | null;
  dataChannel: any | null;
  iceCandidate: string | null;
  packetBuffer: ArrayBuffer[];
  bufferedBytes: number;
  connectPromise: Promise<void> | null;
  connectionTimeout: NodeJS.Timeout | null;
  iceStatsInterval: NodeJS.Timeout | null;
  lastIceRttMs: number | null;
  hasReceivedFirstPacket: boolean;
  hasNonStunPacketSent: boolean;
  sentMedia: boolean;
  lastRxPacketTime: number;
  isReconnecting: boolean;
  stats: RelayTransportStats;
};

type RelayPacketKind = "stun_alloc" | "stun_bind" | "stun_unknown" | "non_stun";

export type RelayTransportStats = {
  sentPackets: number;
  receivedPackets: number;
  sentBytes: number;
  receivedBytes: number;
  droppedPackets: number;
  openConnections: number;
};

export type RelayTransportConfig = {
  onTransportMessage: (data: Uint8Array, ip: string, port: number) => void;
  onIceRtt?: (rttMs: number, ip: string, port: number) => void;
};

const getConnectionIdentifier = (ip: string, port: number): string =>
  ip.includes(":") ? `[${ip}]:${port}` : `${ip}:${port}`;

const createEmptyStats = (): RelayTransportStats => ({
  sentPackets: 0,
  receivedPackets: 0,
  sentBytes: 0,
  receivedBytes: 0,
  droppedPackets: 0,
  openConnections: 0,
});

const getRelayLookupId = (ip: string, port: number): string =>
  getConnectionIdentifier(ip, port === TRUE_WEB_CLIENT_RELAY_PORT ? TRUE_WEB_CLIENT_RELAY_PORT : port);

const getRtcConnectPort = (info: RelayConnectionInfo): number =>
  info.ip === "157.240.24.133" ? FAUX_WEB_CLIENT_RELAY_PORT : info.port;

const getVoipStackPort = (info: RelayConnectionInfo): number => {
  if (USE_ORIGINAL_RELAY_PORTS) return info.originalPort || info.port;
  const sourcePort = info.originalPort || info.port;
  return sourcePort === TRUE_WEB_CLIENT_RELAY_PORT
    ? TRUE_WEB_CLIENT_RELAY_PORT
    : FAUX_WEB_CLIENT_RELAY_PORT;
};

const bufferPacket = (connection: RelayConnectionRuntime, packet: ArrayBuffer): boolean => {
  if (packet.byteLength > MAX_BUFFER_SIZE) {
    connection.stats.droppedPackets += 1;
    return false;
  }
  while (connection.packetBuffer.length > 0 &&
         connection.bufferedBytes + packet.byteLength > MAX_BUFFER_SIZE) {
    const dropped = connection.packetBuffer.shift();
    if (dropped) {
      connection.bufferedBytes -= dropped.byteLength;
      connection.stats.droppedPackets += 1;
    }
  }
  connection.packetBuffer.push(packet);
  connection.bufferedBytes += packet.byteLength;
  return true;
};

const shiftPacket = (connection: RelayConnectionRuntime): ArrayBuffer | null => {
  const packet = connection.packetBuffer.shift() ?? null;
  if (packet) connection.bufferedBytes -= packet.byteLength;
  return packet;
};

const replaceIceCredentials = (sdp: string, ufrag: string, pwd: string): string =>
  sdp
    .replace(/a=ice-ufrag:[^\r\n]+/g, `a=ice-ufrag:${ufrag}`)
    .replace(/a=ice-pwd:[^\r\n]+/g, `a=ice-pwd:${pwd}`);

const replaceDtlsFingerprint = (sdp: string, algorithm: string, fingerprint: string): string =>
  sdp.replace(/a=fingerprint:[^\r\n]+/g, `a=fingerprint:${algorithm} ${fingerprint}`);

const removeIceCandidates = (sdp: string): string =>
  sdp
    .replace(/a=candidate:[^\r\n]+\r?\n/g, "")
    .replace(/a=end-of-candidates\r?\n?/g, "");

const appendRelayCandidate = (sdp: string, ip: string, port: number): string => {
  const candidate = `a=candidate:2 1 udp 2122262783 ${ip} ${port} typ host generation 0 network-cost 5`;
  return `${removeIceCandidates(sdp)}${candidate}\r\na=end-of-candidates\r\n`;
};

const buildRemoteRelayAnswer = (offerSdp: string, info: RelayConnectionInfo): string => {
  const setupLine = info.enableEdgerayDtlsActiveMode ? "a=setup:active" : "a=setup:passive";
  let answerSdp = offerSdp.replace(/a=setup:actpass/g, setupLine);
  answerSdp = replaceIceCredentials(answerSdp, info.authToken ?? info.token, info.key);
  answerSdp = replaceDtlsFingerprint(answerSdp, "sha-256", RELAY_DTLS_FINGERPRINT);
  answerSdp = answerSdp.replace(/a=ice-options:[^\r\n]+\r\n/g, "");
  answerSdp = answerSdp.replace(/a=max-message-size:[^\r\n]+/g, "a=max-message-size:1200");
  return appendRelayCandidate(answerSdp, info.ip, info.port);
};

const toArrayBuffer = (data: Uint8Array | Buffer): ArrayBuffer => {
  const copy = new Uint8Array(data.byteLength);
  copy.set(data);
  return copy.buffer;
};

const toUint8Array = (data: unknown): Uint8Array | null => {
  if (data instanceof Uint8Array) return new Uint8Array(data);
  if (Buffer.isBuffer(data)) return new Uint8Array(data);
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (data && typeof data === "object" && "byteLength" in data && "buffer" in data) {
    const view = data as ArrayBufferView;
    return new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
  }
  return null;
};

const classifyRelayPacket = (packet: ArrayBuffer): RelayPacketKind => {
  if (packet.byteLength < 2) return "non_stun";
  const bytes = new Uint8Array(packet);
  const first = bytes[0]!;
  const second = bytes[1]!;
  if ((first & 0xc0) !== 0) return "non_stun";
  const stunType = ((first & 0x3f) << 8) | second;
  if (stunType === 0x0001) return "stun_bind";
  if (stunType === 0x0003) return "stun_alloc";
  return "stun_unknown";
};

const appendRelayPacketLog = (
  direction: "send" | "recv",
  connection: RelayConnectionRuntime,
  packet: Uint8Array | ArrayBuffer,
): void => {
  if (!RELAY_PACKET_LOG_PATH) return;
  const buffer = packet instanceof ArrayBuffer
    ? Buffer.from(new Uint8Array(packet))
    : Buffer.from(packet.buffer, packet.byteOffset, packet.byteLength);
  appendFileSync(
    RELAY_PACKET_LOG_PATH,
    JSON.stringify({
      ts: new Date().toISOString(),
      direction,
      id: connection.info.id,
      relayId: connection.info.relayId,
      relayName: connection.info.name,
      ip: connection.info.ip,
      rtcPort: connection.info.port,
      voipPort: getVoipStackPort(connection.info),
      size: buffer.byteLength,
      hex: buffer.toString("hex"),
    }) + "\n",
  );
};

export class RelayRtcTransport {
  /** Set by closeAll(): the transport is torn down and must not renegotiate. */
  #destroyed = false;
  readonly #relayInfoById = new Map<string, RelayConnectionInfo>();
  readonly #connections = new Map<string, RelayConnectionRuntime>();
  readonly #totals = createEmptyStats();
  #wrtcPromise: Promise<any> | null = null;

  constructor(private readonly config: RelayTransportConfig) {}

  updateRelayList = (update: RelayListUpdatePayload): void => {
    const nextInfoById = new Map<string, RelayConnectionInfo>();

    for (const relay of update.relays ?? []) {
      if (relay.token_id == null || relay.token_id < 0 ||
          relay.token_id >= (update.relay_tokens ?? []).length) {
        continue;
      }

      const authToken =
        update.auth_tokens && relay.auth_token_id != null && relay.auth_token_id >= 0
          ? update.auth_tokens[relay.auth_token_id]
          : undefined;

      for (const address of relay.addresses ?? []) {
        if (address.protocol !== RELAY_PROTO_UDP) continue;

        if (address.ipv4 && address.port != null) {
          const clientPort = USE_ORIGINAL_RELAY_PORTS ? address.port : TRUE_WEB_CLIENT_RELAY_PORT;
          const id = getConnectionIdentifier(address.ipv4, clientPort);
          nextInfoById.set(id, {
            id,
            relayId: relay.relay_id,
            ip: address.ipv4,
            port: clientPort,
            originalPort: address.port,
            isIPv6: false,
            token: update.relay_tokens[relay.token_id]!,
            authToken,
            key: update.relay_key,
            name: relay.relay_name,
            enableEdgerayDtlsActiveMode: update.enable_edgeray_dtls_active_mode === true,
          });
        }

        if (!DISABLE_IPV6 && address.ipv6 && address.port_v6 != null) {
          const clientPort = USE_ORIGINAL_RELAY_PORTS ? address.port_v6 : TRUE_WEB_CLIENT_RELAY_PORT;
          const id = getConnectionIdentifier(address.ipv6, clientPort);
          nextInfoById.set(id, {
            id,
            relayId: relay.relay_id,
            ip: address.ipv6,
            port: clientPort,
            originalPort: address.port_v6,
            isIPv6: true,
            token: update.relay_tokens[relay.token_id]!,
            authToken,
            key: update.relay_key,
            name: relay.relay_name,
            enableEdgerayDtlsActiveMode: update.enable_edgeray_dtls_active_mode === true,
          });
        }
      }
    }

    for (const id of this.#relayInfoById.keys()) {
      if (!nextInfoById.has(id)) this.#closeConnection(id);
    }

    this.#relayInfoById.clear();
    for (const [id, info] of nextInfoById) {
      this.#relayInfoById.set(id, info);
      this.#ensureConnectionSafe(info);
    }
  };

  send = (packet: Uint8Array | Buffer, ip: string, port: number): number => {
    const requestedId = getConnectionIdentifier(ip, port);
    const preferredPort = USE_ORIGINAL_RELAY_PORTS ? port : TRUE_WEB_CLIENT_RELAY_PORT;
    const candidateIds = [
      requestedId,
      getConnectionIdentifier(ip, preferredPort),
      getRelayLookupId(ip, TRUE_WEB_CLIENT_RELAY_PORT),
      getRelayLookupId(ip, FAUX_WEB_CLIENT_RELAY_PORT),
    ];

    let info: RelayConnectionInfo | undefined;
    for (const candidateId of candidateIds) {
      info = this.#relayInfoById.get(candidateId);
      if (info) break;
    }

    if (!info) {
      if (DISABLE_IPV6 && ip.includes(":")) return 0;
      const earlyConnection = this.#getOrCreateEarlyConnection(ip, port);
      bufferPacket(earlyConnection, toArrayBuffer(packet));
      return packet.byteLength;
    }

    const connection = this.#getOrCreateConnection(info);
    const arrayBuffer = toArrayBuffer(packet);

    if (classifyRelayPacket(arrayBuffer) === "stun_alloc" &&
        connection.state === "open" &&
        connection.sentMedia &&
        Date.now() - connection.lastRxPacketTime > ICE_RESTART_IDLE_THRESHOLD_MS) {
      connection.packetBuffer = [];
      connection.bufferedBytes = 0;
      bufferPacket(connection, arrayBuffer);
      void this.#restartIce(connection);
      return packet.byteLength;
    }

    if (connection.state === "open" && connection.dataChannel?.readyState === "open") {
      if (!this.#sendBufferedPacket(connection, arrayBuffer)) {
        connection.stats.droppedPackets += 1;
      }
      return packet.byteLength;
    }

    bufferPacket(connection, arrayBuffer);
    this.#ensureConnectionSafe(info);
    return packet.byteLength;
  };

  getStats = (): RelayTransportStats => {
    let openConnections = 0;
    for (const connection of this.#connections.values()) {
      if (connection.state === "open") openConnections += 1;
    }
    return { ...this.#totals, openConnections };
  };

  /**
   * Close every connection and forget the relay list, but stay usable: the
   * next call's relay list opens fresh connections. For when a call ends.
   */
  closeConnections = (): void => {
    for (const id of [...this.#connections.keys()]) this.#closeConnection(id);
    this.#relayInfoById.clear();
  };

  closeAll = async (): Promise<void> => {
    this.#destroyed = true;
    for (const id of [...this.#connections.keys()]) this.#closeConnection(id);
  };

  // ─── private ──────────────────────────────────────────────────────────────

  #getOrCreateConnection = (info: RelayConnectionInfo): RelayConnectionRuntime => {
    const existing = this.#connections.get(info.id);
    if (existing) {
      existing.info = info;
      return existing;
    }
    const created: RelayConnectionRuntime = {
      info, state: "none", peerConnection: null, dataChannel: null,
      iceCandidate: null, packetBuffer: [], bufferedBytes: 0,
      connectPromise: null, connectionTimeout: null, iceStatsInterval: null,
      lastIceRttMs: null, hasReceivedFirstPacket: false,
      hasNonStunPacketSent: false, sentMedia: false, lastRxPacketTime: 0,
      isReconnecting: false, stats: createEmptyStats(),
    };
    this.#connections.set(info.id, created);
    return created;
  };

  #getOrCreateEarlyConnection = (ip: string, port: number): RelayConnectionRuntime => {
    const id = getConnectionIdentifier(ip, port);
    const existing = this.#connections.get(id);
    if (existing) return existing;
    const placeholderInfo: RelayConnectionInfo = {
      id, relayId: 0, ip, port, originalPort: port,
      isIPv6: ip.includes(":"), token: "", key: "",
      name: "early-packet", enableEdgerayDtlsActiveMode: false,
    };
    const created: RelayConnectionRuntime = {
      info: placeholderInfo, state: "none", peerConnection: null,
      dataChannel: null, iceCandidate: null, packetBuffer: [],
      bufferedBytes: 0, connectPromise: null, connectionTimeout: null,
      iceStatsInterval: null, lastIceRttMs: null,
      hasReceivedFirstPacket: false, hasNonStunPacketSent: false,
      sentMedia: false, lastRxPacketTime: 0, isReconnecting: false,
      stats: createEmptyStats(),
    };
    this.#connections.set(id, created);
    return created;
  };

  /**
   * Fire-and-forget entry point for connection setup. Connect failures are a
   * normal part of relay churn (and of a call ending mid-negotiation), so they
   * are swallowed here — an unhandled rejection would take the host process
   * down on Node >= 15.
   */
  #ensureConnectionSafe = (info: RelayConnectionInfo): void => {
    void this.#ensureConnection(info).catch(() => undefined);
  };

  #ensureConnection = async (info: RelayConnectionInfo): Promise<void> => {
    if (this.#destroyed) return;
    const connection = this.#getOrCreateConnection(info);
    if (connection.state === "open" || connection.state === "connecting") {
      return connection.connectPromise ?? Promise.resolve();
    }
    const promise = this.#connect(connection);
    connection.connectPromise = promise;
    try { await promise; } finally { connection.connectPromise = null; }
  };

  #connect = async (connection: RelayConnectionRuntime): Promise<void> => {
    if (this.#destroyed) return;
    const wrtcModule = await this.#loadWrtc();
    const { RTCPeerConnection } = wrtcModule;
    if (typeof RTCPeerConnection !== "function") {
      throw new Error("RTCPeerConnection unavailable from @roamhq/wrtc");
    }

    this.#closePeerObjects(connection);
    connection.state = "connecting";

    const pc = new RTCPeerConnection();
    const dc = pc.createDataChannel("pre-negotiated", {
      negotiated: true, id: 0, ordered: false, maxRetransmits: 0, priority: "high",
    });
    connection.peerConnection = pc;
    connection.dataChannel = dc;
    dc.binaryType = "arraybuffer";

    dc.onopen = (): void => {
      connection.state = "open";
      connection.isReconnecting = false;
      if (connection.connectionTimeout) {
        clearTimeout(connection.connectionTimeout);
        connection.connectionTimeout = null;
      }
      this.#flushBufferedPackets(connection);
      this.#startIceRttPolling(connection);
    };

    dc.onclose = (): void => {
      if (connection.state !== "failed") connection.state = "closed";
    };

    dc.onerror = (): void => {
      connection.state = "failed";
    };

    dc.onmessage = (event: { data: unknown }): void => {
      this.#handleIncomingPacket(connection, event.data);
    };

    pc.onicecandidate = (event: { candidate?: { candidate?: string } | null }): void => {
      if (event.candidate?.candidate && !connection.iceCandidate) {
        connection.iceCandidate = event.candidate.candidate;
      }
    };

    pc.oniceconnectionstatechange = (): void => {
      if (pc.iceConnectionState === "failed" || pc.iceConnectionState === "closed") {
        connection.state = "failed";
      }
    };

    connection.connectionTimeout = setTimeout(() => {
      if (connection.state === "connecting") {
        connection.state = "failed";
        this.#closePeerObjects(connection);
      }
    }, CONNECTION_TIMEOUT_MS);

    /** True once this pc was replaced or closed underneath us. */
    const stale = (): boolean =>
      this.#destroyed || connection.peerConnection !== pc || pc.signalingState === "closed";

    const offer = await pc.createOffer();
    if (stale()) return;
    await pc.setLocalDescription(offer);
    if (stale()) return;
    const remoteSdp = buildRemoteRelayAnswer(offer.sdp ?? "", {
      ...connection.info,
      port: getRtcConnectPort(connection.info),
    });
    if (stale()) return;
    await pc.setRemoteDescription({ type: "answer", sdp: remoteSdp });
  };

  #restartIce = async (connection: RelayConnectionRuntime): Promise<void> => {
    if (connection.isReconnecting || !connection.hasNonStunPacketSent) return;

    const { RTCPeerConnection } = await this.#loadWrtc();
    if (typeof RTCPeerConnection !== "function") return;

    connection.isReconnecting = true;
    try {
      this.#closePeerObjects(connection);
      connection.state = "connecting";

      const pc = new RTCPeerConnection();
      const dc = pc.createDataChannel("pre-negotiated", {
        negotiated: true, id: 0, ordered: false, maxRetransmits: 0, priority: "high",
      });
      connection.peerConnection = pc;
      connection.dataChannel = dc;
      dc.binaryType = "arraybuffer";

      dc.onopen = (): void => {
        connection.state = "open";
        connection.isReconnecting = false;
        this.#flushBufferedPackets(connection);
        this.#startIceRttPolling(connection);
      };
      dc.onclose = (): void => { if (connection.state !== "failed") connection.state = "closed"; };
      dc.onerror = (): void => { connection.state = "failed"; };
      dc.onmessage = (event: { data: unknown }): void => {
        this.#handleIncomingPacket(connection, event.data);
      };
      pc.onicecandidate = (event: { candidate?: { candidate?: string } | null }): void => {
        if (event.candidate?.candidate && !connection.iceCandidate) {
          connection.iceCandidate = event.candidate.candidate;
        }
      };

      const offer = await pc.createOffer({ iceRestart: false });
      let localSdp = offer.sdp ?? "";
      if (connection.iceCandidate) {
        localSdp = `${removeIceCandidates(localSdp)}a=${connection.iceCandidate}\r\na=end-of-candidates\r\n`;
      }
      await pc.setLocalDescription({ type: "offer", sdp: localSdp });
      const remoteSdp = buildRemoteRelayAnswer(localSdp, {
        ...connection.info,
        port: getRtcConnectPort(connection.info),
      });
      await pc.setRemoteDescription({ type: "answer", sdp: remoteSdp });
    } catch {
      connection.state = "failed";
    } finally {
      connection.isReconnecting = false;
    }
  };

  #handleIncomingPacket = (connection: RelayConnectionRuntime, raw: unknown): void => {
    const packet = toUint8Array(raw);
    if (!packet) return;
    connection.stats.receivedPackets += 1;
    connection.stats.receivedBytes += packet.byteLength;
    connection.hasReceivedFirstPacket = true;
    connection.lastRxPacketTime = Date.now();
    this.#totals.receivedPackets += 1;
    this.#totals.receivedBytes += packet.byteLength;
    appendRelayPacketLog("recv", connection, packet);
    this.config.onTransportMessage(packet, connection.info.ip, getVoipStackPort(connection.info));
  };

  #flushBufferedPackets = (connection: RelayConnectionRuntime): void => {
    while (
      connection.state === "open" &&
      connection.dataChannel?.readyState === "open" &&
      connection.packetBuffer.length > 0
    ) {
      const packet = shiftPacket(connection);
      if (!packet) break;
      if (!this.#sendBufferedPacket(connection, packet)) {
        connection.stats.droppedPackets += 1;
        break;
      }
    }
  };

  #sendBufferedPacket = (connection: RelayConnectionRuntime, packet: ArrayBuffer): boolean => {
    try {
      connection.dataChannel?.send(packet);
      if (classifyRelayPacket(packet) === "non_stun") {
        connection.hasNonStunPacketSent = true;
        connection.sentMedia = true;
      }
      connection.stats.sentPackets += 1;
      connection.stats.sentBytes += packet.byteLength;
      this.#totals.sentPackets += 1;
      this.#totals.sentBytes += packet.byteLength;
      appendRelayPacketLog("send", connection, packet);
      return true;
    } catch {
      return false;
    }
  };

  #startIceRttPolling = (connection: RelayConnectionRuntime): void => {
    this.#stopIceRttPolling(connection);
    const pc = connection.peerConnection;
    if (!pc || typeof pc.getStats !== "function" || !this.config.onIceRtt) return;

    const poll = async (): Promise<void> => {
      const rttMs = await this.#readCurrentRoundTripTimeMs(pc);
      if (rttMs == null || connection.lastIceRttMs === rttMs) return;
      connection.lastIceRttMs = rttMs;
      this.config.onIceRtt?.(rttMs, connection.info.ip, connection.info.port);
    };
    void poll();
    connection.iceStatsInterval = setInterval(() => { void poll(); }, ICE_RTT_POLL_MS);
    connection.iceStatsInterval.unref?.();
  };

  #stopIceRttPolling = (connection: RelayConnectionRuntime): void => {
    if (connection.iceStatsInterval) {
      clearInterval(connection.iceStatsInterval);
      connection.iceStatsInterval = null;
    }
    connection.lastIceRttMs = null;
  };

  #readCurrentRoundTripTimeMs = async (pc: any): Promise<number | null> => {
    try {
      const stats = await pc.getStats();
      const reports = stats && typeof stats.values === "function"
        ? Array.from(stats.values())
        : Array.isArray(stats) ? stats : Object.values(stats ?? {});
      if (!reports.length) return null;

      let selectedCandidatePairId = "";
      for (const report of reports as any[]) {
        if (report?.type === "transport" && typeof report.selectedCandidatePairId === "string") {
          selectedCandidatePairId = report.selectedCandidatePairId;
          break;
        }
      }

      const candidatePairs = (reports as any[]).filter((r) => r?.type === "candidate-pair");
      const selectedPair =
        (selectedCandidatePairId && candidatePairs.find((r) => r?.id === selectedCandidatePairId)) ||
        candidatePairs.find((r) => r?.selected || r?.nominated) ||
        candidatePairs[0];

      const rttSeconds =
        typeof selectedPair?.currentRoundTripTime === "number"
          ? selectedPair.currentRoundTripTime
          : typeof selectedPair?.totalRoundTripTime === "number" &&
              typeof selectedPair?.responsesReceived === "number" &&
              selectedPair.responsesReceived > 0
            ? selectedPair.totalRoundTripTime / selectedPair.responsesReceived
            : null;
      if (rttSeconds == null || !Number.isFinite(rttSeconds) || rttSeconds <= 0) return null;
      return Math.max(1, Math.round(rttSeconds * 1000));
    } catch {
      return null;
    }
  };

  #closeConnection = (id: string): void => {
    const connection = this.#connections.get(id);
    if (!connection) return;
    if (connection.connectionTimeout) {
      clearTimeout(connection.connectionTimeout);
      connection.connectionTimeout = null;
    }
    this.#closePeerObjects(connection);
    connection.state = "closed";
    connection.packetBuffer = [];
    connection.bufferedBytes = 0;
    this.#connections.delete(id);
  };

  #closePeerObjects = (connection: RelayConnectionRuntime): void => {
    this.#stopIceRttPolling(connection);
    try { connection.dataChannel?.close?.(); } catch {}
    try { connection.peerConnection?.close?.(); } catch {}
    connection.dataChannel = null;
    connection.peerConnection = null;
  };

  #loadWrtc = (): Promise<any> => {
    this.#wrtcPromise ??= import("@roamhq/wrtc").then(
      (module) => (module.default ?? module) as any,
    );
    return this.#wrtcPromise;
  };
}
