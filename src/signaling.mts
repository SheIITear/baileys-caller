/**
 * Signaling bridge.
 *
 * Glues the WASM VoIP stack to Baileys: encrypts outbound `offer` / `enc_rekey`
 * stanzas, decrypts inbound ones, manages TC tokens, multi-device JID routing,
 * and signal-session refresh.
 *
 * @author ShellTear
 */

export type BaileysSocket = {
  authState: any;
  signalRepository: any;
  generateMessageTag: () => string;
  query: (node: any) => Promise<any>;
  sendNode: (node: any) => Promise<void>;
  waitForMessage: (tag: string, timeoutMs: number) => Promise<any>;
  getUSyncDevices: (jids: string[], ignoreZeroDevices: boolean, forceQuery: boolean) => Promise<any[]>;
  presenceSubscribe: (jid: string) => Promise<void>;
  ws: any;
  ev: any;
};

export type SignalingBridgeConfig = {
  sock: BaileysSocket;
};

const S_WHATSAPP_NET = "@s.whatsapp.net";
const TC_TOKEN_REQUEST_TIMEOUT_MS = 3500;
const SESSION_CACHE_TTL_MS = 5 * 60_000;
const ACK_TIMEOUT_MS = 15_000;

let _baileysModule: any = null;

const loadBaileys = async (): Promise<any> => {
  if (_baileysModule) return _baileysModule;
  try {
    _baileysModule = await import("@whiskeysockets/baileys");
    return _baileysModule;
  } catch {
    throw new Error(
      "Could not import @whiskeysockets/baileys. Install it as a peer dependency.",
    );
  }
};

const getNodeChildren = (node: any): any[] =>
  Array.isArray(node.content) ? node.content : [];

const setNodeChildren = (node: any, children: any[]): void => {
  node.content = children.length ? children : undefined;
};

const replaceNodeChild = (node: any, tag: string, nextChild: any): void => {
  const children = getNodeChildren(node);
  const index = children.findIndex((c: any) => c.tag === tag);
  if (index >= 0) children[index] = nextChild;
  else children.push(nextChild);
  setNodeChildren(node, children);
};

const removeNodeChildrenByTag = (node: any, tag: string): void => {
  setNodeChildren(node, getNodeChildren(node).filter((c: any) => c.tag !== tag));
};

const parseCountAttr = (value: unknown, fallback = 0): number => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

export class SignalingBridge {
  readonly #sock: BaileysSocket;
  #baileys: any = null;
  #voip: any = null;

  readonly #observedTcTokens = new Map<string, { token: Uint8Array; timestamp: string }>();
  readonly #pendingTcTokenWaiters = new Map<string, ((token: Uint8Array | undefined) => void)[]>();
  readonly #ensuredSignalSessions = new Map<string, number>();
  readonly #remoteDevicePeerByCallId = new Map<string, string>();
  readonly #remoteObfuscatedPeerByCallId = new Map<string, string>();
  readonly #remoteXmppRoutePeerByCallId = new Map<string, string>();
  readonly #incomingCallPeerById = new Map<string, string>();

  #outgoingSignalingQueue = Promise.resolve<void>(undefined);
  #incomingSignalingQueue = Promise.resolve<void>(undefined);

  constructor(config: SignalingBridgeConfig) {
    this.#sock = config.sock;
  }

  /** Hand the WASM engine in so we can dispatch ack callbacks back to it. */
  attachEngine = (voip: any): void => {
    this.#voip = voip;
  };

  init = async (): Promise<void> => {
    this.#baileys = await loadBaileys();

    // Hook auth-state writes so we observe TC tokens as they land.
    const originalKeysSet = this.#sock.authState.keys.set.bind(this.#sock.authState.keys);
    this.#sock.authState.keys.set = async (data: any) => {
      const result = await originalKeysSet(data);
      for (const [jid, entry] of Object.entries<any>(data?.tctoken ?? {})) {
        if (entry?.token instanceof Uint8Array && entry.token.length > 0) {
          this.#rememberTcToken(jid, entry.token, entry.timestamp);
        }
      }
      return result;
    };
  };

  sendSignaling = (peerJid: string, callId: string, xmlPayload: Uint8Array): void => {
    this.#outgoingSignalingQueue = this.#outgoingSignalingQueue
      .then(() => this.#doSendSignaling(peerJid, callId, xmlPayload))
      .catch(() => {});
  };

  processIncomingCall = (node: any, voip: any, activeCallId: string): void => {
    this.#incomingSignalingQueue = this.#incomingSignalingQueue
      .then(() => this.#doProcessIncomingCall(node, voip, activeCallId))
      .catch(() => {});
  };

  processIncomingReceipt = (node: any, voip: any, activeCallId: string): void => {
    this.#incomingSignalingQueue = this.#incomingSignalingQueue
      .then(() => this.#doProcessIncomingReceipt(node, voip, activeCallId))
      .catch(() => {});
  };

  requestTcToken = async (jid: string): Promise<Uint8Array | undefined> => {
    const userJid = this.#toBareJid(jid);
    const cached = await this.#getTcToken(userJid);
    if (cached?.length) return cached;

    try {
      const response = await (this.#sock as any).getPrivacyTokens([userJid]);
      const { getBinaryNodeChild, getAllBinaryNodeChildren } = this.#baileys;
      const tokensNode =
        getBinaryNodeChild(response, "tokens") ??
        getBinaryNodeChild(getBinaryNodeChild(response, "iq"), "tokens");
      const tokenNodes = tokensNode
        ? getAllBinaryNodeChildren(tokensNode).filter((c: any) => c.tag === "token")
        : [];

      for (const tokenNode of tokenNodes) {
        const tokenJid = String(tokenNode.attrs.jid ?? "");
        if (this.#baileys.jidNormalizedUser(tokenJid) !== this.#baileys.jidNormalizedUser(userJid)) continue;
        const content = tokenNode.content;
        if (content instanceof Uint8Array && content.length > 0) {
          const token = Buffer.from(content);
          await this.#sock.authState.keys.set({
            tctoken: { [userJid]: { token, timestamp: String(tokenNode.attrs.t ?? "") } },
          });
          return token;
        }
      }
    } catch {}

    return this.#getTcToken(userJid);
  };

  ensureTcToken = async (...jids: string[]): Promise<Uint8Array | undefined> => {
    const uniqueJids = [
      ...new Set(jids.map((j) => this.#toBareJid(String(j ?? "").trim())).filter(Boolean)),
    ];
    for (const jid of uniqueJids) {
      const cached = await this.#getTcToken(jid);
      if (cached?.length) return cached;
    }
    for (const jid of uniqueJids) {
      const fetched = await Promise.race<Uint8Array | undefined>([
        this.requestTcToken(jid),
        new Promise<undefined>((r) => setTimeout(() => r(undefined), TC_TOKEN_REQUEST_TIMEOUT_MS)),
      ]);
      if (fetched?.length) return fetched;
    }
    return undefined;
  };

  discoverPeerDevices = async (peerLidJid: string): Promise<string[]> => {
    const devices = await this.#sock.getUSyncDevices([peerLidJid], true, false);
    return this.#normalizeStartCallPeerList(devices.map((d: any) => d.jid).filter(Boolean));
  };

  ensureSessionsForPeers = async (jids: string[]): Promise<void> => {
    const targets = this.#expandSignalSessionTargets(jids);
    if (targets.length) await this.#ensureSignalSessions(targets, true);
  };

  resolveLid = async (pnJid: string): Promise<string | undefined> =>
    this.#sock.signalRepository.lidMapping?.getLIDForPN(pnJid);

  issueTcToken = async (jid: string): Promise<boolean> => {
    const userJid = this.#toBareJid(jid);
    const issuedAt = Math.floor(Date.now() / 1000);
    try {
      await this.#sock.query({
        tag: "iq",
        attrs: {
          to: S_WHATSAPP_NET, type: "set", xmlns: "privacy",
          id: this.#sock.generateMessageTag(),
        },
        content: [{
          tag: "tokens", attrs: {},
          content: [{
            tag: "token",
            attrs: { jid: userJid, t: String(issuedAt), type: "trusted_contact" },
          }],
        }],
      });
      return true;
    } catch {
      return false;
    }
  };

  getRemoteDeviceJid = (callId: string): string | undefined =>
    this.#remoteDevicePeerByCallId.get(callId);

  // ─── private — outbound signaling ─────────────────────────────────────────

  #doSendSignaling = async (peerJid: string, callId: string, xmlPayload: Uint8Array): Promise<void> => {
    const { decodeBinaryNode, getBinaryNodeChild } = this.#baileys;

    const rawPayload = Buffer.from(xmlPayload);
    let voipNode: any;
    try {
      voipNode = await decodeBinaryNode(Buffer.concat([Buffer.from([0]), rawPayload]));
    } catch {
      voipNode = await decodeBinaryNode(rawPayload);
    }

    const signalingTag = String(voipNode.tag);
    const effectivePeerJid = this.#resolveOutboundPeerJid(callId, peerJid);

    if (signalingTag === "offer" && !voipNode.attrs["call-creator"]) {
      const selfLid = this.#sock.authState.creds.me?.lid;
      if (selfLid) voipNode.attrs["call-creator"] = selfLid;
    }

    // Multi-destination encryption (offer/enc_rekey with <destination>).
    const destination = getBinaryNodeChild(voipNode, "destination");
    if (destination) {
      const destinations = getNodeChildren(destination);
      const destinationJids = destinations
        .map((n: any) => String(n.attrs.jid ?? "").trim())
        .filter(Boolean);
      const sessionTargets = this.#expandSignalSessionTargets(destinationJids);
      if (sessionTargets.length) await this.#ensureSignalSessions(sessionTargets, signalingTag === "offer");

      const rootEnc = getBinaryNodeChild(voipNode, "enc");
      const encCount = parseCountAttr(rootEnc?.attrs.count);
      let includeDeviceIdentity = false;

      for (const destNode of destinations) {
        const targetJid = String(destNode.attrs.jid ?? "").trim();
        const destEnc = getBinaryNodeChild(destNode, "enc");
        if (!targetJid || !destEnc || !(destEnc.content instanceof Uint8Array)) continue;
        try {
          const encrypted = await this.#encryptCallKey(targetJid, destEnc.content, encCount);
          includeDeviceIdentity = includeDeviceIdentity || encrypted.shouldIncludeDeviceIdentity;
          setNodeChildren(destNode, [encrypted.encNode]);
        } catch {
          for (const d of destinations) removeNodeChildrenByTag(d, "enc");
          break;
        }
      }
      if (includeDeviceIdentity) this.#appendDeviceIdentity(voipNode);

      await this.#sendCallStanza(this.#toBareJid(peerJid), voipNode, signalingTag, effectivePeerJid, peerJid);
      return;
    }

    // Single-target encryption.
    if (signalingTag === "offer" || signalingTag === "enc_rekey") {
      const enc = getBinaryNodeChild(voipNode, "enc");
      if (enc && enc.content instanceof Uint8Array) {
        const targetJid = this.#toCallDeviceJid(effectivePeerJid);
        const encrypted = await this.#encryptCallKey(targetJid, enc.content, parseCountAttr(enc.attrs.count));
        replaceNodeChild(voipNode, "enc", encrypted.encNode);
        if (encrypted.shouldIncludeDeviceIdentity) this.#appendDeviceIdentity(voipNode);

        await this.#sendCallStanza(targetJid, voipNode, signalingTag, effectivePeerJid, peerJid);
        return;
      }
    }

    // Non-encrypted signaling (accept, transport, terminate, etc.).
    const routeTo = signalingTag !== "offer" && signalingTag !== "enc_rekey"
      ? this.#toBareJid(effectivePeerJid)
      : this.#toCallDeviceJid(effectivePeerJid);
    await this.#sendCallStanza(routeTo, voipNode, signalingTag, effectivePeerJid, peerJid);
  };

  /**
   * Send a call stanza and feed the resulting server ack back to the WASM —
   * without this, the WASM stalls and never receives the relay-list update.
   */
  #sendCallStanza = async (
    routeTo: string,
    voipNode: any,
    signalingTag: string,
    effectivePeerJid: string,
    callbackPeerJid: string,
  ): Promise<void> => {
    const stanzaId = this.#sock.generateMessageTag();
    await this.#sock.sendNode({
      tag: "call",
      attrs: { to: routeTo, id: stanzaId },
      content: [voipNode],
    });

    void (async () => {
      try {
        const ackNode = await this.#sock.waitForMessage(stanzaId, ACK_TIMEOUT_MS);
        if (!ackNode || !this.#voip) return;
        const { encodeBinaryNode } = this.#baileys;
        const ackPayload = Buffer.from(encodeBinaryNode(ackNode)).toString("base64");
        const tcToken = await this.ensureTcToken(effectivePeerJid, callbackPeerJid);
        try {
          this.#voip.handleSignalingAck({
            payload: ackPayload,
            ackError: ackNode.attrs?.error ?? "0",
            msgType: ackNode.attrs?.type ?? signalingTag,
            peerJid: effectivePeerJid,
            extraData: tcToken,
          });
        } catch {}
      } catch {}
    })();
  };

  // ─── private — inbound signaling ──────────────────────────────────────────

  #doProcessIncomingCall = async (node: any, voip: any, activeCallId: string): Promise<void> => {
    const { getAllBinaryNodeChildren, getBinaryNodeChild, encodeBinaryNode } = this.#baileys;

    const voipChild = getAllBinaryNodeChildren(node)[0];
    if (!voipChild) return;

    const incomingCallId = String(voipChild.attrs["call-id"] ?? voipChild.attrs.call_id ?? "");
    const callIdForRouting = incomingCallId || activeCallId;
    if (activeCallId && incomingCallId && incomingCallId !== activeCallId) return;

    const senderDeviceJid =
      String(voipChild.attrs.participant ?? "") ||
      String(node.attrs.participant ?? "") ||
      String(node.attrs.from ?? "") ||
      String(voipChild.attrs["call-creator"] ?? "");
    const callbackPeerJid = String(node.attrs.from ?? "") || senderDeviceJid;
    const platform = voipChild.attrs.platform ?? node.attrs.platform ?? "";
    const appVersion = voipChild.attrs.version ?? node.attrs.version ?? "";
    const epochId = voipChild.attrs.e ?? node.attrs.e ?? "0";
    const timestamp = voipChild.attrs.t ?? node.attrs.t ?? "0";
    const offline = !!(voipChild.attrs.offline ?? node.attrs.offline);

    let usableNode = voipChild;
    if (getBinaryNodeChild(voipChild, "enc")) {
      usableNode = await this.#maybeDecryptEnc(voipChild, senderDeviceJid);
    }

    // A sender on its primary device (device 0, i.e. a phone) arrives as an AD_JID with device 0, which Baileys
    // decodes as a bare "user@lid" (jidEncode omits device 0). Re-encoding that bare JID writes a JID_PAIR, and
    // the engine then reads call-creator as user:0@s.whatsapp.net, sees it differ from the peer (user:0@lid) and
    // ignores the stanza ("mismatched peer id and creator id"): incoming offers from phones never ring. Desktop
    // senders keep a non-zero device (user:NN@lid) and are not affected. Restore device 0 before re-encoding.
    const callCreator = String(usableNode.attrs?.["call-creator"] ?? "");
    if (/^[0-9]+@lid$/.test(callCreator)) {
      usableNode.attrs["call-creator"] = callCreator.replace("@lid", ":0@lid");
    }

    const b64 = Buffer.from(encodeBinaryNode(usableNode)).toString("base64");

    const storedPeerJid = callIdForRouting ? this.#incomingCallPeerById.get(callIdForRouting) : undefined;
    let mappedRemoteDeviceJid = callIdForRouting ? this.#remoteDevicePeerByCallId.get(callIdForRouting) : undefined;

    if (callIdForRouting && (callbackPeerJid || senderDeviceJid)) {
      this.#remoteXmppRoutePeerByCallId.set(callIdForRouting, callbackPeerJid || senderDeviceJid);
      const hinted = this.#pickConcreteRouteHint(senderDeviceJid, callbackPeerJid);
      if (hinted && hinted !== mappedRemoteDeviceJid) {
        mappedRemoteDeviceJid = hinted;
        this.#remoteDevicePeerByCallId.set(callIdForRouting, hinted);
      }
    }

    const routedPeerJid = usableNode.tag === "offer"
      ? this.#preferDeviceRouteJid(senderDeviceJid, callbackPeerJid, storedPeerJid)
      : this.#preferOrderedRouteJid(mappedRemoteDeviceJid, storedPeerJid, senderDeviceJid, callbackPeerJid);

    if (callIdForRouting && routedPeerJid) {
      this.#incomingCallPeerById.set(callIdForRouting, routedPeerJid);
    }

    const tcToken = await this.ensureTcToken(routedPeerJid, callbackPeerJid);

    switch (usableNode.tag) {
      case "offer":
        voip.handleSignalingOffer({
          payload: b64,
          peerPlatform: Number(platform || 0),
          peerAppVersion: appVersion,
          epochId, timestamp,
          isOffline: offline,
          isOfferNotContact: false,
          peerJid: routedPeerJid,
          tcToken,
        });
        break;
      case "ack":
        voip.handleSignalingAck({
          payload: b64,
          ackError: usableNode.attrs.error ?? "0",
          msgType: usableNode.attrs.type ?? "",
          peerJid: routedPeerJid,
          extraData: tcToken,
        });
        break;
      default:
        voip.handleSignalingMessage({
          payload: b64,
          peerPlatform: platform,
          peerAppVersion: appVersion,
          epochId, timestamp,
          isOffline: offline,
          peerJid: routedPeerJid,
          tcToken,
        });
        if (callIdForRouting && (usableNode.tag === "terminate" || usableNode.tag === "reject")) {
          this.#incomingCallPeerById.delete(callIdForRouting);
          this.#remoteDevicePeerByCallId.delete(callIdForRouting);
          this.#remoteObfuscatedPeerByCallId.delete(callIdForRouting);
          this.#remoteXmppRoutePeerByCallId.delete(callIdForRouting);
        }
        break;
    }
  };

  #doProcessIncomingReceipt = async (node: any, voip: any, activeCallId: string): Promise<void> => {
    const { getAllBinaryNodeChildren, encodeBinaryNode } = this.#baileys;
    const receiptChild = getAllBinaryNodeChildren(node)[0];
    if (!receiptChild) return;

    const incomingCallId = String(receiptChild.attrs["call-id"] ?? receiptChild.attrs.call_id ?? "");
    const callIdForRouting = incomingCallId || activeCallId;
    if (activeCallId && incomingCallId && incomingCallId !== activeCallId) return;

    const callbackPeerJid = String(node.attrs.from ?? receiptChild.attrs["call-creator"] ?? "");
    const storedPeerJid = callIdForRouting ? this.#incomingCallPeerById.get(callIdForRouting) : undefined;
    const routedPeerJid = this.#preferOrderedRouteJid(storedPeerJid, callbackPeerJid);
    if (callIdForRouting && routedPeerJid) this.#incomingCallPeerById.set(callIdForRouting, routedPeerJid);

    const tcToken = await this.ensureTcToken(routedPeerJid, callbackPeerJid);
    voip.handleSignalingReceipt({
      payload: Buffer.from(encodeBinaryNode(node)).toString("base64"),
      peerJid: routedPeerJid,
      tcToken,
    });
  };

  #maybeDecryptEnc = async (voipNode: any, peerJid: string): Promise<any> => {
    const { getBinaryNodeChild, unpadRandomMax16, proto } = this.#baileys;
    const enc = getBinaryNodeChild(voipNode, "enc");
    if (!enc || !(enc.content instanceof Uint8Array)) return voipNode;
    const type = enc.attrs.type;
    if (type !== "pkmsg" && type !== "msg") return voipNode;

    const candidates = [...new Set([peerJid, this.#toCallDeviceJid(peerJid)])].filter(Boolean);
    let lastErr: unknown;
    for (const jid of candidates) {
      try {
        const decrypted = await this.#sock.signalRepository.decryptMessage({
          jid, type, ciphertext: enc.content,
        });
        const parsed = proto.Message.decode(unpadRandomMax16(decrypted));
        const callKey = parsed.call?.callKey;
        if (!callKey || callKey.length === 0) {
          throw new Error("decrypted signaling has no call.callKey");
        }
        enc.content = callKey;
        return voipNode;
      } catch (err) {
        lastErr = err;
      }
    }
    throw lastErr;
  };

  #encryptCallKey = async (
    targetJid: string,
    rawCallKey: Uint8Array,
    count: number,
  ): Promise<{ encNode: any; shouldIncludeDeviceIdentity: boolean }> => {
    const { encodeWAMessage } = this.#baileys;
    const primaryDeviceJid = this.#toPrimaryDeviceJid(targetJid);
    const sessionTargets = primaryDeviceJid && primaryDeviceJid !== targetJid
      ? [primaryDeviceJid, targetJid]
      : [targetJid];
    await this.#ensureSignalSessions(sessionTargets, false);

    const { type, ciphertext } = await this.#sock.signalRepository.encryptMessage({
      jid: targetJid,
      data: encodeWAMessage({ call: { callKey: Buffer.from(rawCallKey) } }),
    });

    return {
      encNode: {
        tag: "enc",
        attrs: { v: "2", type, count: String(count) },
        content: Buffer.from(ciphertext),
      },
      shouldIncludeDeviceIdentity: type === "pkmsg",
    };
  };

  #ensureSignalSessions = async (jids: string[], refresh: boolean): Promise<void> => {
    const { parseAndInjectE2ESessions } = this.#baileys;
    const missing: string[] = [];

    for (const jid of [...new Set(jids.filter(Boolean))]) {
      const signalId = this.#sock.signalRepository.jidToSignalProtocolAddress(jid);
      const cachedAt = this.#ensuredSignalSessions.get(signalId);
      if (!refresh && cachedAt && Date.now() - cachedAt < SESSION_CACHE_TTL_MS) continue;
      if (!refresh) {
        const validation = await this.#sock.signalRepository.validateSession(jid);
        if (validation.exists) {
          this.#ensuredSignalSessions.set(signalId, Date.now());
          continue;
        }
      }
      missing.push(jid);
    }
    if (!missing.length) return;

    const sessionNode = await this.#sock.query({
      tag: "iq",
      attrs: { xmlns: "encrypt", type: "get", to: S_WHATSAPP_NET },
      content: [{
        tag: "key", attrs: {},
        content: missing.map((jid) => ({ tag: "user", attrs: { jid } })),
      }],
    });
    await parseAndInjectE2ESessions(sessionNode, this.#sock.signalRepository);
    for (const jid of missing) {
      this.#ensuredSignalSessions.set(
        this.#sock.signalRepository.jidToSignalProtocolAddress(jid),
        Date.now(),
      );
    }
  };

  #appendDeviceIdentity = (voipNode: any): void => {
    const { getBinaryNodeChild, encodeSignedDeviceIdentity } = this.#baileys;
    if (getBinaryNodeChild(voipNode, "device-identity")) return;
    const account = this.#sock.authState.creds.account;
    if (!account) return;
    const children = getNodeChildren(voipNode);
    children.push({
      tag: "device-identity",
      attrs: {},
      content: encodeSignedDeviceIdentity(account, true),
    });
    setNodeChildren(voipNode, children);
  };

  // ─── private — JID utilities ──────────────────────────────────────────────

  #toBareJid = (jid: string): string => {
    const { jidDecode, jidEncode } = this.#baileys;
    const decoded = jidDecode(jid);
    if (!decoded?.user) return jid;
    const server = jid.endsWith("@lid") ? "lid" : "s.whatsapp.net";
    return jidEncode(decoded.user, server);
  };

  #toCallDeviceJid = (jid: string): string => {
    const { jidDecode, jidEncode } = this.#baileys;
    const decoded = jidDecode(jid);
    if (!decoded?.user) return jid;
    const server = jid.endsWith("@lid") ? "lid" : "s.whatsapp.net";
    if (decoded.device == null) return jidEncode(decoded.user, server);
    return `${decoded.user}:${decoded.device}@${server}`;
  };

  #toPrimaryDeviceJid = (jid: string): string | undefined => {
    const { jidDecode, jidEncode } = this.#baileys;
    const decoded = jidDecode(jid);
    if (!decoded?.user) return undefined;
    const device = decoded.device;
    if (device == null || device === 0) return undefined;
    const server = jid.endsWith("@lid") ? "lid" : "s.whatsapp.net";
    return jidEncode(decoded.user, server);
  };

  #hasConcreteDevice = (jid: string): boolean => {
    const decoded = this.#baileys.jidDecode(jid);
    return !!decoded?.user && decoded.device != null;
  };

  #preferDeviceRouteJid = (...candidates: Array<string | undefined>): string => {
    for (const c of candidates) {
      const jid = String(c ?? "").trim();
      if (jid && this.#hasConcreteDevice(jid)) return jid;
    }
    for (const c of candidates) {
      const jid = String(c ?? "").trim();
      if (jid) return this.#toCallDeviceJid(jid);
    }
    return "";
  };

  #preferOrderedRouteJid = (...candidates: Array<string | undefined>): string => {
    for (const c of candidates) {
      const jid = String(c ?? "").trim();
      if (jid) return this.#toCallDeviceJid(jid);
    }
    return "";
  };

  #pickConcreteRouteHint = (...candidates: Array<string | undefined>): string => {
    for (const c of candidates) {
      const jid = String(c ?? "").trim();
      if (jid && this.#hasConcreteDevice(jid)) return jid;
    }
    return "";
  };

  #resolveOutboundPeerJid = (callId: string, wasmPeerJid: string): string => {
    const peerJid = String(wasmPeerJid ?? "").trim();
    if (!peerJid || !callId) return peerJid;
    return this.#remoteDevicePeerByCallId.get(callId) ?? peerJid;
  };

  #expandSignalSessionTargets = (jids: string[]): string[] =>
    [...new Set(jids.flatMap((jid) => {
      const primary = this.#toPrimaryDeviceJid(jid);
      return primary && primary !== jid ? [primary, jid] : [jid];
    }))];

  #normalizeStartCallPeerList = (jids: string[]): string[] => {
    const { jidDecode, jidEncode } = this.#baileys;
    const result = new Set<string>();
    for (const jid of jids) {
      const decoded = jidDecode(jid);
      if (!decoded?.user) {
        result.add(jid);
        continue;
      }
      const server = jid.endsWith("@lid") ? "lid" : "s.whatsapp.net";
      result.add(jidEncode(decoded.user, server));
      if (decoded.device != null) {
        result.add(`${decoded.user}:${decoded.device}@${server}`);
      }
    }
    return [...result].slice(0, 5);
  };

  // ─── private — TC token ───────────────────────────────────────────────────

  #rememberTcToken = (jid: string, token: Uint8Array, timestamp = ""): void => {
    const bareJid = this.#toBareJid(jid);
    if (!token.length) return;
    this.#observedTcTokens.set(bareJid, { token: Buffer.from(token), timestamp });
    const waiters = this.#pendingTcTokenWaiters.get(bareJid);
    if (waiters?.length) {
      this.#pendingTcTokenWaiters.delete(bareJid);
      for (const w of waiters) w(Buffer.from(token));
    }
  };

  #getTcToken = async (jid: string): Promise<Uint8Array | undefined> => {
    const userJid = this.#toBareJid(jid);
    const observed = this.#observedTcTokens.get(userJid)?.token;
    if (observed?.length) return Buffer.from(observed);
    try {
      const data = await this.#sock.authState.keys.get("tctoken", [userJid]);
      const token = data[userJid]?.token;
      if (token && token.length > 0) {
        this.#rememberTcToken(userJid, token, data[userJid]?.timestamp);
        return token;
      }
    } catch {}
    return undefined;
  };
}
