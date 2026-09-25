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
export declare class RelayRtcTransport {
    #private;
    private readonly config;
    constructor(config: RelayTransportConfig);
    updateRelayList: (update: RelayListUpdatePayload) => void;
    send: (packet: Uint8Array | Buffer, ip: string, port: number) => number;
    getStats: () => RelayTransportStats;
    /**
     * Close every connection and forget the relay list, but stay usable: the
     * next call's relay list opens fresh connections. For when a call ends.
     */
    closeConnections: () => void;
    closeAll: () => Promise<void>;
}
export {};
