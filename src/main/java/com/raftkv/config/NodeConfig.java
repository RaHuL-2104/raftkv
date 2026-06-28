package com.raftkv.config;

import java.util.List;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public final class NodeConfig {
    private final String nodeId;
    private final String host;
    private final int port;
    private final List<PeerConfig> peers;

    private final int electionTimeoutMinMs;
    private final int electionTimeoutMaxMs;

    private final int heartbeatIntervalMs;

    public NodeConfig(String nodeId, String host, int port, List<PeerConfig> peers, int electionTimeoutMinMs, int electionTimeoutMaxMs, int heartbeatIntervalMs){
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.peers = peers;
        this.electionTimeoutMinMs = electionTimeoutMinMs;
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;

    }
}
