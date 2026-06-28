package com.raftkv.config;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class PeerConfig {
    private final String nodeId;
    private final String host;
    private final int port;

    public PeerConfig(String nodeId, String host, int port){
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }
}
