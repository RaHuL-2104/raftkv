package com.raftkv.network;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public final class RpcMessage {
    public enum Type {
        REQUEST_VOTE_REQUEST,
        REQUEST_VOTE_RESPONSE,
        APPEND_ENTRIES_REQUEST,
        APPEND_ENTRIES_RESPONSE
    }

    private final Type type;
    private final String payload;

    @JsonCreator
    public RpcMessage(
        @JsonProperty("type") Type type,
        @JsonProperty("payload") String payload
    ){
        this.type = type;
        this.payload = payload;
    }
}
