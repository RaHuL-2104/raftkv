package com.raftkv.network;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public final class AppendEntriesResponse {
    private final long term;
    private final boolean success;

    @JsonCreator
    public AppendEntriesResponse(
        @JsonProperty("term") long term,
        @JsonProperty("success") boolean success){
            this.term = term;
            this.success = success;
        }
}
