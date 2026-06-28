package com.raftkv.network;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public final class RequestVoteResponse {
    private final long term;
    private final boolean voteGranted;

    @JsonCreator
    public RequestVoteResponse(
        @JsonProperty("term") long term,
        @JsonProperty("voteGranted") boolean voteGranted){
            this.term = term;
            this.voteGranted = voteGranted;
        }
}
