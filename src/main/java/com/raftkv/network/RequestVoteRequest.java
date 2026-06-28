package com.raftkv.network;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class RequestVoteRequest {
    private final String candidateId;
    private final long term;
    private final long lastLogIndex;
    private final long lastLogTerm;

    @JsonCreator
    public RequestVoteRequest(
        @JsonProperty("candidateId") String candidateId,
        @JsonProperty("term") long term,
        @JsonProperty("lastLogIndex") long lastLogIndex,
        @JsonProperty("lastLogTerm") long lastLogTerm){
            this.candidateId = candidateId;
            this.term = term;
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }
}
