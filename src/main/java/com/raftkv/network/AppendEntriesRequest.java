package com.raftkv.network;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.raftkv.raft.LogEntry;
import lombok.Getter;
import lombok.ToString;
import java.util.List;
@Getter
@ToString
public final class AppendEntriesRequest {
    private final String leaderId;
    private final long term;
    private final long prevLogIndex;
    private final long prevLogTerm;
    private final long leaderCommit;
    private final List<LogEntry> entries;

    @JsonCreator
    public AppendEntriesRequest(
        @JsonProperty("leaderId") String leaderId,
        @JsonProperty("term") long term,
        @JsonProperty("prevLogIndex") long prevLogIndex,
        @JsonProperty("prevLogTerm") long prevLogTerm,
        @JsonProperty("leaderCommit") long leaderCommit,
        @JsonProperty("entries") List<LogEntry> entries){
            this.leaderId = leaderId;
            this.term = term;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.leaderCommit = leaderCommit;
            this.entries = entries;
        }
}
