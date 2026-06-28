package com.raftkv.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public final class WalRecord {
    public enum Type { TERM, VOTE, ENTRY}

    private final Type type;
    private final long term;
    private final String votedFor;
    private final long index;
    private final String command;

    @JsonCreator
    public WalRecord(
        @JsonProperty("type") Type type,
        @JsonProperty("term") long term,
        @JsonProperty("votedFor") String votedFor,
        @JsonProperty("index") long index,
        @JsonProperty("command") String command
    ){
        this.type = type;
        this.term = term;
        this.votedFor = votedFor;
        this.index = index;
        this.command = command;
    }

    public static WalRecord forTerm(long term){
        return new WalRecord(Type.TERM, term, null, 0, null);
    }

    public static WalRecord forVote(long term, String votedFor){
        return new WalRecord(Type.VOTE, term, votedFor, 0, null);
    }

    public static WalRecord forEntry(long term, long index, String command){
        return new WalRecord(Type.ENTRY, term, null, index, command);
    }
}
