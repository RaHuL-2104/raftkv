package com.raftkv.storage;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public final class Snapshot {

    private final long lastIncludedIndex;
    private final long lastIncludedTerm;
    private final Map<String, String> stateMachineData;

    @JsonCreator
    public Snapshot(
        @JsonProperty("lastIncludedIndex") long lastIncludedIndex,
        @JsonProperty("lastIncludedTerm") long lastIncludedTerm,
        @JsonProperty("stateMachineData") Map<String, String> stateMachineData){
        
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.stateMachineData = stateMachineData;
    }
}
