package com.raftkv.raft;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
public final class LogEntry {
    private final long term;
    private final long index;
    private final String command;

    @JsonCreator
    public LogEntry(
        @JsonProperty("term") long term,
        @JsonProperty("index") long index,
        @JsonProperty("command") String command) {
            this.term = term;
            this.index = index;
            this.command = command;
        }

        public long getTerm() {
            return term;
        }
        public long getIndex(){
            return index;
        }
        public String getCommand(){
            return command;
        }

        @Override
        public String toString(){
            return "LogEntry{term=" + term + ", index=" + index + ", command='" + command + "'}";
        }
}
