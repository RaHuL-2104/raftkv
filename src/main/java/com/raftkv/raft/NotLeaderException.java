package com.raftkv.raft;

public final class NotLeaderException extends RuntimeException{
    private final String knownLeaderId;
    public NotLeaderException(String knownLeaderId){
        super("Not the leader. Known leader: " + knownLeaderId);
        this.knownLeaderId = knownLeaderId;
    }

    public String getKnownLeaderId(){
        return knownLeaderId;
    }
}
