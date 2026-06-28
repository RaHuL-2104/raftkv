package com.raftkv.raft;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;

public class PeerReplicationState {
    @Getter
    private final String peerId;
    private final AtomicLong nextIndex;
    private final AtomicLong matchIndex;

    public PeerReplicationState(String peerId, long initialNextIndex){
        this.peerId = peerId;
        this.nextIndex = new AtomicLong(initialNextIndex);
        this.matchIndex = new AtomicLong(0);
    }
    
    public long getNextIndex(){
        return nextIndex.get();
    }

    public void setNextIndex(long v){
        nextIndex.set(v);
    }

    public long getMatchIndex(){
        return matchIndex.get();
    }

    public void setMatchIndex(long v){
        matchIndex.set(v);
    }

    public void advanceAfterSuccess(long lastEntrySentIndex){
        matchIndex.set(lastEntrySentIndex);
        matchIndex.set(lastEntrySentIndex + 1);
    }

    public void backOffAfterFailure(){
        long current = nextIndex.get();
        if(current > 1){
            nextIndex.set(current - 1);
        }
    }
}
