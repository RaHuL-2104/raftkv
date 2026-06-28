package com.raftkv.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;



import java.util.concurrent.atomic.AtomicInteger;
import com.raftkv.storage.WriteAheadLog;
import com.raftkv.storage.WalRecord;
import java.io.IOException;

public final class RaftState {

    private static final org.slf4j.Logger log4j = org.slf4j.LoggerFactory.getLogger(RaftState.class);
    private final AtomicLong currentTerm = new AtomicLong(0);
    private volatile String votedFor = null;
    private final List<LogEntry> log = new ArrayList<>();

    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicLong lastApplied = new AtomicLong(0);
    private final AtomicLong snapshotOffset = new AtomicLong(0);
    private final AtomicLong snapshotTerm = new AtomicLong(0);


    private final WriteAheadLog wal;

    public RaftState(WriteAheadLog wal){
        this.wal = wal;
    }
    public enum Role {FOLLOWER, CANDIDATE, LEADER}
    private volatile Role role = Role.FOLLOWER;

    void restoreTerm(long term){
        currentTerm.set(term);
    }
    void restoreVote(String nodeId){
        votedFor = nodeId;
    }
    void restoreFromSnapshot(long lastIncludedIndex, long lastIncludedTerm){
        snapshotOffset.set(lastIncludedIndex);
        snapshotTerm.set(lastIncludedTerm);
        lastApplied.set(lastIncludedIndex);
        commitIndex.set(lastIncludedIndex);
    }
    public long getSnapshotOffset(){
        return snapshotOffset.get();
    }
    public long getSnapshotTerm(){
        return snapshotTerm.get();
    }
    void restoreEntry(LogEntry entry){
        log.add(entry);
    }
    public long getCurrentTerm() {
        return currentTerm.get();
    }
    public void setCurrentTerm(long term){
        persistTerm(term);
        currentTerm.set(term);
    }
    public long incrementTerm(){
        long newTerm =  currentTerm.incrementAndGet();
        persistTerm(newTerm);
        return newTerm;
    }

    private void persistTerm(long term){
        try{
            wal.append(WalRecord.forTerm(term));
        } catch(IOException e){
            log4j.error("FATAL: failed to persist term {} to WAL", term, e);
            throw new RuntimeException("WAL write failed - cannot safely continue",e);
        }
    }

    public String getVotedFor(){
        return votedFor;
    }
    public void setVotedFor(String nodeId){
        persistVote(currentTerm.get(), nodeId);
        this.votedFor = nodeId;
    }

    private void persistVote(long term, String votedFor){
        try{
            wal.append(WalRecord.forVote(term, votedFor));
        } catch(IOException e){
            log4j.error("FATAL : faileed to persist vote to WAL", e);
            throw new RuntimeException("WAL write failed - cannot safely continue", e);
        }
    }
    public synchronized void appendEntry(LogEntry entry){
        try{
            wal.append(WalRecord.forEntry(entry.getTerm(), entry.getIndex(), entry.getCommand()));
        }catch(IOException e){
            log4j.error("FATAL: failed to persist log entry {} to WAL", entry.getIndex(), e);
            throw new RuntimeException("WAL write failed - cannot safely continue", e);
        }
        log.add(entry);
    }

    public synchronized void appendOrOverwrite(List<LogEntry> newEntries){
        for(LogEntry newEntry : newEntries){
            int zeroBasedIndex = (int) (newEntry.getIndex() - snapshotOffset.get() - 1);
            if(zeroBasedIndex < log.size()){
                LogEntry existing = log.get(zeroBasedIndex);
                if(existing.getTerm() != newEntry.getTerm()){
                    while(log.size() > zeroBasedIndex){
                        log.remove(log.size() - 1);
                    }
                    persistEntry(newEntry);
                    log.add(newEntry);
                }
            } else{
                persistEntry(newEntry);
                log.add(newEntry);
            }
        }
    }

    private void persistEntry(LogEntry entry){
        try{
            wal.append(WalRecord.forEntry(entry.getTerm(), entry.getIndex(), entry.getCommand()));
        } catch (IOException e){
            log4j.error("FATAL: failed to persist log entry {} to WAL", entry.getIndex(), e);
            throw new RuntimeException("WAL write failed - cannot safely continue", e);
        }
    }
    public synchronized LogEntry getEntry(long logIndex){
        int arrayPos = (int) (logIndex - snapshotOffset.get() - 1);
        return log.get(arrayPos);
    }
    public synchronized int getLogSize(){
        return log.size();
    }
    public synchronized long getLastLogIndex(){
        if( !log.isEmpty()) return log.get(log.size() - 1).getIndex();
        return snapshotOffset.get();

    }
    public synchronized long getLastLogTerm(){
         if(!log.isEmpty()) return  log.get(log.size() - 1).getTerm();
         return snapshotTerm.get();
    }
    public long getCommitIndex(){
        return commitIndex.get();
    }
    public void setCommitIndex(long index){
        commitIndex.set(index);
    }
    public long getLastApplied(){
        return lastApplied.get();
    }
    public void setLastApplied(long index){
        lastApplied.set(index);
    }
    public Role getRole(){
        return role;
    }
    public void setRole(Role role){
        this.role = role;
    }

    public synchronized void compactLogUpTo(long lastIncludedIndex, long lastIncludedTerm){
        int entriesToRemove = (int) (lastIncludedIndex - snapshotOffset.get());
        for(int i = 0; i < entriesToRemove && !log.isEmpty(); i++){
            log.remove(0);
        }

        snapshotOffset.set(lastIncludedIndex);
        snapshotTerm.set(lastIncludedTerm);
    }
}
