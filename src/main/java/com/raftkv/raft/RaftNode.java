package com.raftkv.raft;

import com.raftkv.config.NodeConfig;
import com.raftkv.config.PeerConfig;
import com.raftkv.network.*;
import com.raftkv.statemachine.KeyValueStateMachine;
import com.raftkv.storage.WalRecord;
import com.raftkv.storage.WriteAheadLog;
import com.raftkv.storage.Snapshot;
import com.raftkv.storage.SnapshotStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class RaftNode implements RpcMessageHandler{
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);
    private final NodeConfig config;
    private final WriteAheadLog wal;
    private final SnapshotStore snapshotStore;
    private static final int SNAPSHOT_THRESHOLD = 100;
    private final RaftState state;

    private final KeyValueStateMachine stateMachine = new KeyValueStateMachine();

    private final List<RpcClient> peerClients = new ArrayList<>();
    private final RpcServer server;
    private final ScheduledExecutorService executor = 
                    Executors.newSingleThreadScheduledExecutor(r ->{
                        Thread t = new Thread(r, "raft-core");
                        t.setDaemon(true);
                        return t;
                    });
    private final Random random = new Random();

    private ScheduledFuture<?> electionTimerHandle;
    private final Map<String, PeerReplicationState> replicationState = new ConcurrentHashMap<>();
    private final Map<Long, CompletableFuture<String>> pendingClientRequests = new ConcurrentHashMap<>();
    private volatile String currentLeaderId = null;

    public RaftNode(NodeConfig config){
        this.config = config;
        this.wal = new WriteAheadLog(config.getNodeId());
        this.snapshotStore = new SnapshotStore(config.getNodeId());
        this.state = new RaftState(wal);
        this.server = new RpcServer(config.getPort(), this);
    }

    public void start() throws InterruptedException{

        try{
            restoreFromSnapshotIfPresent();
            wal.open();
            replayWal();
        } catch (IOException e){
            throw new RuntimeException("Failed to open/replay WAL - cannot start safely", e);
        }
        server.start();
        for(PeerConfig peer : config.getPeers()){
            RpcClient client = new RpcClient(peer.getNodeId(), peer.getHost(), peer.getPort());
            peerClients.add(client);
            connectWithRetry(client, peer);
            // try{
            //     client.connect();
            //     peerClients.add(client);
            //     log.info("Node {} connected to peer {}", config.getNodeId(), peer.getNodeId());
            // } catch(Exception e){
            //     log.warn("Node {} could not connect to peer {} - will retry later", config.getNodeId(), peer.getNodeId());
            // }
        }
        state.setRole(RaftState.Role.FOLLOWER);
        //resetElectionTimer();
        waitForFirstPeerThenStartTimer();

        log.info("RaftNode {} started on port {}", config.getNodeId(), config.getPort());
    }

    private void replayWal() throws IOException{
        List<WalRecord> records = wal.replay();
        for(WalRecord record : records){
            switch(record.getType()){
                case TERM -> state.restoreTerm(record.getTerm());
                case VOTE -> state.restoreVote(record.getVotedFor());
                case ENTRY -> state.restoreEntry(new LogEntry(record.getTerm(), record.getIndex(), record.getCommand()));
            }
        }
        if(!records.isEmpty()){
            log.info("Node {} restored state from WAL: term={}, votedFor={}, logSize={}", config.getNodeId(), state.getCurrentTerm(), state.getVotedFor(), state.getLogSize());
        }
    }

    private void restoreFromSnapshotIfPresent() throws IOException{
        snapshotStore.load().ifPresent(snapshot -> {
            stateMachine.restoreState(snapshot.getStateMachineData());
            state.restoreFromSnapshot(snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm());
            log.info("Node {} restored from snapshot up to index {} ", config.getNodeId(), snapshot.getLastIncludedIndex());
        });
    }

    public CompletableFuture<String> submitCommand(String command){
        CompletableFuture<String> resultFuture = new CompletableFuture<>();
        executor.submit(() -> {
            if(state.getRole() != RaftState.Role.LEADER){
                resultFuture.completeExceptionally(
                    new NotLeaderException(currentLeaderId)
                );
                return;
            }

            long newIndex = state.getLastLogIndex() + 1;
            LogEntry entry = new LogEntry(state.getCurrentTerm(), newIndex, command);
            state.appendEntry(entry);

            log.info("Node {} appended new entry at index {}: {}", config.getNodeId(), newIndex, command);
            pendingClientRequests.put(newIndex, resultFuture);
            tryAdvanceCommitIndex();
            replicateToAllPeers();
        });
        return resultFuture;
    }

    private void waitForFirstPeerThenStartTimer(){
        boolean anyConnected = peerClients.isEmpty() || peerClients.stream().anyMatch(RpcClient::isActive);
        if(anyConnected){
            resetElectionTimer();
        } else{
            executor.schedule(this::waitForFirstPeerThenStartTimer, 50, TimeUnit.MILLISECONDS);
        }
    }
    private void connectWithRetry(RpcClient client, PeerConfig peer){
        executor.submit(() -> {
            try{
                client.connect();
                log.info("Node {} connected to peer {}", config.getNodeId(), peer.getNodeId());
            } catch(Exception e){
                log.debug("Node {} retrying connection to peer {} in 100ms", config.getNodeId(), peer.getNodeId());
                executor.schedule(() -> connectWithRetry(client, peer), 100, TimeUnit.MILLISECONDS);
            }
        });
    }

    public void shutdown(){
        if(electionTimerHandle != null){
            electionTimerHandle.cancel(false);
        }
        executor.shutdown();
        peerClients.forEach(RpcClient::shutdown);
        server.shutdown();
        try{
            wal.close();
        } catch(IOException e){
            log.warn("Error closing WAL for node {}", config.getNodeId(), e);
        }
        log.info("RaftNode {} shut down", config.getNodeId());
    }

    private void resetElectionTimer(){
        if(electionTimerHandle != null){
            electionTimerHandle.cancel(false);
        }

        int timeout = config.getElectionTimeoutMinMs() + random.nextInt(config.getElectionTimeoutMaxMs() - config.getElectionTimeoutMinMs());
        electionTimerHandle = executor.schedule(this::startElection, timeout, TimeUnit.MILLISECONDS);
    }

    private void startElection(){
        state.setRole(RaftState.Role.CANDIDATE);
        long newTerm = state.incrementTerm();
        state.setVotedFor(config.getNodeId());

        log.info("Node {} starting election for term {}", config.getNodeId(), newTerm);

        int clusterSize = config.getPeers().size() + 1;
        int votesNeeded = (clusterSize/2) + 1;
        AtomicInteger votesReceived = new AtomicInteger(1);

        if(votesReceived.get() >= votesNeeded){
            becomeLeader();
            return;
        }

        RequestVoteRequest voteRequest = new RequestVoteRequest(
            config.getNodeId(),
            newTerm,
            state.getLastLogIndex(),
            state.getLastLogTerm()
        );

        for(RpcClient peer : peerClients){
            peer.requestVote(voteRequest).whenComplete((response, error) -> {
                executor.submit(() -> handleVoteResponse(response, error, newTerm, votesReceived, votesNeeded));
            });
        }

        resetElectionTimer();
    }

    private void handleVoteResponse(RequestVoteResponse response, Throwable error, long electionTerm, AtomicInteger votesReceived, int votesNeeded){
        if(error != null){
            log.warn("Node {} got vote RPC error: {}", config.getNodeId(), error.getMessage());
            return;
        }

        if(response.getTerm() > state.getCurrentTerm()){
            log.info("Node {} stepping down - saw higher term {}", config.getNodeId(), response.getTerm());
            stepDown(response.getTerm());
            return;
        }

        if(state.getRole() != RaftState.Role.CANDIDATE || electionTerm != state.getCurrentTerm()){
            return;
        }
        if(response.isVoteGranted()){
            int votes = votesReceived.incrementAndGet();
            log.info("Node {} received vote - total {}/{}", config.getNodeId(), votes, votesNeeded);
            if(votes >= votesNeeded){
                becomeLeader();
            }
        }
    }

    private void becomeLeader(){
        if(state.getRole() != RaftState.Role.CANDIDATE) return;
        state.setRole(RaftState.Role.LEADER);
        currentLeaderId = config.getNodeId();
        log.info("*** Node {} became LEADER for term {} ***", config.getNodeId(), state.getCurrentTerm());

        if(electionTimerHandle != null){
            electionTimerHandle.cancel(false);
        }
        
        replicationState.clear();
        long optimisticNextIndex = state.getLastLogIndex() + 1;
        for(RpcClient peer : peerClients){
            replicationState.put(peer.getPeerId(), new PeerReplicationState(peer.getPeerId(), optimisticNextIndex));
        }
        // sendHeartbeats();
        replicateToAllPeers();
        executor.scheduleAtFixedRate(this::replicateToAllPeers, config.getHeartbeatIntervalMs() ,config.getHeartbeatIntervalMs() ,TimeUnit.MILLISECONDS);
    }

    private void stepDown(long newTerm){
        state.setCurrentTerm(newTerm);
        state.setRole(RaftState.Role.FOLLOWER);
        state.setVotedFor(null);
        resetElectionTimer();
    }

    // private void sendHeartbeats(){
    //     if(state.getRole() != RaftState.Role.LEADER) return;

    //     AppendEntriesRequest heartbeat = new AppendEntriesRequest(
    //         config.getNodeId(),
    //         state.getCurrentTerm(),
    //         state.getLastLogIndex(),
    //         state.getLastLogTerm(),
    //         state.getCommitIndex(),
    //         Collections.emptyList()
    //     );

    //     for(RpcClient peer : peerClients){
    //         peer.appendEntries(heartbeat).whenComplete((response, error) -> {
    //             executor.submit(() -> handleAppendEntriesResponse(response, error));
    //         });
    //     }
    // }

    private void replicateToAllPeers(){
        if(state.getRole() != RaftState.Role.LEADER) return;
        for(RpcClient peer : peerClients){
            replicateToPeer(peer);
        }
    }

    private void replicateToPeer(RpcClient peer){
        PeerReplicationState repState = replicationState.get(peer.getPeerId());
        if(repState == null) return;

        long nextIndex = repState.getNextIndex();
        long prevLogIndex = nextIndex - 1;
        long prevLogTerm = 0;
        if(prevLogIndex > 0){
            if(prevLogIndex == state.getSnapshotOffset()){
                prevLogTerm = state.getSnapshotTerm();
            } else if(prevLogIndex > state.getSnapshotOffset()){
                prevLogTerm = state.getEntry(prevLogIndex).getTerm();
            }
        }
        // long prevLogTerm = prevLogIndex > 0 && prevLogIndex <= state.getLogSize() ? state.getEntry((int) prevLogIndex - 1).getTerm() : 0;

        // List<LogEntry> entriesToSend = new ArrayList<>();
        // for(long i = nextIndex; i <= state.getLastLogIndex(); i++){
        //     entriesToSend.add(state.getEntry((int) i - 1));
        // }
        List<LogEntry> entriesToSend = new ArrayList<>();
        for(long i = nextIndex; i <= state.getLastLogIndex(); i++){
            entriesToSend.add(state.getEntry(i));
        }
        AppendEntriesRequest request = new AppendEntriesRequest(
            config.getNodeId(),
            state.getCurrentTerm(),
            prevLogIndex,
            prevLogTerm,
            state.getCommitIndex(),
            entriesToSend
        );

        long lastIndexInThisBatch = entriesToSend.isEmpty() ? prevLogIndex : entriesToSend.get(entriesToSend.size() - 1).getIndex();

        peer.appendEntries(request).whenComplete((response, error) -> {
            executor.submit(() -> handleAppendEntriesResponse(peer.getPeerId(), response, error, lastIndexInThisBatch));
        });
    }
    private void handleAppendEntriesResponse(String peerId, AppendEntriesResponse response, Throwable error, long lastIndexSent){
        if(error != null){
            log.debug("Node {} heartbeat error to {}: {}", config.getNodeId(), peerId, error.getMessage());
            return;
        }

        if(response.getTerm() > state.getCurrentTerm()){
            log.info("Node {} stepping down - saw higher term {} from {}", config.getNodeId(), response.getTerm(), peerId);
            stepDown(response.getTerm());
            return;
        }

        if(state.getRole() != RaftState.Role.LEADER) return;

        PeerReplicationState repState = replicationState.get(peerId);
        if(repState == null) return;

        if(response.isSuccess()){
            repState.advanceAfterSuccess(lastIndexSent);
            log.debug("Node {} confirmed replication to {} up to index {}", config.getNodeId(), peerId, lastIndexSent);
            tryAdvanceCommitIndex();
        }else{
            repState.backOffAfterFailure();
            log.debug("Node {} backing off nextIndex for {} to {}", config.getNodeId(), peerId, repState.getNextIndex());
            for(RpcClient peer : peerClients){
                if(peer.getPeerId().equals(peerId)){
                    replicateToPeer(peer);
                    break;
                }
            }
        }
    }

    private void tryAdvanceCommitIndex(){
        long lastLogIndex = state.getLastLogIndex();
        int clusterSize = peerClients.size() + 1;
        int majority = (clusterSize / 2) + 1;

        for(long index = lastLogIndex; index > state.getCommitIndex(); index--){
            int countWithEntry = 1;
            for(PeerReplicationState repState : replicationState.values()){
                if(repState.getMatchIndex() >= index){
                    countWithEntry++;
                }
            }
            if(countWithEntry >= majority){
                long entryTerm = state.getEntry(index).getTerm();
                if(entryTerm == state.getCurrentTerm()){
                    state.setCommitIndex(index);
                    log.info("Node {} advanced commitIndex to {}", config.getNodeId(), index);
                    applyCommittedEntries();
                }
                break;
            }
        }  
    }

    private void applyCommittedEntries(){
        while(state.getLastApplied() < state.getCommitIndex()){
            long nextToApply = state.getLastApplied() + 1;
            LogEntry entry = state.getEntry(nextToApply);
            String result = stateMachine.apply(entry.getCommand());
            state.setLastApplied(nextToApply);
            log.debug("Node {} applied index {} ({}): {}", config.getNodeId(), nextToApply, entry.getCommand(), result);

            CompletableFuture<String> waiting = pendingClientRequests.remove(nextToApply);
            if(waiting != null){
                waiting.complete(result);
            }
        }
        maybeTakeSnapshot();
    }

    private void maybeTakeSnapshot(){
        long entriesSinceSnapshot = state.getLastApplied() - state.getSnapshotOffset();
        if(entriesSinceSnapshot < SNAPSHOT_THRESHOLD) return;

        long snapshotIndex = state.getLastApplied();
        long snapshotTerm = state.getEntry(snapshotIndex).getTerm();

        Snapshot snapshot = new Snapshot(snapshotIndex, snapshotTerm, stateMachine.exportState());

        try{
            snapshotStore.save(snapshot);
            state.compactLogUpTo(snapshotIndex, snapshotTerm);
            wal.truncate();
            wal.append(WalRecord.forTerm(state.getCurrentTerm()));
            if(state.getVotedFor() != null){
                wal.append(WalRecord.forVote(state.getCurrentTerm(), state.getVotedFor()));
            }

            log.info("Node {} took snapshot at index {}, compacted log, truncated WAL", config.getNodeId(), snapshotIndex);
        } catch(IOException e){
            log.error("Node  {} failed to save snapshot - will retry next time threshold is hit", config.getNodeId(), e);
        }
    }

    @Override
    public RequestVoteResponse onRequestVote(RequestVoteRequest request){
        try{
        return executor.submit(() -> {
            if(request.getTerm() <  state.getCurrentTerm()){
                log.info("Node {} rejecting vote for {} - stale term",
                    config.getNodeId(), request.getCandidateId()
                );
                return new RequestVoteResponse(state.getCurrentTerm(), false);
            }
            if(request.getTerm() > state.getCurrentTerm()){
                stepDown(request.getTerm());
            }

            boolean alreadyVoted = state.getVotedFor() != null && !state.getVotedFor().equals(request.getCandidateId());
            // log.info("VOTE-DEBUG voter={} candTerm={} candIdx={} myLastTerm={} myLastIdx={} mySnapOffset={} mySnapTerm={}",  
            //     config.getNodeId(), request.getLastLogTerm(), request.getLastLogIndex(), state.getLastLogTerm(), state.getLastLogIndex(),
            //     state.getSnapshotOffset(), state.getSnapshotTerm()
            // );

            boolean candidateLogUpToDate = request.getLastLogTerm() > state.getLastLogTerm() || (request.getLastLogTerm() == state.getLastLogTerm() && request.getLastLogIndex() >= state.getLastLogIndex());


            if(!alreadyVoted && candidateLogUpToDate){
                state.setVotedFor(request.getCandidateId());
                resetElectionTimer();
                log.info("Node {} granted vote to {} for term {}", config.getNodeId(), request.getCandidateId(), request.getTerm());
                return new RequestVoteResponse(state.getCurrentTerm(), true);
            }

            log.info("Node {} denied vote to {} (alreadyVoted = {}, logUpToDate = {})",
                            config.getNodeId(), request.getCandidateId(), alreadyVoted, candidateLogUpToDate);
            return new RequestVoteResponse(state.getCurrentTerm(), false);


        }).get();
    }catch(ExecutionException | InterruptedException e){
        log.error("Node {} failed to process RequestVote", config.getNodeId(), e);
        return new RequestVoteResponse(state.getCurrentTerm(), false);
    }
}

    @Override
    public AppendEntriesResponse onAppendEntries(AppendEntriesRequest request){
        try{
        return executor.submit(() -> {
            if(request.getTerm() < state.getCurrentTerm()){
                return new AppendEntriesResponse(state.getCurrentTerm(), false);
            }

            if(request.getTerm() > state.getCurrentTerm()){
                stepDown(request.getTerm());
            } else {
                resetElectionTimer();
            }
            currentLeaderId = request.getLeaderId();
            state.setRole(RaftState.Role.FOLLOWER);

            // if(request.getLeaderCommit() > state.getCommitIndex()){
            //     state.setCommitIndex(Math.min(request.getLeaderCommit(), state.getLastLogIndex()));
            // }
            // return new AppendEntriesResponse(state.getCurrentTerm(), true);

            long prevLogIndex = request.getPrevLogIndex();
            if(prevLogIndex > 0){
                if(prevLogIndex > state.getLastLogIndex()){
                    log.debug("Node {} rejecting AppendEntries - missing prevLogIndex {}", config.getNodeId(), prevLogIndex);
                    return new AppendEntriesResponse(state.getCurrentTerm(), false);
                }

                // long ourTermAtPrevIndex = state.getEntry((int) prevLogIndex - 1).getTerm();
                long ourTermAtPrevIndex = prevLogIndex == state.getSnapshotOffset() ? state.getSnapshotTerm() : state.getEntry(prevLogIndex).getTerm();
                if(ourTermAtPrevIndex != request.getPrevLogTerm()){
                    log.debug("Node {} rejecting AppendEntries - term mismatch at index {}", config.getNodeId(), prevLogIndex);
                    return new AppendEntriesResponse(state.getCurrentTerm(), false);
                }
            }

            List<LogEntry> newEntries = request.getEntries();
            if(newEntries != null && !newEntries.isEmpty()){
                state.appendOrOverwrite(newEntries);
                log.debug("Node {} appended {} entries, lastLogIndex now {}", config.getNodeId(), newEntries.size(), state.getLastLogIndex());
            }

            if(request.getLeaderCommit() > state.getCommitIndex()){
                state.setCommitIndex(Math.min(request.getLeaderCommit(), state.getLastLogIndex()));
                applyCommittedEntries();
            }

            return new AppendEntriesResponse(state.getCurrentTerm(), true);
        }).get();
    } catch(ExecutionException | InterruptedException e){
        log.error("Node {} failed to process AppendEntries", config.getNodeId(),e);
        return new AppendEntriesResponse((state.getCurrentTerm()), false);
    }
}

    public String getNodeId() {
        return config.getNodeId();
    }
    public RaftState.Role getRole(){
        return state.getRole();
    }
    public long getCurrentTerm(){
        return state.getCurrentTerm();
    }
    public String getCurrentLeaderId(){
        return currentLeaderId;
    }
}

