package com.raftkv.network;

public interface RpcMessageHandler {
    RequestVoteResponse onRequestVote(RequestVoteRequest request);
    AppendEntriesResponse onAppendEntries(AppendEntriesRequest request);
    
} 
