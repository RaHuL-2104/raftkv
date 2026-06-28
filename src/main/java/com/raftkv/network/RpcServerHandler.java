package com.raftkv.network;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public final class RpcServerHandler extends SimpleChannelInboundHandler<RpcMessage>{
    private static final Logger log = LoggerFactory.getLogger(RpcServerHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RpcMessageHandler handler;

    public RpcServerHandler(RpcMessageHandler handler){
        this.handler = handler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg) throws Exception{
        switch(msg.getType()){
            case REQUEST_VOTE_REQUEST -> {
                RequestVoteRequest req = MAPPER.readValue(msg.getPayload(), RequestVoteRequest.class);
                RequestVoteResponse resp = handler.onRequestVote(req);
                String payload = MAPPER.writeValueAsString(resp);
                ctx.writeAndFlush(new RpcMessage(RpcMessage.Type.REQUEST_VOTE_RESPONSE, payload));
            }
            
            case APPEND_ENTRIES_REQUEST -> {
                AppendEntriesRequest req = MAPPER.readValue(msg.getPayload(), AppendEntriesRequest.class);
                AppendEntriesResponse resp = handler.onAppendEntries(req);
                String payload = MAPPER.writeValueAsString(resp);
                ctx.writeAndFlush(new RpcMessage(RpcMessage.Type.APPEND_ENTRIES_RESPONSE, payload));
            }
            default -> log.warn("RpcServerHandler received unexpected message type: {}", msg.getType());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause){
        log.error("Error in RpcServerHandler", cause);
        ctx.close();
    }
}
