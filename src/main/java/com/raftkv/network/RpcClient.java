package com.raftkv.network;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public final class RpcClient {
    private static final Logger log = LoggerFactory.getLogger(RpcClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long RPC_TIMEOUT_MS = 500;

    private final String peerId;
    private final String host;
    private final int port;
    private final EventLoopGroup group;

    private Channel channel;
    private final ConcurrentHashMap<Long, CompletableFuture<RpcMessage>> pendingRequests = new ConcurrentHashMap<>();
    private final AtomicLong requestIdGen = new AtomicLong(0);

    public RpcClient(String peerId, String host, int port){
        this.peerId = peerId;
        this.host = host;
        this.port = port;
        this.group = new NioEventLoopGroup(1);
    }

    public void connect() throws InterruptedException{
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch){
                        ch.pipeline().addLast(JsonCodec.newFrameDecoder());
                        ch.pipeline().addLast(JsonCodec.newFramePrepender());
                        ch.pipeline().addLast(new JsonCodec());
                        ch.pipeline().addLast(new ResponseHandler());
                    }
                });
        channel = bootstrap.connect(host, port).sync().channel();
        log.info("RpcClient connected to peer {} at {}:{}", peerId, host, port);
    }

    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request){
        return sendAndRecieve(RpcMessage.Type.REQUEST_VOTE_REQUEST, request)
                .thenApply(msg -> {
                    try{
                        return MAPPER.readValue(msg.getPayload(), RequestVoteResponse.class);
                    } catch(Exception e){
                        throw new RuntimeException("Failed to deserialize RequestVoteResponse", e);
                    }
                });
    }

    public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request){
        return sendAndRecieve(RpcMessage.Type.APPEND_ENTRIES_REQUEST, request)
                .thenApply(msg -> {
                    try{
                        return MAPPER.readValue(msg.getPayload(), AppendEntriesResponse.class);
                    } catch(Exception e){
                        throw new RuntimeException("Failed to deserialize AppendEntriesResponse", e);
                    }
                });
    }

    private CompletableFuture<RpcMessage> sendAndRecieve(RpcMessage.Type type, Object body){
        CompletableFuture<RpcMessage> future = new CompletableFuture<>();

        if(channel == null || !channel.isActive()){
            future.completeExceptionally(new RuntimeException("Not connected to peer " + peerId));
            return future;
        }

        long requestId = requestIdGen.incrementAndGet();
        pendingRequests.put(requestId, future);

        group.schedule(() -> {
            CompletableFuture<RpcMessage> pending = pendingRequests.remove(requestId);
            if(pending != null && !pending.isDone()){
                pending.completeExceptionally(new RuntimeException("RPC timeout to peer " + peerId));
            }
        }, RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        try{
            String payload = MAPPER.writeValueAsString(body);
            RpcMessage envelope = new RpcMessage(type, payload);
            channel.writeAndFlush(envelope);
        } catch(Exception e){
            pendingRequests.remove(requestId);
            future.completeExceptionally(e);
        }
        return future;
    }

    public String getPeerId(){
        return peerId;
    }
    public boolean isActive(){
        return channel != null && channel.isActive();
    }

    public void shutdown(){
        if(channel != null){
            channel.close();
        }
        group.shutdownGracefully();
        log.info("RpcClient to peer {} shut down", peerId);
    }

    private class ResponseHandler extends SimpleChannelInboundHandler<RpcMessage>{
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg){
            pendingRequests.entrySet().stream()
                            .min(java.util.Map.Entry.comparingByKey())
                            .ifPresent(entry ->{
                                pendingRequests.remove(entry.getKey());
                                entry.getValue().complete(msg);
                            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause){
            log.error("Error in RpcClient ResponseHandler for peer {}", peerId, cause);
            pendingRequests.forEach((id, future) ->
                future.completeExceptionally(cause));
            pendingRequests.clear();
            ctx.close();
        }
    }
}
