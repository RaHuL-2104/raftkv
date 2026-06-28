package com.raftkv.network;

import io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public final class RpcServer {
    private static final Logger log = LoggerFactory.getLogger(RpcServer.class);
    private final int port;
    private final RpcMessageHandler handler;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public RpcServer(int port, RpcMessageHandler handler){
        this.port = port;
        this.handler = handler;
    }

    public void start() throws InterruptedException{
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch){
                        ch.pipeline().addLast(JsonCodec.newFrameDecoder());
                        ch.pipeline().addLast(JsonCodec.newFramePrepender());
                        ch.pipeline().addLast(new JsonCodec());
                        ch.pipeline().addLast(new RpcServerHandler(handler));
                    }
        });
        serverChannel = bootstrap.bind(port).sync().channel();
        log.info("RpcServer listening on port {}", port);
    }

    public void shutdown(){
        if(serverChannel != null){
            serverChannel.close();
        }
        if(bossGroup != null){
            bossGroup.shutdownGracefully();
        }
        if(workerGroup != null){
            workerGroup.shutdownGracefully();
        }
        log.info("RpcServer on port {} shut down", port);
    }
}
