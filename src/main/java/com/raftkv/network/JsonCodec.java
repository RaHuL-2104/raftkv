package com.raftkv.network;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.MessageToMessageCodec;

public final class JsonCodec extends MessageToMessageCodec<ByteBuf, RpcMessage>{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    protected void encode(ChannelHandlerContext ctx, RpcMessage msg, List<Object> out) throws Exception{
        byte[] json = MAPPER.writeValueAsBytes(msg);
        ByteBuf buf = ctx.alloc().buffer(json.length);
        buf.writeBytes(json);
        out.add(buf);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception{
        byte[] bytes = new byte[msg.readableBytes()];
        msg.readBytes(bytes);
        String json = new String(bytes, StandardCharsets.UTF_8);
        RpcMessage rpcMessage = MAPPER.readValue(json, RpcMessage.class);
        out.add(rpcMessage);
    }

    public static LengthFieldBasedFrameDecoder newFrameDecoder(){
        return new LengthFieldBasedFrameDecoder(10 * 1024 * 1024, 0, 4, 0, 4);
    }

    public static LengthFieldPrepender newFramePrepender(){
        return new LengthFieldPrepender(4);
    }
}
