package com.github.stuxuhai.hdata.plugin.reader.socket.netty;

import com.github.stuxuhai.hdata.plugin.reader.socket.ReadListener;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;

public class Receiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    private String ipAddr;
    private int port;
    private String charset;
    private ReadListener readListener;

    public Receiver(String ipAddr, int port, String charset, ReadListener readListener) {
        this.ipAddr = ipAddr;
        this.port = port;
        this.charset = charset;
        this.readListener = readListener;
    }

    public void start() {
        LOGGER.info("starting netty socket server... bind {}, port {}", ipAddr, port);
        EventLoopGroup boosGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(boosGroup, workerGroup);
            b.channel(NioServerSocketChannel.class).option(ChannelOption.SO_BACKLOG, 10)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline().addLast(new LineBasedFrameDecoder(1024 * 1024));
                            socketChannel.pipeline().addLast(new StringDecoder(Charset.forName(charset)));
                            socketChannel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                    readListener.callback((String) msg);
                                }
                            });
                        }
                    });
            ChannelFuture f = b.bind(new InetSocketAddress(ipAddr, port)).sync();
            f.channel().closeFuture().sync();
        } catch (Exception e) {
            LOGGER.error("netty socket server start failed, cause by: {}", e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        } finally {
            boosGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
