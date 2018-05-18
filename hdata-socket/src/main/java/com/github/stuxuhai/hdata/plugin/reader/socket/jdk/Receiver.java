package com.github.stuxuhai.hdata.plugin.reader.socket.jdk;

import com.github.stuxuhai.hdata.plugin.reader.socket.ReadListener;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;
import java.util.Set;

public class Receiver {

    private InetSocketAddress inetSocketAddress;
    private Handler handler;

    public Receiver(int port, ReadListener readListener) {
        inetSocketAddress = new InetSocketAddress("127.0.0.1", port);
        handler = new ReceiverHandler(readListener);
    }

    public void start() {
        try {
            Selector selector = Selector.open(); // 打开选择器
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open(); // 打开通道
            serverSocketChannel.configureBlocking(false); // 非阻塞
            serverSocketChannel.socket().bind(inetSocketAddress);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT); // 向通道注册选择器和对应事件标识
            while (true) { // 轮询
                int nKeys = selector.select();
                if (nKeys > 0) {
                    Set<SelectionKey> selectedKeys = selector.selectedKeys();
                    Iterator<SelectionKey> it = selectedKeys.iterator();
                    while (it.hasNext()) {
                        SelectionKey key = it.next();
                        if (key.isAcceptable()) {
                            handler.handleAccept(key);
                        } else if (key.isReadable()) {
                            handler.handleRead(key);
                        } else if (key.isWritable()) {
                            handler.handleWrite(key);
                        }
                        it.remove();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}