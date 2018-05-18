package com.github.stuxuhai.hdata.plugin.reader.socket.jdk;

import com.github.stuxuhai.hdata.plugin.reader.socket.ReadListener;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

class ReceiverHandler implements Handler {

    public ReadListener readListener;

    public ReceiverHandler(ReadListener readListener) {
        this.readListener = readListener;
    }

    @Override
    public void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.register(key.selector(), SelectionKey.OP_READ);
    }

    @Override
    public void handleRead(SelectionKey key) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 256); // 256kb
        SocketChannel socketChannel = (SocketChannel) key.channel();
        while (true) {
            int readBytes = socketChannel.read(byteBuffer);
            if (readBytes > 0) {
                readListener.callback(new String(byteBuffer.array(), 0, readBytes));
                break;
            }
        }
        socketChannel.close();
    }

    @Override
    public void handleWrite(SelectionKey key) throws IOException {

    }
}