package com.github.stuxuhai.hdata.plugin.reader.socket.jdk;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public interface Handler {

    void handleAccept(SelectionKey key) throws IOException;

    void handleRead(SelectionKey key) throws IOException;

    void handleWrite(SelectionKey key) throws IOException;
}