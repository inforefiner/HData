package com.github.stuxuhai.hdata.plugin.dubbo.writer;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * Created by joey on 2017/7/7.
 */
public class Lz4Util {

    private static LZ4Factory factory = LZ4Factory.fastestInstance();

    public static byte[] compress(byte[] bytes, int length) {
        LZ4Compressor compressor = factory.fastCompressor();
        int maxCompressedLength = compressor.maxCompressedLength(length);
        byte[] compressed = new byte[maxCompressedLength];
        compressor.compress(bytes, 0, length, compressed, 0, maxCompressedLength);
        return compressed;
    }
}
