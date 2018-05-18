package lz4;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.commons.lang3.RandomUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by joey on 2017/7/7.
 */
public class Lz4Test {


    public static void main(String[] args) {

        List<Object[]> data = new ArrayList();
        for(int i = 0; i < 10000; i++){
            Object[] arr = new Object[30];
            for(int j = 0; j < 30; j ++){
                arr[j] =  "data - " + RandomUtils.nextInt(100000, 2000000);
            }
            data.add(arr);
        }

        long l = System.currentTimeMillis();
        byte[] bytes = new byte[0];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(bos);
            oos.writeObject(data);
            bytes = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        final int decompressedLength = bytes.length;
        System.out.println("cover to bytes use time : " + (System.currentTimeMillis() - l) + ", size: " + decompressedLength/1024 + "kb");



        l = System.currentTimeMillis();
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = factory.fastCompressor();
        int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(bytes, 0, decompressedLength, compressed, 0, maxCompressedLength);
        System.out.println("compress use time : " + (System.currentTimeMillis() - l) + ", size: " + compressedLength/1024 + "kb");

        l = System.currentTimeMillis();
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        byte[] restored = new byte[decompressedLength];
        decompressor.decompress(compressed, 0, restored, 0, decompressedLength);

        ByteArrayInputStream bais = new ByteArrayInputStream(restored);
        try {
            ObjectInputStream ois = new ObjectInputStream(bais);
            List<Object[]> list = (List<Object[]>)ois.readObject();
            for (Object[] arr: list){
//                System.out.println(ArrayUtils.toString(arr));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("decompress use time : " + (System.currentTimeMillis() - l));
    }
}
