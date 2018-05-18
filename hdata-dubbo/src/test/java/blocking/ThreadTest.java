package blocking;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by joey on 2017/7/7.
 */
public class ThreadTest {


    public static void main(String[] args) {

//        final BatchBlockingQueue<Integer> batchBlockingQueue = new BatchBlockingQueue(10);
        final BlockingQueue queue = new ArrayBlockingQueue(10, true);


        Thread writer = new Thread(new Runnable() {
            @Override
            public void run() {
                for(int i = 0; i < 100; i++){
                    try {
                        queue.put(i);
//                        batchBlockingQueue.put(i);
                        System.out.println("put " + i);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        Thread reader = new Thread(new Runnable() {
            @Override
            public void run() {
                while(true){
                    try {
                        if(queue.remainingCapacity() == 0){
                            List list = new ArrayList();
                            queue.drainTo(list);
                            System.out.println("take = " + list);
                            Thread.sleep(1000 * 5);
                        }
//                        int i = batchBlockingQueue.poll();
//                        System.out.println(i);
//                        Thread.sleep(1000);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        writer.start();
//        reader.start();

        try {
            Thread.sleep(1000 * 30);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        System.out.println("over");
//        try {
//            writer.join();
//            reader.join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }
}
