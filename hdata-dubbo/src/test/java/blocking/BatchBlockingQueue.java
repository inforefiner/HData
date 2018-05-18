package blocking;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;

public class BatchBlockingQueue<T> {

    private ArrayList<T> queue;
    private Semaphore readerLock;
    private Semaphore writerLock;
    private int batchSize;

    public BatchBlockingQueue(int batchSize) {
        this.queue = new ArrayList<>(batchSize);
        this.readerLock = new Semaphore(0);
        this.writerLock = new Semaphore(batchSize);
        this.batchSize = batchSize;
    }

    public void put(T e) throws InterruptedException {
        writerLock.acquire();
        queue.add(e);
        if (queue.size() == batchSize) {
            readerLock.release(batchSize);
        }
    }

    public T poll() throws InterruptedException {
        readerLock.acquire();
        T ret = queue.remove(0);
        if (queue.isEmpty()) {
            writerLock.release(batchSize);
        }
        return ret;
    }

}