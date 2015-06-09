package src;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;


public class Controller {

    public static int _NUM_VBUCKETS = 1024;
    private static AtomicInteger _keyCounter = new AtomicInteger(0);
    private static Queue<String> _keyQueue = new ConcurrentLinkedQueue<String>();

    public static int addToKeyQueue(String key) {
        // Enqueue
        _keyQueue.add(key);
        return _keyCounter.incrementAndGet();
    }

    public static String getFromKeyQueue(boolean doRepeat) {
        if (_keyQueue.isEmpty()) {
            return null;
        }
        // Dequeue
        if (doRepeat) {
            String key = _keyQueue.remove();
            _keyQueue.add(key);
            return key;
        } else {
            return _keyQueue.remove();
        }
    }

    public static boolean shouldWaitForProducer(int finalCount) {
        if (_keyQueue.isEmpty()) {
            if (_keyCounter.get() >= finalCount) {
                // Producer has finished its work on the queue
                return false;
            }
        }
        // Wait in every other case
        return true;
    }

}
