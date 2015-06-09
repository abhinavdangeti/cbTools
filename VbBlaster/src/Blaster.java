package src;

import com.couchbase.client.CouchbaseClient;


public class Blaster {

    public static void runThemAll(final CouchbaseClient client, final int _targetVB,
            final boolean _repeat, final String _prefix,
            final boolean _json, final int _itemCount,
            final int _itemSize, final boolean _checkFlag)
        throws InterruptedException {


        Runnable _producer = new Runnable() {
            public void run() {
                try {
                    System.out.println("Spawning producer ...");
                    Producer.produce(_targetVB, _prefix, _itemCount);
                } catch (Exception e) {
                    // e.printStackTrace();
                }
            }
        };

        Runnable _consumer = new Runnable() {
            public void run() {
                try {
                    System.out.println("Spawning consumer ...");
                    Consumer.consume(client, _repeat, _json,
                            _itemCount, _itemSize, _checkFlag);
                } catch (Exception e) {
                    // e.printStackTrace();
                }
            }
        };

        Thread _producer_ = new Thread(_producer);
        Thread _consumer_ = new Thread(_consumer);

        _producer_.start();
        _consumer_.start();
        _producer_.join();
        _consumer_.join();

        // Disconnecting client
        client.shutdown();
    }

}
