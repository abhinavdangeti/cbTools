import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

public class Helper {

    private static String _addr = "127.0.0.1";
    private static String _port = "9000";
    private static String _bkt = "loadtest";
    private static String _pass = "";
    private static int _count = 5;
    private static String _prefix = "key+";

    public static void main(String args[]) throws InterruptedException {
        CouchbaseClient client = connect();
        int i;
        for(i = 0; i < _count; i++) {
            client.set(_prefix + i, "ABCD");
        }

        Thread.sleep(5000);

        i = 0;
        while (true) {
            i = i % _count;
            Object cake = client.getAndLock(_prefix + i, 1);
            if (cake == null) {
                System.out.println("Failed to retrieve: " + (_prefix + i));
            } else {
                System.out.println((_prefix + i) + " : " + cake.toString());
            }
            i++;
            Thread.sleep(2000);
        }

    }

    private static CouchbaseClient connect() {
        /*
         * CouchbaseClient Connection to bucket
         */
        List<URI> uris = new LinkedList<URI>();
        uris.add(URI.create(String.format("http://" + _addr + ":" + _port + "/pools")));
        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        try {
            return new CouchbaseClient(cfb.buildCouchbaseConnection(uris, _bkt, _pass));
        } catch (Exception e) {
            System.err.println("Error connecting to Couchbase: "
                    + e.getMessage());
            System.exit(0);
        }
        return null;
    }
}
