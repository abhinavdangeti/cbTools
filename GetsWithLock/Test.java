import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import net.spy.memcached.CASValue;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

public class Test {

    private static String _addr = "127.0.0.1";
    private static String _port = "9000";
    private static String _bkt = "default";
    private static String _pass = "";
    private static String _key = "Key";
    private static String _val = "{'name':'value'}";

    public static void main(String args[]) throws InterruptedException {
        CouchbaseClient client = connect();
        client.set(_key, _val);
        CASValue<Object> cake = client.getAndLock(_key, 15);
        if (cake != null) {
            System.out.println("---> _key:" + _key + " acquired with lock!");
            System.out.println("---> _cas:" + cake.toString());
            boolean ret = client.unlock(_key, cake.getCas());
            if (ret) {
                System.out.println("---> _key:" + _key + " unlocked successfully!");
            } else {
                System.out.println("---> Failed to unlock _key:" + _key + " !");
            }
        } else {
            System.out.println("---> Failed to acquire lock for _key: " + _key + " !");
        }
        client.shutdown();
        System.exit(0);

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
