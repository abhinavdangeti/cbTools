import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.CouchbaseMetaClient;
import com.couchbase.client.MetaData;

public class Fetcher {

    private static String _node1 = "127.0.0.1";
    private static int _port1 = 9000;
    private static String _bkt = "default";
    private static String _pass = "";
    private static String _key = "key_0";
    private static boolean printVal = false;

    public static void main(String args[]) throws URISyntaxException, IOException, InterruptedException, ExecutionException {
        CouchbaseMetaClient client = connect(_node1, _port1);
        OperationFuture<MetaData> retm = client.getReturnMeta(_key);;
        String value = client.get(_key).toString();

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("KEY: " + _key + "\nMETADATA: " + retm.get().toString() + "\nDATASIZE: " + value.length());
        System.out.println("-----------------------------------------------------------------------");
        if (printVal) {
            System.out.println("DATA: " + value);
            System.out.println("-----------------------------------------------------------------------");
        }

        System.exit(0);

    }

    private static final CouchbaseMetaClient connect(String _serverAddr, int port) throws URISyntaxException, IOException{
        List<URI> uris = new LinkedList<URI>();
        uris.add(URI.create(String.format("http://" + _serverAddr + ":" + Integer.toString(port) + "/pools")));
        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        try {
            return new CouchbaseMetaClient(cfb.buildCouchbaseConnection(uris, _bkt, _pass));
        } catch (Exception e) {
            System.err.println("Error connecting to Couchbase: " + e.getMessage());
            System.exit(0);
        }
        return null;
    }
}

