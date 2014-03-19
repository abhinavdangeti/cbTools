import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.CouchbaseMetaClient;
import com.couchbase.client.MetaData;


public class Orchestrator {

    private static String _nodeIP = "127.0.0.1";
    private static int _port = 9000;
    private static String _bkt = "default";
    private static String _pass = "";
    private static String _prefix = "key_";
    private static int _item_size = 256;
    private static int _exp_time = 600;

    public static void main(String args[]) throws UnknownHostException, InterruptedException, ExecutionException {

        CouchbaseMetaClient client = connect();
        StringBuffer value = new StringBuffer();
        String CHAR_LIST = "ABCD";
        while (value.length() < _item_size) {
            value.append(CHAR_LIST);
        }

        int i = 0;
        int batch = 10000;
        String key = String.format("%s%d", _prefix, i);
        client.set(key, _exp_time, value.toString());
        while (_retrieve_val("vb_active_perc_mem_resident", client) > 5) {
            i++;
            while (i % batch != 0) {
                key = String.format("%s%d", _prefix, i);
                client.set(key, _exp_time, value.toString());
                i++;
            }
        }

        System.out.println("\n:: RESIDENT RATIO : "
                + _retrieve_val("vb_active_perc_mem_resident", client)
                + "% : ITEM_COUNT : " + i + " approximately:\n");
        System.out.println("Hit ENTER, to run CMD_TOUCH on non-resident items"
                + ", with same expiration time as before.\n");
        Scanner scan = new Scanner(System.in);
        scan.nextLine();

        System.out.println("Running touch command on keys starting with "
                + _prefix + "0 uptil " + _prefix + batch + "...");
        OperationFuture<MetaData> retm = null;
        for (i = 0; i < batch; i++) {
            key = String.format("%s%d", _prefix, i);
            retm = client.getReturnMeta(key);
            client.touch(key, (int) (retm.get().getExpiration() - (System.currentTimeMillis() / 1000L)));
        }

        System.out.println("\nDONE\n");
        System.exit(0);

    }

    private static CouchbaseMetaClient connect() {
        /*
         * CouchbaseMetaClient Connection to bucket
         */

        List<URI> uris = new LinkedList<URI>();
        uris.add(URI.create(String.format("http://" + _nodeIP + ":" + Integer.toString(_port) + "/pools")));
        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        try {
            return new CouchbaseMetaClient(cfb.buildCouchbaseConnection(uris, _bkt, _pass));
        } catch (Exception e) {
            System.err.println("Error connecting to Couchbase: " + e.getMessage());
            System.exit(0);
        }
        return null;

    }

    private static float _retrieve_val(String parameter, CouchbaseMetaClient client)
        throws UnknownHostException {
        Map<SocketAddress, Map<String, String>> map = client.getStats();
        Iterator<SocketAddress> iterator = map.keySet().iterator();
        Object key = iterator.next();

        Map<String, String> map1 = map.get(key);
        Iterator<String> tt = map1.keySet().iterator();
        //System.out.println(key.toString());
        float resident_ratio = 100;
        while (tt.hasNext()) {
            String val1 = tt.next();
            if ((val1.toString().equals(parameter))) {
                String val2 = map1.get(val1);
                //System.out.println(val1.toString() + "  " + val2.toString());
                resident_ratio = Float.parseFloat(val2.toString());
            }
        }

        //throw new RuntimeException("Unable to find the stat '" + parameter + "'!\n");
        return resident_ratio;

    }

}
