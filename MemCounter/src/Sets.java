package src;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.codehaus.jettison.json.JSONException;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;

/*
 * Module that runs set operations, and sets with expires
 */

public class Sets {
    public static void set_items (CouchbaseClient client, String _prefix,
                                  boolean isJson, int itemSize)
                                  throws JSONException, InterruptedException {

        Random gen = new Random(987654321);
        List<OperationFuture<Boolean>> sets = new LinkedList<OperationFuture<Boolean>>();
        int i = 0;
        while (true) {
            OperationFuture<Boolean> setOp;
            String key = String.format("%s%d", _prefix, i);
            if (isJson) {
                setOp = client.set(key, Gen.retrieveJSON(gen, itemSize).toString());
            } else {
                setOp = client.set(key, Gen.retrieveBinary(itemSize));
            }
            i++;
        }
    }
}
