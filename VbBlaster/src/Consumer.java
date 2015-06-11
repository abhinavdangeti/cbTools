package src;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.codehaus.jettison.json.JSONException;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;


public class Consumer extends Controller {

    public static void consume(CouchbaseClient client, boolean _repeat, boolean _json,
            int _itemCount, int _itemSize, boolean _checkFlag)
        throws InterruptedException, JSONException, ExecutionException {

        Random gen = new Random(987654321);
        List<OperationFuture<Boolean>> sets = new LinkedList<OperationFuture<Boolean>>();
        int counter = _itemCount;
        while (counter != 0) {
            OperationFuture<Boolean> setOp;
            String key = getFromKeyQueue(_repeat);
            if (key != null) {
                if (_json) {
                    setOp = client.set(key, Gen.retrieveJSON(gen, _itemSize).toString());
                } else {
                    setOp = client.set(key, Gen.retrieveBinary(_itemSize));
                }
                assert setOp.isDone();
                if (_checkFlag) {
                    sets.add(setOp);
                }
                if (!_repeat) {
                    --counter;
                }
            } else if (shouldWaitForProducer(_itemCount)) {
                Thread.sleep(1000);
            } else {
                break;
            }
        }

        if (_checkFlag) {
            int count = 0;
            while (!sets.isEmpty()) {
                try {
                    if (sets.get(0).get().booleanValue() == false) {
                        count++;
                        // TODO: Set failed
                    }
                } catch (Exception e) {
                    // e.printStackTrace();
                }
                sets.remove(0);
            }
            System.out.println("Missed " + count + " SETs.");
        }
    }

}
