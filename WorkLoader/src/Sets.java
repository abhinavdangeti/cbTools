package src;

import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.codehaus.jettison.json.JSONException;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;

public class Sets {

    public static void set_items(CouchbaseClient client, CouchbaseClient tClient,
                                 boolean _isJson, String _prefix,
            double _resRatio, int _itemSize, boolean _checkFlag,
            int start, int end) throws JSONException, UnknownHostException {

        Random gen = new Random(987654321);
        List<OperationFuture<Boolean>> sets = new LinkedList<OperationFuture<Boolean>>();
        OperationFuture<Boolean> setOp;
        String key;
        if (_resRatio <= 0) {
            /*
             * If resident ratio not set and itemCount entered
             */
            for (int i = start; i < end; i++) {
                key = String.format("%s%d", _prefix, i);
                if (_isJson) {
                    setOp = client.set(key, Gen.retrieveJSON(gen, _itemSize).toString());
                } else {
                    setOp = client.set(key, Gen.retrieveBinary(_itemSize));
                }
                if (_checkFlag) {
                    sets.add(setOp);
                }
            }
        } else {
            /*
             * If desired resident ratio entered
             */
            int j = start;
            int batch = 10000;
            key = String.format("%s%d", _prefix, j);
            if (_isJson) {
                setOp = client.set(key, Gen.retrieveJSON(gen, _itemSize).toString());
            } else {
                setOp = client.set(key, Gen.retrieveBinary(_itemSize));
            }
            if (_checkFlag) {
                sets.add(setOp);
            }
            while (_retrieve_val("vb_active_perc_mem_resident", tClient) > _resRatio) {
                j++;
                while (j % batch != 0) {
                    key = String.format("%s%d", _prefix, j);
                    if (_isJson) {
                        setOp = client.set(key, Gen.retrieveJSON(gen, _itemSize).toString());
                    } else {
                        setOp = client.set(key, Gen.retrieveBinary(_itemSize));
                    }
                    j++;
                    if (_checkFlag) {
                        sets.add(setOp);
                    }
                }
            }
        }

        while (!sets.isEmpty()) {
            try {
                if (sets.get(0).get().booleanValue() == false) {
                    // TODO: Something I'd guess, as set failed
                }
            } catch (Exception e) {
                // e.printStackTrace();
            }
            sets.remove(0);
        }
    }

    private static double _retrieve_val(String parameter, CouchbaseClient client)
        throws UnknownHostException {
        Map<SocketAddress, Map<String, String>> map = client.getStats();
        Iterator<SocketAddress> iterator = map.keySet().iterator();
        Object key = iterator.next();

        Map<String, String> map1 = map.get(key);
        Iterator<String> tt = map1.keySet().iterator();
        double resident_ratio = 100.0;
        while (tt.hasNext()) {
            String val1 = tt.next();
            if ((val1.toString().equals(parameter))) {
                String val2 = map1.get(val1);
                //System.out.println(val1.toString() + "  " + val2.toString());
                resident_ratio = Double.parseDouble(val2.toString());
            }
        }
        return resident_ratio;
    }
}
