package src;

import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;

public class Tracker {

    public static void track_items(CouchbaseClient client, String[] item_chkpts)
            throws UnknownHostException, InterruptedException {

        for (int i = 0; i < item_chkpts.length; i++) {
            long checkpoint = Long.parseLong(item_chkpts[i]);
            long curr = Long.parseLong(_retrieve_val("vb_active_curr_items", client));
            while ((curr == 0) || curr < checkpoint) {
                Thread.sleep(1000);
                curr = Long.parseLong(_retrieve_val("vb_active_curr_items", client));
            }
            long usage = Long.parseLong(_retrieve_val("mem_used", client));
            while (usage == 0) {
                usage = Long.parseLong(_retrieve_val("mem_used", client));
            }
            double residentRatio =
                Double.parseDouble(_retrieve_val("vb_active_perc_mem_resident", client));
            while (residentRatio == 0) {
                residentRatio =
                    Double.parseDouble(_retrieve_val("vb_active_perc_mem_resident", client));
            }
            System.out.println("Current items: " + curr + " :: Memory usage: " + usage
                                + " :: Resident ratio: " + residentRatio);
        }
    }

    private static String _retrieve_val(String parameter, CouchbaseClient client)
        throws UnknownHostException {
        Map<SocketAddress, Map<String, String>> map = client.getStats();
        Iterator<SocketAddress> iterator = map.keySet().iterator();
        Object key = iterator.next();

        Map<String, String> map1 = map.get(key);
        Iterator<String> tt = map1.keySet().iterator();
        String val = "0";
        while (tt.hasNext()) {
            String val1 = tt.next();
            if ((val1.toString().equals(parameter))) {
                String val2 = map1.get(val1);
                val = val2.toString();
            }
        }
        return val;
    }
}
