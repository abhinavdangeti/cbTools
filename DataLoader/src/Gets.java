package src;

import com.couchbase.client.CouchbaseClient;

public class Gets {

    public static void get_items(CouchbaseClient client, boolean _isJson, String _prefix,
            double _resRatio, int start, int end) {

        String key;
        if (_resRatio <= 0) {
            int count = end - start;
            int cnt = 0;
            while (count > cnt) {
                for (int i = start; i < end; i++) {
                    key = String.format("%s%d", _prefix, i);
                    Object item = null;
                    try {
                        item = client.get(key);
                    } catch (Exception e) {
                        continue;
                    }
                    if (item != null) {
                        cnt++;
                    }
                }
            }
        } else {
            int i = start;
            while (true) {
                key = String.format("%s%d", _prefix, i);
                try {
                    client.get(key);
                } catch (Exception e) {
                    i = start;
                }
                i++;
            }

        }

    }
}
