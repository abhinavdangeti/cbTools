package src;

import java.net.UnknownHostException;

import org.codehaus.jettison.json.JSONException;

import com.couchbase.client.CouchbaseClient;

public class TaskRouter {

    public static void runClientOps(final CouchbaseClient client, final String _prefix,
                                    final boolean _json, int _itemCount, final int _itemSize,
                                    int _threadCount, double _setThreadRatio, final double _resRatio,
                                    final boolean _checkFlag) throws InterruptedException {

        class SetRunnable implements Runnable {
            int start, end;
            SetRunnable(int st, int ed) { start = st; end = ed; }
            public void run() {
                try {
                    Sets.set_items(client, _json, _prefix, _resRatio, _itemSize, _checkFlag,
                            start, end);
                } catch (JSONException e) {
                    //e.printStackTrace();
                } catch (UnknownHostException e) {
                    //e.printStackTrace();
                }
            }
        }

        class GetRunnable implements Runnable {
            int start, end;
            GetRunnable(int st, int ed) { start = st; end = ed; }
            public void run() {
                Gets.get_items(client, _json, _prefix, _resRatio, start, end);
            }
        }

        int get_thread_count = (int) (_threadCount * (1 - _setThreadRatio));
        int set_thread_count = _threadCount - get_thread_count;

        Thread[] _getLaunch = new Thread[get_thread_count];
        Thread[] _setLaunch = new Thread[set_thread_count];

        double ratio = (double) _itemCount / set_thread_count;

        if (_resRatio <= 0 && ratio == 0) {
            System.out.println("Item count at 0 and resident ratio not set either, no workload!");
            return;
        }

        int i,j;

        for (i = 0; i < set_thread_count; i++) {
            //System.out.println("Launching sets' thread " + i);
            if (_resRatio <= 0) {
                _setLaunch[i] = new Thread(new SetRunnable((int)(i * ratio), (int)((i + 1) * ratio)));
            } else {
                _setLaunch[i] = new Thread(new SetRunnable((i * 100000), ((i + 1) * 100000)));
            }
            _setLaunch[i].start();
        }

        ratio = (double) _itemCount / get_thread_count;

        for (j = 0; j < get_thread_count; j++) {
            //System.out.println("Launching gets' thread " + j);
            if (_resRatio <= 0.0) {
                _getLaunch[j] = new Thread(new GetRunnable((int)(j * ratio), (int)((j + 1) * ratio)));
            } else {
                _getLaunch[j] = new Thread(new GetRunnable((j * 100000), ((j + 1) * 100000)));
            }
            _getLaunch[j].start();
        }

        for (i = 0; i < set_thread_count; i++) {
            _setLaunch[i].join();
        }
        for (j = 0; j < get_thread_count; j++) {
            if (_resRatio <= 0) {
                _getLaunch[j].join();
            } else {
                _getLaunch[j].interrupt();
            }
        }

        Thread.sleep(1000);
        client.shutdown();
    }

}
