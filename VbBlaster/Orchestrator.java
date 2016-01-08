import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

import src.*;

public class Orchestrator {

    private static String _Node = "";
    private static String _Port = "";
    private static String[] _buckets = {};
    private static String[] _targetVBs = {};
    private static boolean _repeat = false;
    private static String _prefix = "";
    private static boolean _json = false;
    private static int _itemCount = 0;
    private static int _itemSize = 0;
    private static boolean _checkFlag = false;

    public static void main(String args[]) throws InterruptedException {

        try {
            File file = new File("test.properties");
            FileInputStream fileInput = new FileInputStream(file);
            Properties properties = new Properties();
            properties.load(fileInput);
            fileInput.close();

            parse_input(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Thread[][] _control = new Thread[_buckets.length][_targetVBs.length];
        for (int i = 0; i < _buckets.length; i++) {
            if (_buckets[i].isEmpty()) {
                continue;
            }

            final String bucketname = _buckets[i].split(":")[0];
            String bucketpasswd = "";
            if (_buckets[i].contains(":")) {
                bucketpasswd = _buckets[i].split(":")[1];
            }

            final CouchbaseClient client = connect(_Node, bucketname, bucketpasswd);

            for (int j = 0; j < _targetVBs.length; j++) {
                if (_targetVBs[j].isEmpty()) {
                    continue;
                }

                final int vb = Integer.parseInt(_targetVBs[j]);
                Runnable _control_ = new Runnable() {
                    public void run() {
                        try {
                            Blaster.runThemAll(client, bucketname, vb, _repeat, _prefix,
                                               _json, _itemCount, _itemSize, _checkFlag);
                        } catch (Exception e) {
                            // e.printStackTrace();
                        }
                    }
                };
                _control[i][j] = new Thread(_control_);
                _control[i][j].start();
            }
        }

        for (int i = 0; i < _buckets.length; i++) {
            if (!_buckets[i].isEmpty()) {
                for (int j = 0; j < _targetVBs.length; j++) {
                    if (!_targetVBs[j].isEmpty()) {
                        _control[i][j].join();
                    }
                }
            }
        }

        TimeUnit.SECONDS.sleep(2);
        System.out.println(" ........... done ...........");
        System.exit(0);
    }

    private static CouchbaseClient connect(String _addr, String _bucketName, String _bucketPasswd) {
        /*
         * CouchbaseClient Connection to bucket
         */
        List<URI> uris = new LinkedList<URI>();
        uris.add(URI.create(String.format("http://" + _addr + ":" + _Port + "/pools")));
        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        try {
            return new CouchbaseClient(cfb.buildCouchbaseConnection(uris, _bucketName, _bucketPasswd));
        } catch (Exception e) {
            System.err.println("Error connecting to Couchbase: "
                    + e.getMessage());
            System.exit(0);
        }
        return null;
    }

    private static void parse_input(Properties properties) {
        /*
         * Read test variables from test.properties file
         */
        Enumeration<Object> enuKeys = properties.keys();
        while(enuKeys.hasMoreElements()){
            String key = (String) enuKeys.nextElement();
            if (key.equals("node"))
                _Node = properties.getProperty(key);
            if (key.equals("port"))
                _Port = properties.getProperty(key);
            if (key.equals("buckets"))
                _buckets = properties.getProperty(key).split(",");

            if (key.equals("target-vbs"))
                _targetVBs = properties.getProperty(key).split(",");
            if (key.equals("repeat"))
                _repeat = Boolean.parseBoolean(properties.getProperty(key));

            if (key.equals("prefix"))
                _prefix = properties.getProperty(key);
            if (key.equals("json"))
                _json = Boolean.parseBoolean(properties.getProperty(key));
            if (key.equals("item-count"))
                _itemCount = Integer.parseInt(properties.getProperty(key));
            if (key.equals("item-size"))
                _itemSize = Integer.parseInt(properties.getProperty(key));
            if (key.equals("check-flag"))
                _checkFlag = Boolean.parseBoolean(properties.getProperty(key));

        }
    }
}
