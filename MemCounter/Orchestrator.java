import src.*;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;


public class Orchestrator {

    private static String _Node = "default";    //Servers, Format: server1_ip,server2_ip..
    private static String _Port = "";           //Couchbase port
    private static String _prefix = "";         //Key prefixes
    private static boolean _json = false;
    private static String[] _itemChkPts = {};
    private static int _itemSize = 0;

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

        final CouchbaseClient client1 = connect();
        final CouchbaseClient client2 = connect();

        Runnable _sets_ = new Runnable() {
            public void run() {
                try {
                    Sets.set_items(client1, _prefix, _json, _itemSize);
                } catch (Exception e) {
                    // e.printStackTrace();
                }
            }
        };

        Runnable _track_ = new Runnable() {
            public void run() {
                try {
                    Tracker.track_items(client2, _itemChkPts);
                } catch (Exception e) {
                    // e.printStackTrace();
                }
            }
        };

        Thread _sets = new Thread(_sets_);
        Thread _track = new Thread(_track_);

        _track.start();
        _sets.start();
        _track.join();
        _sets.interrupt();

        client1.shutdown();
        client2.shutdown();
        System.out.println(" ........... done ...........");
        System.exit(0);

    }

    private static CouchbaseClient connect() {
        /*
         * CouchbaseClient Connection to bucket
         */
        List<URI> uris = new LinkedList<URI>();
        uris.add(URI.create(String.format("http://" + _Node + ":" + _Port + "/pools")));
        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        try {
            return new CouchbaseClient(cfb.buildCouchbaseConnection(uris, "default", ""));
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
            if (key.equals("prefix"))
                _prefix = properties.getProperty(key);

            if (key.equals("json"))
                _json = (Boolean.parseBoolean(properties.getProperty(key)));
            if (key.equals("item-counts"))
                _itemChkPts = properties.getProperty(key).split(",");
            if (key.equals("item-size"))
                _itemSize = (Integer.parseInt(properties.getProperty(key)));
        }
    }
}
