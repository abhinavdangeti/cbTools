import java.io.File;
import java.io.FileInputStream;
import java.util.Enumeration;
import java.util.Properties;
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

    private static String _node = "127.0.0.1";
    private static int _port = 9000;
    private static String _bkt = "default";
    private static String _pass = "";
    private static String _key = "key_0";
    private static boolean _printVal = false;

    public static void main(String args[]) throws URISyntaxException, IOException, InterruptedException, ExecutionException {

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

        CouchbaseMetaClient client = connect(_node, _port);
        OperationFuture<MetaData> retm = client.getReturnMeta(_key);;

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("KEY: " + _key);
        if (retm.get() != null) {
            System.out.println("METADATA: " + retm.get().toString());
        } else {
            System.out.println("(returnMeta) --> - KEY NOT FOUND - ");
        }
        System.out.println("-----------------------------------------------------------------------");
        if (client.get(_key) != null) {
            String value = client.get(_key).toString();
            System.out.println("DATASIZE: " + value.length());
            if (_printVal) {
                System.out.println("DATA: " + value);
            }
            System.out.println("-----------------------------------------------------------------------");
        } else {
            System.out.println("(get) --> - GET RETURNED NULL - ");
            System.out.println("-----------------------------------------------------------------------");
        }

        client.shutdown();
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

    private static void parse_input(Properties properties) {
        /*
         * Read test variables from test.properties file
         */
        Enumeration<Object> enuKeys = properties.keys();
        while(enuKeys.hasMoreElements()){
            String key = (String) enuKeys.nextElement();
            if (key.equals("node"))
                _node = properties.getProperty(key);
            if (key.equals("port"))
                _port = (Integer.parseInt(properties.getProperty(key)));
            if (key.equals("bucket-name"))
                _bkt = properties.getProperty(key);
            if (key.equals("bucket-password"))
                _pass = properties.getProperty(key);
            if (key.equals("key"))
                _key = properties.getProperty(key);
            if (key.equals("doPrintValue"))
                _printVal = (Boolean.parseBoolean(properties.getProperty(key)));
        }
    }
}

