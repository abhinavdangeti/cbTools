package src;

import java.util.zip.CRC32;


public class Producer extends Controller {

    public static int getVbucket(String key) {
        CRC32 myCRC = new CRC32();
        myCRC.update(key.getBytes());
        return (int)(((myCRC.getValue() >> 16) & 0x7fff) & (_NUM_VBUCKETS - 1));
    }

    public static void produce(int _targetVB, String _prefix, int _itemCount) {

        int counter = _itemCount;
        int i = 0, x = 0;
        while (counter != 0) {
            String key = String.format("%s%d", _prefix, i);
            if (getVbucket(key) == _targetVB) {
                x = addToKeyQueue(key);
                --counter;
            }
            ++i;
        }
        if (x != _itemCount) {
            System.out.println("WARNING: Expected items count: " + _itemCount +
                    ", but produced item count: " + x);
        }
    }

}
