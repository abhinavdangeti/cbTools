import sys
import zlib

NUM_VBUCKETS = 1024

def get_vbucket(key):
    return (((zlib.crc32(key)) >> 16) & 0x7fff) & (NUM_VBUCKETS - 1)

def getList(count, vbucket):
    i = 0;
    my_list = []
    while len(my_list) < count:
        key = "key" + str(i)
        if (get_vbucket(key) == vbucket):
            my_list.append(key)
        i += 1
    return my_list

if (len(sys.argv) == 3):
    count = sys.argv[1]
    vbucket = sys.argv[2]
    print "Count: " + count + "; vb: " + vbucket
    list = getList(int(count), int(vbucket))
    print list
else:
    print "Usage: python targetKeys.py <no_of_items> <vbucket>"

