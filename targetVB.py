import sys
import zlib

NUM_VBUCKETS = 1024

def get_vbucket(key):
       return (((zlib.crc32(key)) >> 16) & 0x7fff) & (NUM_VBUCKETS - 1)

if (len(sys.argv) == 2):
    print get_vbucket(str(sys.argv[1]));
else:
    print "Usage: Enter the key as an argument!"

