import zlib

NUM_VBUCKETS = 1024

def get_vbucket(key):
       return (((zlib.crc32(key)) >> 16) & 0x7fff) & (NUM_VBUCKETS - 1)

print get_vbucket("key_0");
