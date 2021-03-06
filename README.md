cbTools
=======
(Tools that play with couchbase-server)

    - DataFetcher       :   Fetches {key, metadata, value} of specified key.
    - DataLoader        :   A multi-threaded read-write generator (creates and gets only).
    - GetsWithLock      :   Runs GetWithLock op on select items infinitely.
    - MemCounter        :   Runs a load and retrieves memory usage at different item counts.
    - MetaVerifier      :   Verifies metadata of keys between 2 clusters (setup through XDCR typically).
    - TouchExp          :   Tests TOUCH related bug, of updating a non-resident item's expiration time to the same value as before.
    - VbBlaster         :   Load generator for a specific vbuckets across buckets.

    - targetKeys.py     :   Program that prints a list of keys (count specifiable), for a specified vbucket.
    - targetVB.py       :   Program that tells the target vbucket for a specified key.
    - vbMemDiskcount.sh :   Program that detects mismatches in item count between memory and disk across all vbuckets (works for couchstore & forestDB storage engines).
