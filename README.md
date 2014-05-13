bugBusters: Tools to track specific bugs in Couchbase, and verify fixes.

- DataFetcher   :   Fetches {key, metadata, value} of specified key.
- GetsWithLock  :   Runs GetWithLock op on select items infinitely.
- MetaVerifier  :   Verifies metadata of keys between 2 clusters (setup through XDCR typically).
- TouchExp      :   Tests TOUCH related bug, of updating a non-resident item's expiration time
                    to the same value as before.
- WorkLoader    :   A multi-threaded read-write generator (creates and gets only).
