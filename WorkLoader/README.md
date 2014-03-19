workLoader
==========
- If resident-ratio is set, item-count won’t be considered.
- resident-ratio should be greater than 0.0 to be considered.
- set-thread-ratio of thread-count is the number of set threads, the remaining will be get threads.

=======================
Setting test.properties:

    - nodes...............ip of one node from cluster
    - port................couchbase port
    - buckets.............list of buckets [format: bucket1:pwd1,bkt2,bkt3:pwd3]
    - prefix..............prefix for keys generated
    - json................boolean value for json or binary
    - item-count..........no. of items to be created
    - item-size...........size of each item created
    - thread-count........no. of set and get threads in total
    - set-thread-ratio....ratio of no. of set thread to get threads
    - loop................boolean value - to loop the load infinitely or not
    - check-flag..........boolean value - store return value of a set to later verify the set success
