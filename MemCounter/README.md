MemCounter
==========

Runs a set load with one thread.
Fetches memory usage and resident ratio with the other thread at specified checkpoints.

Setting test.properties:

    - node................ip of one node from cluster
    - port................couchbase port
    - prefix..............prefix for keys generated
    - json................is json?
    - item-counts.........item count checkpoints [format: 1000,10000,100000]
    - item-max............max count
    - item-size...........size of each item created
