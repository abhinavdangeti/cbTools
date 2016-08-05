#!/bin/bash

# This script detects mismatches in the number of items in memory
# and disk for a couchbase bucket with couchstore or forestdb as
# storage engines.

"""
MUST DOs:
    - Set BIN_DIR, DATA_DIR, BUCKET, IP, PORT for correct results.
    - Select the right underlying store for the couchbase bucket.
      (If forestDB is chosen, set SHARD_COUNT appropriately)
    - Optionally, set writeOutputToFile to true if you want in-mem
      and disk results to be written to a file.
"""

verbose=false
if [ $# -eq 1 ] ; then
    if [ "$1" = '-v' ] ; then
        verbose=true
    fi
fi

BIN_DIR="/Users/abhinavdangeti/Documents/couchbase/install/bin"
DATA_DIR="/Users/abhinavdangeti/Documents/couchbase/ns_server/data/n_0/data"
BUCKET="default"

IP='127.0.0.1'
PORT='12000'

NUM_VBUCKETS=1024
STORE='forestdb'
SHARD_COUNT=4

if [ ! -d "$BIN_DIR" ] ; then
    echo 'Dir:'$BIN_DIR', DOES NOT EXIST!'
    exit
fi

if [ ! -d "$DATA_DIR" ] ; then
    echo 'Dir:'$DATA_DIR', DOES NOT EXIST!'
    exit
fi

export DYLD_LIBRARY_PATH=$BIN_DIR/../lib

writeOutputToFile=false

inMemCountFile="./temp_inMem.txt"
diskCountFile="./temp_inDisk.txt"

if [ "$writeOutputToFile" = true ] ; then
    rm $inMemCountFile
    rm $diskCountFile
fi

count=0
total_mem_count=0
total_disk_count=0
for (( i=0; i<$NUM_VBUCKETS; i++ ))
do
    x=`$BIN_DIR/cbstats $IP:$PORT vbucket-details $i | grep 'num_items'`
    if [ "$writeOutputToFile" = true ] ; then
        printf '%s ' $x >> $inMemCountFile
        printf '\n' >> $inMemCountFile
    fi
    mem_count=${x##* }
    let "total_mem_count += mem_count"
    if [ $STORE = 'couchstore' ]; then
        y=`$BIN_DIR/couch_dbinfo $DATA_DIR/$BUCKET/$i.couch.* | grep '  doc count'`
    elif [ $STORE = 'forestdb' ]; then
        y=`$BIN_DIR/forestdb_dump $DATA_DIR/$BUCKET/$(($i % $SHARD_COUNT)).fdb.* --kvs partition$i --no-body | grep 'Doc ID: ' | wc -l`
    else
        echo 'Unknown store: '$STORE', !'
        exit
    fi
    if [ "$writeOutputToFile" = true ] ; then
        z='vb: '$i' '$y
        printf '%s ' $z  >> $diskCountFile
        printf '\n' >> $diskCountFile
    fi
    disk_count=${y##* }
    let "total_disk_count += disk_count"

    if [ -n "$mem_count" ] && [ -n "$disk_count" ] ; then
        if [ "$mem_count" != "$disk_count" ] ; then
            echo 'Mismatch:: vb: '$i' => in-memory: '$mem_count'; in-disk: '$disk_count
            let "count++"
        elif [ "$verbose" = true ] ; then
            echo 'No mismatch on vb: '$i
        fi
    fi
done

echo '======================================================'
echo 'Mismatches detected in: '$count' vbuckets!'
echo '======================================================'
echo 'Mem_item_count: '$total_mem_count'; Disk_item_count: '$total_disk_count
echo '======================================================'
