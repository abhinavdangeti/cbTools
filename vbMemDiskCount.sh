#!/bin/bash

#This script detects mismatches in the number of items in memory
#and disk for a couchbase bucket.
#Do set BIN_DIR, DATA_DIR, BUCKET, IP, PORT for correct results.

verbose=false
if [ $# -eq 1 ] ; then
    if [ "$1" = '-v' ] ; then
        verbose=true
    fi
fi

BIN_DIR="/Users/abhinavdangeti/Documents/couchbaseS/install/bin"
DATA_DIR="/Users/abhinavdangeti/Documents/couchbaseS/ns_server/data/n_0/data"
BUCKET="default"

IP='127.0.0.1'
PORT='12000'

NUM_VBUCKETS=1024

writeOutputToFile=false

inMemCountFile="./temp_inMem.txt"
diskCountFile="./temp_inDisk.txt"

if [ "$writeOutputToFile" = true ] ; then
    rm $inMemCountFile
    rm $diskCountFile
fi

count=0
for (( i=0; i<$NUM_VBUCKETS; i++ ))
do
    x=`$BIN_DIR/cbstats $IP:$PORT vbucket-details $i | grep 'num_items'`
    if [ "$writeOutputToFile" = true ] ; then
        printf '%s ' $x >> $inMemCountFile
        printf '\n' >> $inMemCountFile
    fi
    mem_count=${x##* }
    y=`$BIN_DIR/couch_dbinfo $DATA_DIR/$BUCKET/$i.couch.* | grep '  doc count'`
    if [ "$writeOutputToFile" = true ] ; then
        z='vb: '$i' '$y
        printf '%s ' $z  >> $diskCountFile
        printf '\n' >> $diskCountFile
    fi
    disk_count=${y##* }

    if [ -n "$mem_count" ] && [ -n "$disk_count" ] ; then
        if [ "$mem_count" != "$disk_count" ] ; then
            echo 'Mismatch:: vb: '$i' => in-memory: '$mem_count'; in-disk: '$disk_count
            let "count++"
        elif [ "$verbose" = true ] ; then
            echo 'No mismatch on vb: '$i
        fi
    fi
done

echo '====================================='
echo 'Mismatches detected in: '$count' vbuckets!'
echo '====================================='
