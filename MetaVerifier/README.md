README
======

- Edit cluster configuration in test.properties

======
    - node1                 : IP of source
    - port1                 : port of source
    - node2                 : IP of destination
    - port2                 : port of destination
    - bucket                : bucket name
    - password              : bucket password
    - prefix                : prefix for keys
    - start                 : start point of range (keys)
    - end                   : end point of range (keys)
    - write-output-to-file  : boolean value to write mismatches to file (log.txt)
    - write-all-to-file     : boolean value to write all records to file
======

- Run the following commands:
- To compile: make compile
- To run: make run
- To clean: make clean
