README
======

- Make cluster configuration test.properties

    - node1         : IP of source
    - port1         : port of source
    - node2         : IP of destination
    - port2         : port of destination
    - bucket        : bucket name
    - password      : bucket password
    - prefix        : prefix for keys
    - start         : start point of range (keys)
    - end           : end point of range (keys)
    - write-to-file : boolean value to write records to file or not

- Run the following commands:
- To compile: make compile
- To run: make run
- To clean: make clean
