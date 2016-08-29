#!/bin/bash

GOGC=off numactl -C 0-31 tpcc-report -ncores=32 -nsecs=15 -sys=3 -report -tc=test.conf -p=false -loaders=8 -mergers=8
GOGC=off numactl -C 0-31 tpcc-report -ncores=32 -nsecs=15 -sys=0 -report -p=true -mergers=4 -loaders=4 -sr=500
GOGC=off numactl -C 0-31 tpcc-report -ncores=32 -nsecs=15 -sys=1 -report -p=false
GOGC=off numactl -C 0-31 tpcc-report -ncores=32 -nsecs=15 -sys=2 -report -p=false
#GOGC=off numactl -C 0-31 tpcc-report -ncores=32 -nsecs=15 -sys=1 -report
#GOGC=off numactl -C 0-31 tpcc-report -ncores=32 -nsecs=15 -sys=2 -report
#for i in 1 2 3 4 6 12 20 32
#for i in 1 2 4 8 16 32
#do
#    GOGC=off numactl -C 0-31 tpcc-report -ncores=32 -nsecs=2 -sys=3 -report -sr=500 -tc=test.conf -mergers=${i} -loaders=${i} -p=true
#done
