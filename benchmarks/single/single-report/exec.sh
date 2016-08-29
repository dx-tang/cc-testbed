#!/bin/sh

GOGC=off numactl -C 0-31 single-report -ncores=32 -nsecs=15 -sys=3 -report -p=false -loaders=8 -sr=500 -mergers=8
GOGC=off numactl -C 0-31 single-report -ncores=32 -nsecs=15 -sys=0 -report -p -loaders=4 -sr=500 -mergers=4
GOGC=off numactl -C 0-31 single-report -ncores=32 -nsecs=15 -sys=1 -report -p=false -loaders=4 -sr=500 -mergers=4
GOGC=off numactl -C 0-31 single-report -ncores=32 -nsecs=15 -sys=2 -report -p=false -loaders=4 -sr=500 -mergers=4
#GOGC=off numactl -C 0-31 single-report -ncores=32 -nsecs=15 -sys=1 -report -p -loaders=4 -sr=500 -mergers=4
#GOGC=off numactl -C 0-31 single-report -ncores=32 -nsecs=15 -sys=2 -report -p -loaders=4 -sr=500 -mergers=4 
#GOGC=off numactl -C 0-31 single-report -ncores=32 -nsecs=4 -sys=2 -report -p=false -loaders=4 -sr=1000 -mergers=4
#for i in 1
#do
#	GOGC=off numactl -C 0-31 single-report -ncores=32 -nsecs=2 -sys=3 -report -p -loaders=${i} -sr=500 -mergers=${i}
#done

