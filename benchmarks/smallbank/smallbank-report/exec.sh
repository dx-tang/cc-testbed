#!/bin/sh

#for i in 4 8
#do
#	GOGC=off numactl -C 0-31 smallbank-report -loaders=${i} -mergers=${i} -ncores=32 -nsecs=2 -report -sys=3 -p=true -sr=500;
#done

GOGC=off numactl -C 0-31 smallbank-report -loaders=8 -mergers=8 -ncores=32 -nsecs=15 -report -sys=3 -p=false -sr=500
GOGC=off numactl -C 0-31 smallbank-report -loaders=4 -mergers=4 -ncores=32 -nsecs=15 -report -sys=0 -p=true
GOGC=off numactl -C 0-31 smallbank-report -loaders=4 -mergers=4 -ncores=32 -nsecs=15 -report -sys=1 -p=false
GOGC=off numactl -C 0-31 smallbank-report -loaders=4 -mergers=4 -ncores=32 -nsecs=15 -report -sys=2 -p=false
#GOGC=off numactl -C 0-31 smallbank-report -loaders=4 -mergers=4 -ncores=32 -nsecs=15 -report -sys=1 -p=false
#GOGC=off numactl -C 0-31 smallbank-report -loaders=4 -mergers=4 -ncores=32 -nsecs=15 -report -sys=2 -p=false
##for i in 1 2 3 4 6 12 20 32
#do
#	GOGC=off numactl -C 0-31 smallbank-report -loaders=${i} -mergers=${i} -ncores=32 -nsecs=30 -report -sys=3 -p=false -sr=2000
#done
