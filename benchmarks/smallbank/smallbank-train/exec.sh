#!/bin/sh

#GOGC=off numactl -C 0-31 smallbank-train -ncores=32 -nsecs=1 -sr=500 -sys=3 -tm=3 -tc=sb-index-train.conf -wl=../smallbank.txt
#GOGC=off numactl -C 0-31 smallbank-train -ncores=32 -nsecs=1 -sr=500 -sys=3 -tm=0 -tc=sb-part-train.conf -wl=../smallbank.txt
#GOGC=off numactl -C 0-31 smallbank-train -ncores=32 -nsecs=1 -sr=500 -sys=3 -tm=1 -tc=sb-occ-train.conf -wl=../smallbank.txt
#GOGC=off numactl -C 0-31 smallbank-train -ncores=32 -nsecs=1 -sr=500 -sys=3 -tm=2 -tc=sb-occ-train.conf -wl=../smallbank.txt
#GOGC=off numactl -C 0-31 smallbank-train -ncores=32 -nsecs=1 -sr=500 -sys=3 -tm=2 -tc=sb-hotcold-test.conf -wl=../smallbank-big.txt
#GOGC=off numactl -C 0-31 smallbank-train -ncores=32 -nsecs=1 -sr=500 -sys=3 -tm=2 -tc=sb-zipf-test.conf -wl=../smallbank-big.txt
GOGC=off numactl -C 0-31 smallbank-train -ncores=32 -nsecs=1 -sr=500 -sys=3 -tm=2 -tc=train.conf -wl=../smallbank.txt
