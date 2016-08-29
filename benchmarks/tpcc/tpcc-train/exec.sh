#!/bin/sh

GOGC=off numactl -C 0-31 tpcc-train -ncores=32 -nsecs=1 -sr=500 -sys=3 -tm=2 -wl=../tpcc.txt -tc=train.conf -topk
#GOGC=off numactl -C 0-31 tpcc-train -ncores=32 -nsecs=1 -sr=500 -sys=3 -tm=0 -wl=../tpcc.txt -tc=tpcc-part-train.conf
#GOGC=off numactl -C 0-31 tpcc-train -ncores=32 -nsecs=1 -sr=500 -sys=3 -tm=1 -wl=../tpcc.txt -tc=tpcc-pure-train.conf
#GOGC=off numactl -C 0-31 tpcc-train -ncores=32 -nsecs=1 -sr=500 -sys=3 -tm=2 -wl=../tpcc.txt -tc=tpcc-pure-train.conf
#GOGC=off numactl -C 0-31 tpcc-train -ncores=32 -nsecs=1 -sr=500 -sys=3 -tm=3 -wl=../tpcc.txt -tc=tpcc-index-train.conf
#GOGC=off numactl -C 0-31 tpcc-train -ncores=32 -nsecs=1 -sr=500 -sys=3 -tm=2 -wl=../tpcc.txt -tc=tpcc-zipf-test.conf
#GOGC=off numactl -C 0-31 tpcc-train -ncores=32 -nsecs=1 -sr=500 -sys=3 -tm=2 -wl=../tpcc.txt -tc=tpcc-hotcold-test.conf
