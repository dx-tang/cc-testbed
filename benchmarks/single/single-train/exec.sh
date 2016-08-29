#!/bin/bash

#GOGC=off numactl -C 0-31 single-train -ncores=32 -nw=true -sr=500 -sys=3 -tc=single-part-train.conf -nsecs=1 -tm=0 -wl=../single.txt
#GOGC=off numactl -C 0-31 single-train -ncores=32 -nw=true -sr=500 -sys=3 -tc=single-occ-train.conf -nsecs=1 -tm=1 -wl=../single.txt
#GOGC=off numactl -C 0-31 single-train -ncores=32 -nw=true -sr=500 -sys=3 -tc=single-occ-train.conf -nsecs=1 -tm=2 -wl=../single.txt
#GOGC=off numactl -C 0-31 single-train -ncores=32 -nw=true -sr=500 -sys=3 -tc=single-index-train.conf -nsecs=1 -tm=3 -wl=../single.txt
#GOGC=off numactl -C 0-31 single-train -ncores=32 -nw=true -sr=500 -sys=3 -tc=single-hotcold-test.conf -nsecs=1 -tm=2 -wl=../single.txt
#GOGC=off numactl -C 0-31 single-train -ncores=32 -nw=true -sr=500 -sys=3 -tc=single-zipf-test.conf -nsecs=1 -tm=2 -wl=../single.txt
GOGC=off numactl -C 0-31 single-train -ncores=32 -nw=true -sr=500 -sys=3 -tc=train.conf -nsecs=1 -tm=2 -wl=../single.txt

