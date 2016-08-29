#!/bin/bash

GOGC=off numactl -C 0-31 single-simple -contention=1.01 -cr=100 -len=18 -mp=2 -ncores=32 -nsecs=2 -p=false -rr=50 -sys=2 -tp=0:100
