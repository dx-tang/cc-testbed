#!/bin/bash

for cr in 30
do
    for i in 0 1 2 3 4 5 6 7 8 9
    do
    addone -nsec=10 -ncores=2 -txnlen=100 -rr=50 -mp=2 -cr=${cr} -nkeys=5000000 -bs=benchstat
    done
done
