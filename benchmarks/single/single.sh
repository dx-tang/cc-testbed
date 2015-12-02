#!/bin/bash


#for i in 0 1 2 3 4 5 6 7 8 9
#do
#    single -nsec=10 -ncores=2 -txnlen=10 -rr=50 -mp=2 -cr=0 -nkeys=500000 -bs=benchstat -sys=1 -tt=updatestring -contention=1.01 -p
#done

#for i in 0 1 2 3 4 5 6 7 8 9
#do
#    single -nsec=10 -ncores=2 -txnlen=100 -rr=50 -mp=2 -cr=0 -nkeys=5000000 -bs=benchstat -sys=1 -tt=addone -contention=1
#done

#for i in 0 1 2 3 4 5 6 7 8 9
#do
#    single -nsec=10 -ncores=2 -txnlen=100 -rr=50 -mp=2 -cr=0 -nkeys=5000000 -bs=benchstat -sys=1 -tt=addone -contention=1.01 -p
#done

#for cr in 0 10 20 30 40 50 60 70 80 90 100
#do
    #for i in 0 1 2 3 4 5 6 7 8 9
    #do
    single -nsec=10 -ncores=2 -txnlen=100 -rr=50 -mp=2 -cr=0 -nkeys=500000 -bs=benchstat -sys=0 -tt=addone -contention=1
    #done
#done


#for cr in 0 10 20 30 40 50 60 70 80 90 100
#do
#    for i in 0 1 2 3 4 5 6 7 8 9
#    do
#    single -nsec=10 -ncores=2 -txnlen=100 -rr=50 -mp=2 -cr=${cr} -nkeys=5000000 -bs=benchstat -sys=1 -tt=addone -contention=1.01 -p
#    done
#done
