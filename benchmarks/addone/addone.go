package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/totemtang/cc-testbed"
	"github.com/totemtang/cc-testbed/clog"
)

var nsec = flag.Int("nsec", 2, "number of seconds to run")
var rr = flag.Float64("rr", 0, "percentage of read operations")
var contention = flag.Float64("contention", 1, "theta factor of Zipf, 1 for uniform")
var txnlen = flag.Int("txnlen", 16, "number of operations for each transaction")
var nKeys = flag.Int64("nkeys", 1000000, "number of keys")
var out = flag.String("out", "data.out", "output file path")
var skew = flag.Float64("skew", 0, "skew factor for partition-based concurrency control (Zipf)")
var mp = flag.Int("mp", 1, "Max partitions cross-partition transactions will touch")
var benchStat = flag.String("bs", "", "Output file for benchmark statistics")

func main() {
	flag.Parse()

	// set max cores used, number of clients and number of workers
	runtime.GOMAXPROCS(*testbed.NumPart)
	clients := *testbed.NumPart
	nworkers := *testbed.NumPart

	if *contention < 0 || *contention > 1 {
		clog.Error("Contention factor should be between 0 and 1")
	}

	clog.Info("Number of clients %v, Number of workers %v \n", clients, nworkers)

	// create store
	s := testbed.NewStore()
	var nParts int
	var hp testbed.Partitioner = nil
	var pKeysArray []int64

	if *testbed.SysType == testbed.PARTITION {
		nParts = *testbed.NumPart
		pKeysArray = make([]int64, nParts)

		hp = &testbed.HashPartitioner{
			NParts: int64(nParts),
			NKeys:  int64(*nKeys),
		}

		var partNum int
		for i := int64(0); i < *nKeys; i++ {
			k := testbed.CKey(i)
			partNum = hp.GetPartition(k)
			pKeysArray[partNum]++
			s.CreateKV(k, int64(0), testbed.SINGLEINT, partNum)
		}

	} else {
		nParts = 1
		for i := int64(0); i < *nKeys; i++ {
			k := testbed.CKey(i)
			s.CreateKV(k, int64(0), testbed.SINGLEINT, 0)
		}
	}

	generators := make([]*testbed.TxnGen, nworkers)

	for i := 0; i < nParts; i++ {
		zk := testbed.NewZipfKey(i, *nKeys, nParts, pKeysArray, *contention, hp)
		generators[i] = testbed.NewTxnGen(testbed.ADD_ONE, *rr, *txnlen, *mp, zk)
	}

	coord := testbed.NewCoordinator(nworkers, s)

	clog.Info("Done with Initialization")

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(n int) {
			//var count int
			w := coord.Workers[n]
			end_time := time.Now().Add(time.Duration(*nsec) * time.Second)
			for {
				tm := time.Now()
				if !end_time.After(tm) {
					break
				}
				//if count >= 64775 {
				//	break
				//}
				//count++
				//tm := time.Now()
				q := generators[n].GenOneQuery()
				w.NGen += time.Since(tm)
				//time.Sleep(1 * time.Microsecond)
				tm = time.Now()
				_, err := w.One(q)
				w.NExecute += time.Since(tm)
				if err == testbed.ENOKEY {
					clog.Error("No Key Error")
					break
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	f, err := os.OpenFile(*out, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		clog.Error("Open File Error %s\n", err.Error())
	}
	defer f.Close()
	coord.PrintStats(f)

	if *benchStat != "" {
		bs, err := os.OpenFile(*benchStat, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			clog.Error("Open File Error %s\n", err.Error())
		}
		defer bs.Close()

		bs.WriteString(fmt.Sprintf("%v\t%v\n", *testbed.CrossPercent, coord.NStats[testbed.NTXN]))
	}

}
