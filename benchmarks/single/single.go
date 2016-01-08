package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
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
var txntype = flag.String("tt", "addone", "set transaction type")

func main() {
	flag.Parse()

	// set max cores used, number of clients and number of workers
	runtime.GOMAXPROCS(*testbed.NumPart)
	clients := *testbed.NumPart
	nworkers := *testbed.NumPart

	if *contention < 1 {
		clog.Error("Contention factor should be between no less than 1")
	}

	clog.Info("Number of clients %v, Number of workers %v \n", clients, nworkers)
	if *testbed.SysType == testbed.PARTITION {
		clog.Info("Using Partition-based CC\n")
	} else if *testbed.SysType == testbed.OCC {
		if *testbed.PhyPart {
			clog.Info("Using OCC with partition\n")
		} else {
			clog.Info("Using OCC\n")
		}
	} else {
		clog.Error("Not supported type %v CC\n", *testbed.SysType)
	}

	tt, dt := getTxn(*txntype)

	// create store
	s := testbed.NewStore()
	var nParts int
	var hp testbed.Partitioner = nil
	var pKeysArray []int64
	var value interface{}

	if *testbed.SysType == testbed.PARTITION || *testbed.PhyPart {
		nParts = *testbed.NumPart
		pKeysArray = make([]int64, nParts)

		hp = &testbed.HashPartitioner{
			NParts: int64(nParts),
			NKeys:  int64(*nKeys),
		}

		var partNum int
		for i := int64(0); i < *nKeys; i++ {
			k := testbed.Key(i)
			partNum = hp.GetPartition(k)
			pKeysArray[partNum]++
			if dt == testbed.SINGLEINT {
				value = int64(0)
			} else if dt == testbed.STRINGLIST {
				value = testbed.GenStringList()
			}
			s.CreateKV(k, value, dt, partNum)
		}

	} else {
		nParts = 1
		for i := int64(0); i < *nKeys; i++ {
			k := testbed.Key(i)
			if dt == testbed.SINGLEINT {
				value = int64(0)
			} else if dt == testbed.STRINGLIST {
				value = testbed.GenStringList()
			}
			s.CreateKV(k, value, dt, 0)
		}
	}

	generators := make([]*testbed.TxnGen, nworkers)

	for i := 0; i < nworkers; i++ {
		p := &testbed.HashPartitioner{
			NParts: int64(nParts),
			NKeys:  int64(*nKeys),
		}
		zk := testbed.NewZipfKey(i, *nKeys, nParts, pKeysArray, *contention, p)
		generators[i] = testbed.NewTxnGen(i, tt, *rr, *txnlen, *mp, zk)
	}

	coord := testbed.NewCoordinator(nworkers, s)

	clog.Info("Done with Initialization")

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(n int) {
			//var txn int64
			//var count int
			w := coord.Workers[n]
			end_time := time.Now().Add(time.Duration(*nsec) * time.Second)
			for {
				tm := time.Now()
				if !end_time.After(tm) {
					break
				}

				q := generators[n].GenOneQuery()

				//q.DoNothing()
				w.NGen += time.Since(tm)
				tm = time.Now()
				_, err := w.One(q)
				w.NExecute += time.Since(tm)
				if err == testbed.ENOKEY {
					clog.Error("No Key Error")
					break
				}
				//txn++
			}
			//clog.Info("Worker %d issues %d transactions\n", n, txn)
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

		//bs.WriteString(fmt.Sprintf("%v\t%v\n", *testbed.CrossPercent, coord.NStats[testbed.NTXN]-coord.NStats[testbed.NABORTS]))
		bs.WriteString(fmt.Sprintf("%.f\n", float64(coord.NStats[testbed.NTXN]-coord.NStats[testbed.NABORTS])/coord.NExecute.Seconds()))
	}

}

func getTxn(txntype string) (int, testbed.RecType) {
	if strings.Compare(txntype, "addone") == 0 {
		return testbed.ADD_ONE, testbed.SINGLEINT
	} else if strings.Compare(txntype, "updateint") == 0 {
		return testbed.RANDOM_UPDATE_INT, testbed.SINGLEINT
	} else if strings.Compare(txntype, "updatestring") == 0 {
		return testbed.RANDOM_UPDATE_STRING, testbed.STRINGLIST
	} else {
		clog.Error("Not Supported %s Transaction", txntype)
		return -1, -1
	}
}
