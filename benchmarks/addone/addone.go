package main

import (
	"flag"
	"runtime"

	"github.com/totemtang/cc-testbed"
	"github.com/totemtang/cc-testbed/clog"
)

var ncores = flag.Int("ncores", 2, "number of cores to be used")
var nsec = flag.Int("nsec", 10, "number of seconds to run")
var rr = flag.Float64("rr", 0.5, "percentage of read operations")
var contention = flag.Float64("contention", 1, "theta factor of Zipf, 1 for uniform")
var txnlen = flag.Int("txnlen", 16, "number of operations for each transaction")
var nKeys = flag.Int64("nkeys", 1000000, "number of keys")
var out = flag.String("out", "data.out", "output file path")
var skew = flag.Float64("skew", 0, "skew factor for partition-based concurrency control (Zipf)")
var sys = flag.Int("sys", testbed.PARTITION, "System Type we will use")
var mp = flag.Int("mp", 1, "Max partitions cross-partition transactions will touch")

func main() {
	flag.Parse()

	// set max cores used, number of clients and number of workers
	runtime.GOMAXPROCS(*ncores)
	clients := *ncores
	nworkers := *ncores

	if *contention < 0 || *contention > 1 {
		clog.Error("Contention factor should be between 0 and 1")
	}

	clog.Info("Number of clients %v, Number of workers %v \n", clients, nworkers)

	// create store
	s := testbed.NewStore()
	var nParts int
	var hp testbed.Partitioner = nil
	var pKeysArray []int64

	if *sys == testbed.PARTITION {
		nParts = *ncores
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
		zk := testbed.NewZipfKey(i, nKeys, nParts, pKeysArray, *contention, hp)
		generators[i] = testbed.NewTxnGen(testbed.ADD_ONE, *rr, *txnlen, *mp)
	}

	clog.Info("Done with Initialization")

}
