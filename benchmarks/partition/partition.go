package main

import (
	"flag"
	"fmt"
	"github.com/totemtang/cc-testbed"
	"log"
	"runtime"
)

var ncores = flag.Int("ncores", 2, "number of cores to be used")
var nsec = flag.Int("nsec", 10, "number of seconds to run")
var rr = flag.Float64("rr", 0.5, "percentage of read operations")
var contention = flag.Float64("contention", 0, "theta factor of Zipf")
var txnlen = flag.Int("txnlen", 16, "number of operations for each transaction")
var nkeys = flag.Int64("nkeys", 1000000, "number of keys")
var out = flag.String("out", "data.out", "output file path")
var skew = flag.Float64("skew", 0, "skew factor for partition-based concurrency control (Zipf)")
var sys = flag.Int("sys", testbed.PARTITION, "System Type we will use")

func main() {
	flag.Parse()

	// set max cores used, number of clients and number of workers
	runtime.GOMAXPROCS(*ncores)
	clients := *ncores
	nworkers := *ncores

	if *contention < 0 || *contention > 1 {
		log.Fatalf("Contention factor should be between 0 and 1")
	}

	fmt.Printf("Number of clients %v, Number of workers %v \n", clients, nworkers)

	// create store
	s := testbed.NewStore()
	var nParts int
	if *sys == testbed.PARTITION {
		nParts = *ncores
		hp := &testbed.HashPartitioner{
			NParts: int64(nParts),
		}
		for i := int64(0); i < *nkeys; i++ {
			k := testbed.CKey(i)
			s.CreateKV(k, int64(0), testbed.SINGLEINT, hp.GetPartition(k))
		}
	} else {
		nParts = 1
		for i := int64(0); i < *nkeys; i++ {
			k := testbed.CKey(i)
			s.CreateKV(k, int64(0), testbed.SINGLEINT, 0)
		}
	}
}
