package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/totemtang/cc-testbed"
	"github.com/totemtang/cc-testbed/clog"
)

var nsecs = flag.Int("nsecs", 2, "number of seconds to run")
var cr = flag.Float64("cr", 0, "percentage of cross-partition transactions")
var wl = flag.String("wl", "", "workload to be used")
var contention = flag.Float64("contention", 1, "theta factor of Zipf, 1 for uniform")
var tp = flag.String("tp", "100:0:0:0:0:0", "Percetage of Each Transaction")
var out = flag.String("out", "data.out", "output file path")
var stat = flag.String("stat", "stat.out", "statistics")
var prof = flag.Bool("prof", false, "whether perform CPU profile")

const (
	TRIAL = 1
)

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*testbed.NumPart)
	nWorkers := *testbed.NumPart

	if strings.Compare(*wl, "") == 0 {
		clog.Error("WorkLoad not specified\n")
	}

	if *prof {
		f, err := os.Create("smallbank.prof")
		if err != nil {
			clog.Error(err.Error())
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	nParts := nWorkers
	isPartition := true
	clog.Info("Number of workers %v \n", nWorkers)
	if *testbed.SysType == testbed.PARTITION {
		clog.Info("Using Partition-based CC\n")
	} else if *testbed.SysType == testbed.OCC {
		if *testbed.PhyPart {
			clog.Info("Using OCC with partition\n")
		} else {
			nParts = 1
			isPartition = false
			clog.Info("Using OCC\n")
		}
	} else if *testbed.SysType == testbed.LOCKING {
		if *testbed.PhyPart {
			clog.Info("Using 2PL with partition\n")
		} else {
			nParts = 1
			isPartition = false
			clog.Info("Using 2PL\n")
		}
	} else {
		clog.Error("Not supported type %v CC\n", *testbed.SysType)
	}

	sb := testbed.NewSmallBankWL(*wl, nParts, isPartition, nWorkers, *contention, *tp, *cr)
	coord := testbed.NewCoordinator(nWorkers, sb.GetStore(), sb.GetTableCount())

	clog.Info("Done with Populating Store\n")

	var wg sync.WaitGroup
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func(n int) {
			//var txn int64
			//txn := 100000
			w := coord.Workers[n]
			gen := sb.GetTransGen(n)
			end_time := time.Now().Add(time.Duration(*nsecs) * time.Second)
			for {
				tm := time.Now()
				if !end_time.After(tm) {
					break
				}
				//if txn <= 0 {
				//	break
				//}
				//tm := time.Now()
				t := gen.GenOneTrans()
				w.NGen += time.Since(tm)
				tm = time.Now()
				for j := 0; j < TRIAL; j++ {
					_, err := w.One(t)
					if err == nil {
						break
					} else if err == testbed.ENOKEY {
						clog.Error("%s\n", err.Error())
					} else if err != testbed.EABORT {
						clog.Error("%s\n", err.Error())
					}
				}
				w.NExecute += time.Since(tm)
				//txn--
			}
			//clog.Info("Worker %d issues %d transactions\n", n, txn)
			wg.Done()
		}(i)
	}
	wg.Wait()

	sb.PrintChecking()

	f, err := os.OpenFile(*out, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		clog.Error("Open File Error %s\n", err.Error())
	}
	defer f.Close()
	coord.PrintStats(f)

	if *stat != "" {
		st, err := os.OpenFile(*stat, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			clog.Error("Open File Error %s\n", err.Error())
		}
		defer st.Close()

		//bs.WriteString(fmt.Sprintf("%v\t%v\n", *testbed.CrossPercent, coord.NStats[testbed.NTXN]-coord.NStats[testbed.NABORTS]))
		st.WriteString(fmt.Sprintf("%.f\n", float64(coord.NStats[testbed.NTXN]-coord.NStats[testbed.NABORTS])/coord.NExecute.Seconds()))
	}

}
