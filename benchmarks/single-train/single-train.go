package main

import (
	"flag"
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
var tp = flag.String("tp", "50:50", "Percetage of Each Transaction")
var out = flag.String("out", "data.out", "output file path")
var trainOut = flag.String("train", "train.out", "training set")
var prof = flag.Bool("prof", false, "whether perform CPU profile")
var tlen = flag.Int("len", 10, "number of operations per transaction")
var contention = flag.Float64("contention", 1, "theta factor of Zipf, 1 for uniform")
var rr = flag.Int("rr", 0, "Rate Percentage From 0 to 100")
var sr = flag.Int("st", 100, "Sample Rate")
var mp = flag.Int("mp", 2, "Number of Partitions to be Acccessed")
var ps = flag.Float64("ps", 1, "Skew For Partition")

const (
	BUFSIZE = 5
)

func main() {
	flag.Parse()
	if *testbed.SysType != testbed.ADAPTIVE {
		clog.Error("Training only Works for Adaptive CC\n")
	}

	if strings.Compare(*wl, "") == 0 {
		clog.Error("WorkLoad not specified\n")
	}

	if strings.Compare(*trainOut, "") == 0 {
		clog.Error("Training Output not specified\n")
	}

	if *testbed.Report {
		clog.Error("Report not Needed for Adaptive CC\n")
	}

	runtime.GOMAXPROCS(*testbed.NumPart)
	nWorkers := *testbed.NumPart

	if *prof {
		f, err := os.Create("single.prof")
		if err != nil {
			clog.Error(err.Error())
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	nParts := nWorkers
	*testbed.PhyPart = true
	isPartition := true
	clog.Info("Number of workers %v \n", nWorkers)
	clog.Info("Adaptive CC Training\n")

	single := testbed.NewSingleWL(*wl, nParts, isPartition, nWorkers, *contention, *tp, *cr, *tlen, *rr, *mp, *ps)
	coord := testbed.NewCoordinator(nWorkers, single.GetStore(), single.GetTableCount(), testbed.PARTITION, "", *sr, single.GetIDToKeyRange())

	clog.Info("Done with Populating Store\n")

	for j := testbed.PARTITION; j < testbed.ADAPTIVE; j++ {
		var wg sync.WaitGroup
		for i := 0; i < nWorkers; i++ {
			wg.Add(1)
			go func(n int, mode int) {
				//var txn int64
				//txn := 100000
				var t testbed.Trans
				//tq := testbed.NewTransQueue(BUFSIZE)
				w := coord.Workers[n]
				w.SetMode(mode)
				gen := single.GetTransGen(n)
				end_time := time.Now().Add(time.Duration(*nsecs) * time.Second)
				w.Start()
				for {
					tm := time.Now()
					if !end_time.After(tm) {
						break
					}
					//if txn <= 0 {
					//	break
					//}
					//tm := time.Now()
					//if tq.IsFull() {
					//	t = tq.Dequeue()
					//} else {
					t = gen.GenOneTrans()
					//}
					w.NGen += time.Since(tm)

					//tm = time.Now()
					//_, err := w.One(t)
					w.One(t)
					//w.NExecute += time.Since(tm)
					/*
						if err != nil {
							if err == testbed.EABORT {
								tq.Enqueue(t)
							} else if err == testbed.ENOKEY {
								clog.Error("%s\n", err.Error())
							} else if err != testbed.EABORT {
								clog.Error("%s\n", err.Error())
							}
						}*/
					//txn--
				}
				//clog.Info("Worker %d issues %d transactions\n", n, txn)
				w.Finish()
				wg.Done()
			}(i, j)
		}
		wg.Wait()

		coord.Finish()

		f, err := os.OpenFile(*out, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			clog.Error("Open File Error %s\n", err.Error())
		}
		defer f.Close()
		coord.PrintStats(f)

		f1, err1 := os.OpenFile(*trainOut, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err1 != nil {
			clog.Error("Open File Error %s\n", err.Error())
		}
		defer f1.Close()
		coord.PrintTraining(f1)

		coord.Reset()
	}

	clog.Info("%.f\n", float64(coord.NStats[testbed.NTXN]-coord.NStats[testbed.NABORTS])/coord.NExecute.Seconds())

}
