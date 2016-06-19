package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/totemtang/cc-testbed"
	"github.com/totemtang/cc-testbed/clog"
)

const (
	CROSSRATE  = "CROSSRATE"
	PARTSKEW   = "PARTSKEW"
	CONTENTION = "CONTENTION"
	TRANSPER   = "TRANSPER"
	PERFDIFF   = 0.03
)

var nsecs = flag.Int("nsecs", 2, "number of seconds to run")
var wl = flag.String("wl", "../tpcc.txt", "workload to be used")
var out = flag.String("out", "data.out", "output file path")
var ro = flag.String("ro", "report.out", "report out")
var tc = flag.String("tc", "test.conf", "Test Configuration")
var prof = flag.Bool("prof", false, "whether perform CPU profile")
var sr = flag.Int("sr", 500, "Sample Rate")
var isPart = flag.Bool("p", true, "Whether partition index")
var dataDir = flag.String("dd", "../data", "TPCC Data Dir")

const (
	TRIALS  = 3
	BUFSIZE = 3
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*testbed.NumPart)
	nWorkers := *testbed.NumPart
	testbed.WLTYPE = testbed.TPCCWL

	nParts := nWorkers
	isPartition := true
	initMode := testbed.PARTITION
	lockInit := false

	if *testbed.SysType == testbed.PARTITION {
		clog.Info("Using Partition-based CC\n")
	} else if *testbed.SysType == testbed.OCC {
		initMode = testbed.OCC
		if *isPart {
			clog.Info("Using OCC with partition\n")
		} else {
			nParts = 1
			isPartition = false
			clog.Info("Using OCC\n")
		}
	} else if *testbed.SysType == testbed.LOCKING {
		initMode = testbed.LOCKING
		if *isPart {
			clog.Info("Using 2PL with partition\n")
		} else {
			nParts = 1
			isPartition = false
			clog.Info("Using 2PL\n")
		}
	} else if *testbed.SysType == testbed.ADAPTIVE {
		clog.Info("Using Adaptive CC\n")
	} else {
		clog.Error("Not supported type %v CC\n", *testbed.SysType)
	}

	if strings.Compare(*dataDir, "") == 0 {
		clog.Error("Datadir not specified\n")
	}

	if strings.Compare(*wl, "") == 0 {
		clog.Error("WorkLoad not specified\n")
	}

	if strings.Compare(*ro, "") == 0 {
		clog.Error("Report Output not specified\n")
	}

	if strings.Compare(*tc, "") == 0 {
		clog.Error("Test Configuration not specified\n")
	}

	if !*testbed.Report {
		clog.Error("Report Needed for Adaptive CC Execution\n")
	}

	testbed.InitGlobalBuffer()

	clog.Info("Number of workers %v \n", nWorkers)

	var tpccWL *testbed.TPCCWorkload = nil
	var coord *testbed.Coordinator = nil

	testCases := testbed.BuildTestCases(*tc, testbed.TPCCWL)

	clog.Info("Populating Whole Store\n")
	tpccWL = testbed.NewTPCCWL(*wl, nParts, isPartition, nWorkers, testCases[0].Contention, testCases[0].TPCCTransPer, testCases[0].CR, testCases[0].PS, *dataDir, initMode, false)
	coord = testbed.NewCoordinator(nWorkers, tpccWL.GetStore(), tpccWL.GetTableCount(), initMode, *sr, testCases, *nsecs, testbed.TPCCWL, tpccWL)

	// Populate Key Gen and Part Gen
	clog.Info("Populating Key Generators and Part Generators\n")
	if *prof {
		f, err := os.Create("tpcc.prof")
		if err != nil {
			clog.Error(err.Error())
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	clog.Info("CR %v PS %v Contention %v TransPer %v \n", testCases[0].CR, testCases[0].PS, testCases[0].Contention, testCases[0].TPCCTransPer)

	ts := testbed.TID(0)
	var wg sync.WaitGroup
	coord.SetMode(initMode)
	coord.Start()
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func(n int) {
			if !lockInit {
				testbed.InitLockReqBuffer(n)
			}
			var t testbed.Trans
			w := coord.Workers[n]
			gen := tpccWL.GetTransGen(n)
			tq := testbed.NewTransQueue(BUFSIZE)
			w.Start()
			for {
				tm := time.Now()

				t = tq.Executable()

				if t == nil {
					t = gen.GenOneTrans(initMode)
					t.SetTrial(TRIALS)
					if *testbed.SysType == testbed.LOCKING && !*testbed.NoWait {
						tid := testbed.TID(atomic.AddUint64((*uint64)(&ts), 1))
						t.SetTID(tid)
					}
				}
				if t.GetTXN() != -1 {
					w.NGen += time.Since(tm)
				}

				_, err := w.One(t)

				if err != nil {
					if err == testbed.EABORT {
						t.DecTrial()
						if t.GetTrial() == 0 {
							gen.ReleaseOneTrans(t)
						} else {
							penalty := time.Now().Add(time.Duration(testbed.PENALTY) * time.Microsecond)
							t.SetPenalty(penalty)
							tq.Enqueue(t)
						}
					} else if err == testbed.FINISHED {
						break
					} else if err != testbed.EABORT {
						clog.Error("%s\n", err.Error())
					}
				} else {
					if t.GetTXN() != -1 {
						gen.ReleaseOneTrans(t)
					}
				}
			}
			w.Finish()
			wg.Done()
		}(i)
	}
	wg.Wait()

	coord.Finish()

	f, err := os.OpenFile(*ro, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		clog.Error("Open File Error %s\n", err.Error())
	}
	defer f.Close()

	txnAr := coord.TxnAR
	modeAr := coord.ModeAR
	for i, txn := range txnAr {
		f.WriteString(fmt.Sprintf("%d\t%.4f\n", modeAr[i], txn))
	}
}
