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
var wl = flag.String("wl", "../smallbank.txt", "workload to be used")
var out = flag.String("out", "data.out", "output file path")
var ro = flag.String("ro", "report.out", "report out")
var tc = flag.String("tc", "test.conf", "Test Configuration")
var prof = flag.Bool("prof", false, "whether perform CPU profile")
var sr = flag.Int("sr", 500, "Sample Rate")
var isPart = flag.Bool("p", true, "Whether partition index")

const (
	TRIALS  = 3
	BUFSIZE = 3
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*testbed.NumPart)
	nWorkers := *testbed.NumPart
	testbed.WLTYPE = testbed.SMALLBANKWL

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
		if *isPart {
			initMode = testbed.PARTITION
			clog.Info("Using Adaptive CC: Starting from PCC")
		} else {
			nParts = 1
			isPartition = false
			initMode = testbed.LOCKING
			clog.Info("Using Adaptive CC: Starting from 2PL")
		}
	} else {
		clog.Error("Not supported type %v CC\n", *testbed.SysType)
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

	var sb *testbed.SBWorkload = nil
	var coord *testbed.Coordinator = nil

	testCases := testbed.BuildTestCases(*tc, testbed.SMALLBANKWL)
	tc := &testCases[0]

	clog.Info("Populating Whole Store\n")
	sb = testbed.NewSmallBankWL(*wl, nParts, isPartition, nWorkers, tc.Contention, tc.SBTransper, tc.CR, tc.PS, initMode, false)
	coord = testbed.NewCoordinator(nWorkers, sb.GetStore(), sb.GetTableCount(), testbed.PARTITION, *sr, testCases, *nsecs, testbed.SMALLBANKWL, sb)

	sb.SetWorkers(coord)

	if *prof {
		f, err := os.Create("smallbank.prof")
		if err != nil {
			clog.Error(err.Error())
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	clog.Info("CR %v PS %v Contention %v TransPer %v \n", tc.CR, tc.PS, tc.Contention, tc.SBTransper)

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
			gen := sb.GetTransGen(n)
			tq := testbed.NewTransQueue(BUFSIZE)
			w.Start()
			for {

				t = tq.Executable()

				if t == nil {
					tm := time.Now()

					t = gen.GenOneTrans(initMode)
					t.SetTrial(TRIALS)
					if *testbed.SysType == testbed.LOCKING && !*testbed.NoWait {
						tid := testbed.TID(atomic.AddUint64((*uint64)(&ts), 1))
						t.SetTID(tid)
					}
					if t.GetTXN() != -1 {
						now := time.Now()
						t.SetStartTime(now)
						w.NGen += now.Sub(tm)
					}
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
