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
	CROSSRATE    = "CROSSRATE"
	MAXPARTITION = "MAXPARTITION"
	PARTSKEW     = "PARTSKEW"
	CONTENTION   = "CONTENTION"
	TRANLEN      = "TRANLEN"
	READRATE     = "READRATE"
	PERFDIFF     = 0.03
)

var nsecs = flag.Int("nsecs", 2, "number of seconds to run")
var wl = flag.String("wl", "../single.txt", "workload to be used")
var tp = flag.String("tp", "0:100", "Percetage of Each Transaction")
var out = flag.String("out", "data.out", "output file path")
var ro = flag.String("ro", "report.out", "report out")
var tc = flag.String("tc", "test.conf", "Test Configuration")
var prof = flag.Bool("prof", false, "whether perform CPU profile")
var sr = flag.Int("sr", 500, "Sample Rate")
var isPart = flag.Bool("p", true, "Whether index partition")
var dl = flag.String("dl", "layout.conf", "data layout")
var hotrec = flag.Int("hr", 100, "Number of Hot Records")

const (
	TRIALS  = 3
	BUFSIZE = 3
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*testbed.NumPart)
	nWorkers := *testbed.NumPart
	testbed.WLTYPE = testbed.SINGLEWL

	nParts := nWorkers
	isPartition := true
	initMode := testbed.PARTITION
	lockInit := false
	isPartAlign := true

	if *testbed.SysType == testbed.PARTITION {
		clog.Info("Using Partition-based CC\n")
	} else if *testbed.SysType == testbed.OCC {
		initMode = testbed.OCC
		isPartAlign = false
		if *isPart {
			clog.Info("Using OCC with partition\n")
		} else {
			nParts = 1
			isPartition = false
			clog.Info("Using OCC\n")
		}
	} else if *testbed.SysType == testbed.LOCKING {
		initMode = testbed.LOCKING
		isPartAlign = false
		if *isPart {
			clog.Info("Using 2PL with partition\n")
		} else {
			nParts = 1
			isPartition = false
			clog.Info("Using 2PL\n")
		}
	} else if *testbed.SysType == testbed.ADAPTIVE {
		if *isPart {
			if *testbed.Hybrid {
				initMode = testbed.LOCKING
				isPartAlign = false
			} else {
				initMode = testbed.PARTITION
				isPartAlign = true
			}

			clog.Info("Using Adaptive CC: Starting from PCC")
		} else {
			isPartAlign = false
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

	testbed.HOTREC = *hotrec * (*testbed.NumPart)

	testbed.InitGlobalBuffer()

	clog.Info("Number of workers %v \n", nWorkers)

	var single *testbed.SingelWorkload = nil
	var coord *testbed.Coordinator = nil

	testCases := testbed.BuildTestCases(*tc, testbed.SINGLEWL)
	wc := testbed.BuildWorkerConfig(*dl)
	useLatch := testbed.BuildUseLatch(wc)

	tc := &testCases[0]

	clog.Info("Populating Whole Store\n")
	single = testbed.NewSingleWL(*wl, nParts, isPartition, nWorkers, tc.Contention, *tp, tc.CR, tc.Tlen, tc.RR, tc.MP, tc.PS, initMode, false, isPartAlign, useLatch)
	single.MixConfig(wc)

	coord = testbed.NewCoordinator(nWorkers, single.GetStore(), single.GetTableCount(), initMode, *sr, testCases, *nsecs, testbed.SINGLEWL, single, wc)

	single.SetWorkers(coord)

	if *prof {
		f, err := os.Create("single.prof")
		if err != nil {
			clog.Error(err.Error())
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	clog.Info("CR %v MP %v PS %v Contention %v Tlen %v RR %v \n", tc.CR, tc.MP, tc.PS, tc.Contention, tc.Tlen, tc.RR)

	ts := testbed.TID(0)
	var wg sync.WaitGroup
	//coord.SetMode(initMode)
	coord.Start()
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func(n int) {
			if !lockInit {
				testbed.InitLockReqBuffer(n)
			}
			var t testbed.Trans
			w := coord.Workers[n]
			gen := single.GetTransGen(n)
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
						genTime := now.Sub(tm)
						tq.AddGen(genTime)
						w.NGen += genTime
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

	// Output Throughput Statistics
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
