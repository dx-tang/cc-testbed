package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
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
var wl = flag.String("wl", "", "workload to be used")
var tp = flag.String("tp", "50:50", "Percetage of Each Transaction")
var out = flag.String("out", "data.out", "output file path")
var ro = flag.String("ro", "report.out", "report out")
var tc = flag.String("tc", "test.conf", "Test Configuration")
var prof = flag.Bool("prof", false, "whether perform CPU profile")
var sr = flag.Int("sr", 100, "Sample Rate")
var rc = flag.String("rc", "report.conf", "configuration for reporting")
var isPart = flag.Bool("p", true, "Whether index partition")

var cr []float64
var mp []int
var ps []float64
var contention []float64
var tlen []int
var rr []int
var tests []int

const (
	BUFSIZE = 5
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*testbed.NumPart)
	nWorkers := *testbed.NumPart

	nParts := nWorkers
	isPhysical := false
	isPartition := true
	curMode := testbed.PARTITION

	if *testbed.SysType == testbed.PARTITION {
		clog.Info("Using Partition-based CC\n")
	} else if *testbed.SysType == testbed.OCC {
		curMode = testbed.OCC
		if *isPart {
			clog.Info("Using OCC with partition\n")
		} else {
			nParts = 1
			isPartition = false
			clog.Info("Using OCC\n")
		}
	} else if *testbed.SysType == testbed.LOCKING {
		curMode = testbed.LOCKING
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

	if strings.Compare(*wl, "") == 0 {
		clog.Error("WorkLoad not specified\n")
	}

	if strings.Compare(*ro, "") == 0 {
		clog.Error("Report Output not specified\n")
	}

	if strings.Compare(*rc, "") == 0 {
		clog.Error("Report Configuration not specified\n")
	}

	if strings.Compare(*tc, "") == 0 {
		clog.Error("Test Configuration not specified\n")
	}

	if !*testbed.Report {
		clog.Error("Report Needed for Adaptive CC Execution\n")
	}

	if *prof {
		f, err := os.Create("single.prof")
		if err != nil {
			clog.Error(err.Error())
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	clog.Info("Number of workers %v \n", nWorkers)

	ParseReportConf(*rc)
	ParseTestConf(*tc)
	keyGenPool := make(map[float64][][]testbed.KeyGen)
	partGenPool := make(map[float64][]testbed.PartGen)
	var single *testbed.SingelWorkload = nil
	var coord *testbed.Coordinator = nil

	for k := 0; k < len(tests); k++ {
		d := tests[k]
		r := d % len(cr)

		tmpCR := cr[r]
		d = d / len(cr)

		r = d % len(mp)
		tmpMP := mp[r]
		d = d / len(mp)

		r = d % len(ps)
		tmpPS := ps[r]
		d = d / len(ps)

		r = d % len(contention)
		tmpContention := contention[r]
		d = d / len(contention)

		r = d % len(tlen)
		tmpTlen := tlen[r]
		d = d / len(tlen)

		tmpRR := rr[d]

		if single == nil {
			// Initialize
			clog.Info("Populating Whole Store\n")
			single = testbed.NewSingleWL(*wl, nParts, isPartition, isPhysical, nWorkers, tmpContention, *tp, tmpCR, tmpTlen, tmpRR, tmpMP, tmpPS)
			coord = testbed.NewCoordinator(nWorkers, single.GetStore(), single.GetTableCount(), testbed.PARTITION, *sr, single.GetIDToKeyRange(), len(tests), *nsecs, testbed.SINGLEWL)

			// Populate Key Gen and Part Gen
			clog.Info("Populating Key Generators and Part Generators\n")
			basic := single.GetBasicWL()
			for _, ct := range contention {
				keyGens, ok := keyGenPool[ct]
				if !ok {
					keyGens = basic.NewKeyGen(ct)
					keyGenPool[ct] = keyGens
				}
			}

			for _, skew := range ps {
				partGens, ok := partGenPool[skew]
				if !ok {
					partGens = basic.NewPartGen(skew)
					partGenPool[skew] = partGens
				}
			}

		} else {
			basic := single.GetBasicWL()
			keyGens, ok := keyGenPool[tmpContention]
			if !ok {
				clog.Error("Lacking Key Gen With %v\n", tmpContention)
			}

			partGens, ok1 := partGenPool[tmpPS]
			if !ok1 {
				clog.Error("Lacking Part Gen With %v\n", tmpPS)
			}
			basic.SetKeyGen(keyGens)
			basic.SetPartGen(partGens)
			single.ResetConf(*tp, tmpCR, tmpMP, tmpTlen, tmpRR)
		}

		clog.Info("CR %v MP %v PS %v Contention %v Tlen %v RR %v \n", tmpCR, tmpMP, tmpPS, tmpContention, tmpTlen, tmpRR)

		ts := testbed.TID(0)
		var wg sync.WaitGroup
		coord.SetMode(curMode)
		coord.Start()
		for i := 0; i < nWorkers; i++ {
			wg.Add(1)
			go func(n int) {
				var t testbed.Trans
				w := coord.Workers[n]
				gen := single.GetTransGen(n)
				w.Start()
				for {
					tm := time.Now()

					t = gen.GenOneTrans()

					w.NGen += time.Since(tm)

					if curMode == testbed.LOCKING && !*testbed.NoWait {
						tid := testbed.TID(atomic.AddUint64((*uint64)(&ts), 1))
						t.SetTID(tid)
					}

					_, err := w.One(t)
					if err == testbed.FINISHED {
						break
					}
				}
				w.Finish()
				wg.Done()
			}(i)
		}
		wg.Wait()

		coord.Finish()
		curMode = coord.GetMode()
		coord.Reset()
	}

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

func ParseTestConf(tc string) {
	f, err := os.OpenFile(tc, os.O_RDONLY, 0600)
	if err != nil {
		clog.Error("Open File Error %s\n", err.Error())
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	var data []byte
	var splits []string

	data, _, err = reader.ReadLine()
	splits = strings.Split(string(data), " ")
	tests = make([]int, len(splits))
	for i, str := range splits {
		tests[i], err = strconv.Atoi(str)
		if err != nil {
			clog.Error("ParseError %v\n", err.Error())
		}
	}
}

func ParseReportConf(rc string) {
	f, err := os.OpenFile(rc, os.O_RDONLY, 0600)
	if err != nil {
		clog.Error("Open File Error %s\n", err.Error())
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	var data []byte
	var splits []string
	var head []byte

	// Read CrossRate
	head, _, err = reader.ReadLine()
	if strings.Compare(string(head), CROSSRATE) != 0 {
		clog.Error("Header %v Not Right\n", string(head))
	}

	data, _, err = reader.ReadLine()
	splits = strings.Split(string(data), ":")
	cr = make([]float64, len(splits))
	for i, str := range splits {
		cr[i], err = strconv.ParseFloat(str, 64)
		if err != nil {
			clog.Error("ParseError %v\n", err.Error())
		}
	}

	// Read MAXPARTITION
	head, _, err = reader.ReadLine()
	if strings.Compare(string(head), MAXPARTITION) != 0 {
		clog.Error("Header %v Not Right\n", string(head))
	}

	data, _, err = reader.ReadLine()
	splits = strings.Split(string(data), ":")
	mp = make([]int, len(splits))
	for i, str := range splits {
		mp[i], err = strconv.Atoi(str)
		if err != nil {
			clog.Error("ParseError %v\n", err.Error())
		}
	}

	// Read PARTSKEW
	head, _, err = reader.ReadLine()
	if strings.Compare(string(head), PARTSKEW) != 0 {
		clog.Error("Header %v Not Right\n", string(head))
	}

	data, _, err = reader.ReadLine()
	splits = strings.Split(string(data), ":")
	ps = make([]float64, len(splits))
	for i, str := range splits {
		ps[i], err = strconv.ParseFloat(str, 64)
		if err != nil {
			clog.Error("ParseError %v\n", err.Error())
		}
	}

	// Read CONTENTION
	head, _, err = reader.ReadLine()
	if strings.Compare(string(head), CONTENTION) != 0 {
		clog.Error("Header %v Not Right\n", string(head))
	}

	data, _, err = reader.ReadLine()
	splits = strings.Split(string(data), ":")
	contention = make([]float64, len(splits))
	for i, str := range splits {
		contention[i], err = strconv.ParseFloat(str, 64)
		if err != nil {
			clog.Error("ParseError %v\n", err.Error())
		}
	}

	// Read TRANLEN
	head, _, err = reader.ReadLine()
	if strings.Compare(string(head), TRANLEN) != 0 {
		clog.Error("Header %v Not Right\n", string(head))
	}

	data, _, err = reader.ReadLine()
	splits = strings.Split(string(data), ":")
	tlen = make([]int, len(splits))
	for i, str := range splits {
		tlen[i], err = strconv.Atoi(str)
		if err != nil {
			clog.Error("ParseError %v\n", err.Error())
		}
	}

	// Read READRATE
	head, _, err = reader.ReadLine()
	if strings.Compare(string(head), READRATE) != 0 {
		clog.Error("Header %v Not Right\n", string(head))
	}

	data, _, err = reader.ReadLine()
	splits = strings.Split(string(data), ":")
	rr = make([]int, len(splits))
	for i, str := range splits {
		rr[i], err = strconv.Atoi(str)
		if err != nil {
			clog.Error("ParseError %v\n", err.Error())
		}
	}
}
