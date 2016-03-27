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
var wl = flag.String("wl", "", "workload to be used")
var out = flag.String("out", "data.out", "output file path")
var ro = flag.String("ro", "report.out", "report out")
var tc = flag.String("tc", "test.conf", "Test Configuration")
var prof = flag.Bool("prof", false, "whether perform CPU profile")
var sr = flag.Int("sr", 100, "Sample Rate")
var rc = flag.String("rc", "report.conf", "configuration for reporting")
var isPart = flag.Bool("p", true, "Whether partition index")

var cr []float64
var ps []float64
var contention []float64
var transper []string
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
		f, err := os.Create("smallbank.prof")
		if err != nil {
			clog.Error(err.Error())
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	clog.Info("Number of workers %v \n", nWorkers)

	ParseTestConf(*tc)
	ParseReportConf(*rc)
	keyGenPool := make(map[float64][][]testbed.KeyGen)
	partGenPool := make(map[float64][]testbed.PartGen)
	var sb *testbed.SBWorkload = nil
	var coord *testbed.Coordinator = nil

	for k := 0; k < len(tests); k++ {
		d := tests[k]
		r := d % len(cr)
		tmpCR := cr[r]
		d = d / len(cr)

		r = d % len(ps)
		tmpPS := ps[r]
		d = d / len(ps)

		r = d % len(contention)
		tmpContention := contention[r]
		d = d / len(contention)

		tmpTP := transper[d]

		if sb == nil {
			clog.Info("Populating Whole Store\n")
			sb = testbed.NewSmallBankWL(*wl, nParts, isPartition, isPhysical, nWorkers, tmpContention, tmpTP, tmpCR, tmpPS)
			coord = testbed.NewCoordinator(nWorkers, sb.GetStore(), sb.GetTableCount(), testbed.PARTITION, *sr, sb.GetIDToKeyRange(), len(tests), *nsecs, testbed.SMALLBANKWL)

			// Populate Key Gen and Part Gen
			clog.Info("Populating Key Generators and Part Generators\n")
			basic := sb.GetBasicWL()
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
			basic := sb.GetBasicWL()
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
			sb.ResetConf(tmpTP, tmpCR)
		}

		clog.Info("CR %v PS %v Contention %v TransPer %v \n", tmpCR, tmpPS, tmpContention, tmpTP)

		ts := testbed.TID(0)
		var wg sync.WaitGroup
		coord.SetMode(curMode)
		coord.Start()
		for i := 0; i < nWorkers; i++ {
			wg.Add(1)
			go func(n int) {
				var t testbed.Trans
				w := coord.Workers[n]
				gen := sb.GetTransGen(n)
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

	// Read TRANSPER
	head, _, err = reader.ReadLine()
	if strings.Compare(string(head), TRANSPER) != 0 {
		clog.Error("Header %v Not Right\n", string(head))
	}

	var count int
	data, _, err = reader.ReadLine()
	if err != nil {
		clog.Error("Read String Error: %v \n", err.Error())
	}
	count, err = strconv.Atoi(string(data))
	if err != nil {
		clog.Error("Parse String Error: %v %s \n", err.Error())
	}

	transper = make([]string, count)
	tmpPer := make([]int, testbed.SBTRANSNUM)
	for i := 0; i < count; i++ {
		data, _, err = reader.ReadLine()
		if err != nil {
			clog.Error("Read String Error: %v \n", err.Error())
		}

		tp := strings.Split(string(data), ":")
		if len(tp) != testbed.SBTRANSNUM {
			clog.Error("Wrong format of transaction percentage string %s\n", string(data))
		}

		for i, str := range tp {
			per, err := strconv.Atoi(str)
			if err != nil {
				clog.Error("TransPercentage Format Error %s\n", str)
			}
			if i != 0 {
				tmpPer[i] = tmpPer[i-1] + per
			} else {
				tmpPer[i] = per
			}
		}

		if tmpPer[testbed.SBTRANSNUM-1] != 100 {
			clog.Error("Wrong format of transaction percentage string %s; Sum should be 100\n", string(data))
		}

		transper[i] = string(data)
	}

}
