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
var trainOut = flag.String("train", "train.out", "training set")
var prof = flag.Bool("prof", false, "whether perform CPU profile")
var sr = flag.Int("sr", 100, "Sample Rate")
var tc = flag.String("tc", "train.conf", "configuration for training")

var cr []float64
var ps []float64
var contention []float64
var transper []string

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

	if strings.Compare(*tc, "") == 0 {
		clog.Error("Training Configuration not specified\n")
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

	f, err := os.OpenFile(*trainOut, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		clog.Error("Open File Error %s\n", err.Error())
	}
	defer f.Close()

	nParts := nWorkers
	*testbed.PhyPart = true
	isPartition := true
	clog.Info("Number of workers %v \n", nWorkers)
	clog.Info("Adaptive CC Training\n")

	ParseTrainConf(*tc)
	keyGenPool := make(map[float64][][]testbed.KeyGen)
	partGenPool := make(map[float64][]testbed.PartGen)
	var sb *testbed.SBWorkload = nil
	var coord *testbed.Coordinator = nil

	var ft []*testbed.Feature = make([]*testbed.Feature, testbed.ADAPTIVE)
	for i := 0; i < testbed.ADAPTIVE; i++ {
		ft[i] = &testbed.Feature{}
	}

	totalTests := len(cr) * len(ps) * len(contention) * len(transper)
	count := 0
	for k := 0; k < totalTests; k++ {
		d := k
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
			sb = testbed.NewSmallBankWL(*wl, nParts, isPartition, nWorkers, tmpContention, tmpTP, tmpCR, tmpPS)
			coord = testbed.NewCoordinator(nWorkers, sb.GetStore(), sb.GetTableCount(), testbed.PARTITION, "", *sr, sb.GetIDToKeyRange())
		} else {
			basic := sb.GetBasicWL()
			keyGens, ok := keyGenPool[tmpContention]
			if !ok {
				keyGens = basic.NewKeyGen(tmpContention)
				keyGenPool[tmpContention] = keyGens
			}

			partGens, ok1 := partGenPool[tmpPS]
			if !ok1 {
				partGens = basic.NewPartGen(tmpPS)
				partGenPool[tmpPS] = partGens
			}
			basic.SetKeyGen(keyGens)
			basic.SetPartGen(partGens)
			sb.ResetConf(tmpTP, tmpCR)
		}
		clog.Info("CR %v PS %v Contention %v TransPer %v \n", tmpCR, tmpPS, tmpContention, tmpTP)

		// One Test
		for j := testbed.PARTITION; j < testbed.ADAPTIVE; j++ {
			var wg sync.WaitGroup
			coord.SetMode(j)
			for i := 0; i < nWorkers; i++ {
				wg.Add(1)
				go func(n int) {
					var t testbed.Trans
					w := coord.Workers[n]
					gen := sb.GetTransGen(n)
					end_time := time.Now().Add(time.Duration(*nsecs) * time.Second)
					w.Start()
					for {
						tm := time.Now()
						if !end_time.After(tm) {
							break
						}

						t = gen.GenOneTrans()

						w.NGen += time.Since(tm)

						w.One(t)
					}
					w.Finish()
					wg.Done()
				}(i)
			}
			wg.Wait()

			coord.Finish()

			tmpFt := coord.GetFeature()
			for p := 0; p < testbed.ADAPTIVE; p++ {
				if ft[p].Txn < tmpFt.Txn {
					for q := testbed.ADAPTIVE - 2; q >= p; q-- {
						ft[q+1].Set(ft[q])
					}
					ft[p].Set(tmpFt)
					break
				}
			}

			coord.Reset()
		}

		f.WriteString(fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t", count, tmpCR, tmpPS, tmpContention, tmpTP))
		if (ft[0].Txn-ft[1].Txn)/ft[0].Txn < PERFDIFF {
			f.WriteString(fmt.Sprintf("%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%v\t%v\n", ft[0].PartAvg, ft[0].PartVar, ft[0].PartLenVar, ft[0].RecAvg, ft[0].PartVar, ft[0].ReadRate, ft[0].Mode, ft[1].Mode))
		} else {
			f.WriteString(fmt.Sprintf("%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%v\n", ft[0].PartAvg, ft[0].PartVar, ft[0].PartLenVar, ft[0].RecAvg, ft[0].PartVar, ft[0].ReadRate, ft[0].Mode))
		}

		// One Test Finished
		for _, feature := range ft {
			feature.Reset()
		}
		count++
	}
}

func ParseTrainConf(tc string) {
	f, err := os.OpenFile(tc, os.O_RDONLY, 0600)
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
		clog.Error("Parse String Error: %v \n", err.Error())
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
