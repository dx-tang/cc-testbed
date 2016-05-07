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
	CROSSRATE  = "CROSSRATE"
	PARTSKEW   = "PARTSKEW"
	CONTENTION = "CONTENTION"
	TRANSPER   = "TRANSPER"
	PERFDIFF   = 0.03
)

var nsecs = flag.Int("nsecs", 2, "number of seconds to run")
var wl = flag.String("wl", "../smallbank.txt", "workload to be used")
var out = flag.String("out", "data.out", "output file path")
var trainOut = flag.String("train", "train.out", "training set")
var prof = flag.Bool("prof", false, "whether perform CPU profile")
var sr = flag.Int("sr", 500, "Sample Rate")
var tc = flag.String("tc", "train.conf", "configuration for training")
var np = flag.Bool("np", false, "Whether test partition")
var prune = flag.Bool("prune", false, "Whether prune tests")
var isPart = flag.Bool("p", true, "Whether partition index")
var isTest = flag.Bool("test", false, "Whether test or train")
var dataDir = flag.String("dd", "../data", "TPCC Data Dir")

var cr []float64
var ps []float64
var contention []float64
var transper []string

const (
	TRIALS  = 3
	BUFSIZE = 3
)

var (
	lockInit = false
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
		clog.Error("Report not Needed for Training\n")
	}

	runtime.GOMAXPROCS(*testbed.NumPart)
	nWorkers := *testbed.NumPart
	testbed.WLTYPE = testbed.TPCCWL

	f, err := os.OpenFile(*trainOut, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		clog.Error("Open File Error %s\n", err.Error())
	}
	defer f.Close()

	nParts := nWorkers
	var isPartition bool
	if !*isPart && *np {
		nParts = 1
		isPartition = *isPart
	} else {
		isPartition = true
	}
	clog.Info("Number of workers %v \n", nWorkers)
	clog.Info("Adaptive CC Training\n")

	ParseTrainConf(*tc)
	keyGenPool := make(map[float64][][]testbed.KeyGen)
	partGenPool := make(map[float64][]testbed.KeyGen)
	var tpccWL *testbed.TPCCWorkload
	var coord *testbed.Coordinator = nil

	var ft [][]*testbed.Feature = make([][]*testbed.Feature, testbed.ADAPTIVE)
	for i := 0; i < testbed.ADAPTIVE; i++ {
		ft[i] = make([]*testbed.Feature, 3)
		for j := 0; j < 3; j++ {
			ft[i][j] = &testbed.Feature{}
		}
	}

	var totalTests int
	if !*isTest && !*np { // Training for Partition
		totalTests = len(ps) * len(contention) * len(transper)
	} else {
		totalTests = len(cr) * len(ps) * len(contention) * len(transper)
	}

	count := 0
	for k := 0; k < totalTests; k++ {
		startCR := 0
		endCR := 50
		curCR := 0

		d := k
		r := 0
		if *isTest || *np {
			r = d % len(cr)
			curCR = int(cr[r])
			d = d / len(cr)
		}

		r = d % len(ps)
		tmpPS := ps[r]
		d = d / len(ps)

		r = d % len(contention)
		tmpContention := contention[r]
		d = d / len(contention)

		tmpTP := transper[d]

		for {

			// Single Pruning
			tp := strings.Split(tmpTP, ":")
			if strings.Compare(tp[0], "0") == 0 && strings.Compare(tp[1], "0") == 0 && curCR > 0 {
				break
			}

			if tpccWL == nil {
				tpccWL = testbed.NewTPCCWL(*wl, nParts, isPartition, nWorkers, tmpContention, tmpTP, float64(curCR), tmpPS, *dataDir)
				coord = testbed.NewCoordinator(nWorkers, tpccWL.GetStore(), tpccWL.GetTableCount(), testbed.PARTITION, *sr, -1, -1, testbed.TPCCWL)
				if *prof {
					f, err := os.Create("smallbank.prof")
					if err != nil {
						clog.Error(err.Error())
					}
					pprof.StartCPUProfile(f)
					defer pprof.StopCPUProfile()
				}
			} else {
				keyGens, ok := keyGenPool[tmpContention]
				if !ok {
					keyGens = tpccWL.NewKeyGen(tmpContention)
					keyGenPool[tmpContention] = keyGens
				}

				partGens, ok1 := partGenPool[tmpPS]
				if !ok1 {
					partGens = tpccWL.NewPartGen(tmpPS)
					partGenPool[tmpPS] = partGens
				}
				tpccWL.SetKeyGens(keyGens)
				tpccWL.SetPartGens(partGens)
				tpccWL.ResetConf(tmpTP, float64(curCR), coord)
			}
			clog.Info("CR %v PS %v Contention %v TransPer %v \n", curCR, tmpPS, tmpContention, tmpTP)

			oneTest(tpccWL, coord, ft, nWorkers)

			for z := 0; z < testbed.ADAPTIVE; z++ {
				tmpFeature := ft[z]
				for x := 0; x < 2; x++ {
					tmp := tmpFeature[x]
					tmpI := x
					for y := x + 1; y < testbed.ADAPTIVE; y++ {
						if tmp.Txn < tmpFeature[y].Txn {
							tmp = tmpFeature[y]
							tmpI = y
						}
					}
					tmp = tmpFeature[x]
					tmpFeature[x] = tmpFeature[tmpI]
					tmpFeature[tmpI] = tmp
				}
			}

			for x := 0; x < 2; x++ {
				tmp := ft[x][1]
				tmpI := x
				for y := x + 1; y < testbed.ADAPTIVE; y++ {
					if tmp.Txn < ft[y][1].Txn {
						tmp = ft[y][1]
						tmpI = y
					}
				}
				tmp = ft[x][1]
				ft[x][1] = ft[tmpI][1]
				ft[tmpI][1] = tmp
			}

			if !*isTest {
				for z := 0; z < 9; z++ {
					x := z / 3
					y := z % 3
					if !(x == 0 && y == 1) {
						ft[0][1].Add(ft[x][y])
					}
				}

				if *np {
					ft[0][1].Avg(float64(6))
				} else {
					ft[0][1].Avg(float64(9))
				}
			}

			f.WriteString(fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t", count, curCR, tmpPS, tmpContention, tmpTP))
			if (ft[0][1].Txn-ft[1][1].Txn)/ft[0][1].Txn < PERFDIFF {
				//f.WriteString(fmt.Sprintf("%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%v\t%v\n", ft[0][1].PartAvg, ft[0][1].PartVar, ft[0][1].PartLenVar, ft[0][1].PartConf, ft[0][1].RecAvg, ft[0][1].HitRate, ft[0][1].ReadRate, ft[0][1].ConfRate, ft[0][1].Mode, ft[1][1].Mode))
				f.WriteString(fmt.Sprintf("%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%v\t%v\n", ft[0][1].PartConf, ft[0][1].PartVar, ft[0][1].RecAvg, ft[0][1].Latency, ft[0][1].ReadRate, ft[0][1].ConfRate, ft[0][1].Mode, ft[1][1].Mode))
			} else {
				//f.WriteString(fmt.Sprintf("%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%v\n", ft[0][1].PartAvg, ft[0][1].PartVar, ft[0][1].PartLenVar, ft[0][1].PartConf, ft[0][1].RecAvg, ft[0][1].HitRate, ft[0][1].ReadRate, ft[0][1].ConfRate, ft[0][1].Mode))
				f.WriteString(fmt.Sprintf("%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%v\n", ft[0][1].PartConf, ft[0][1].PartVar, ft[0][1].RecAvg, ft[0][1].Latency, ft[0][1].ReadRate, ft[0][1].ConfRate, ft[0][1].Mode))
			}

			win := ft[0][1].Mode

			// One Test Finished
			for _, features := range ft {
				for _, tmp := range features {
					tmp.Reset()
				}
			}
			count++

			if *isTest || *np || endCR-startCR <= 3 {
				break
			} else {
				if win == testbed.PARTITION {
					startCR = curCR
				} else {
					endCR = curCR
				}
				curCR = (startCR + endCR) / 2
			}
		}
	}
}

func oneTest(tpccWL *testbed.TPCCWorkload, coord *testbed.Coordinator, ft [][]*testbed.Feature, nWorkers int) {

	// One Test
	for a := 0; a < 3; a++ {
		for j := testbed.PARTITION; j < testbed.ADAPTIVE; j++ {

			if *np {
				if j == testbed.PARTITION {
					continue
				}
			}

			ts := testbed.TID(0)
			var wg sync.WaitGroup
			coord.SetMode(j)
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
					end_time := time.Now().Add(time.Duration(*nsecs) * time.Second)
					w.Start()
					for {
						tm := time.Now()
						if !end_time.After(tm) {
							break
						}
						if tq.IsFull() {
							t = tq.Dequeue()
						} else {
							t = gen.GenOneTrans(j)
							t.SetTrial(TRIALS)
							if *testbed.SysType == testbed.LOCKING && !*testbed.NoWait {
								tid := testbed.TID(atomic.AddUint64((*uint64)(&ts), 1))
								t.SetTID(tid)
							}
						}
						w.NGen += time.Since(tm)

						//tm = time.Now()
						_, err := w.One(t)
						//w.NExecute += time.Since(tm)

						if err != nil {
							if err == testbed.EABORT {
								t.DecTrial()
								if t.GetTrial() == 0 {
									gen.ReleaseOneTrans(t)
								} else {
									tq.Enqueue(t)
								}
							} else if err != testbed.EABORT {
								clog.Error("%s\n", err.Error())
							}
						} else {
							gen.ReleaseOneTrans(t)
						}
					}
					w.Finish()
					wg.Done()
				}(i)
			}
			wg.Wait()

			coord.Finish()

			if !lockInit {
				lockInit = true
			}

			if *np && j == testbed.PARTITION {
				coord.Reset()
				continue
			}

			tmpFt := coord.GetFeature()

			ft[j][a].Set(tmpFt)

			coord.Reset()
		}
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
	tmpPer := make([]int, testbed.TPCCTRANSNUM)
	for i := 0; i < count; i++ {
		data, _, err = reader.ReadLine()
		if err != nil {
			clog.Error("Read String Error: %v \n", err.Error())
		}

		tp := strings.Split(string(data), ":")
		if len(tp) != testbed.TPCCTRANSNUM {
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

		if tmpPer[testbed.TPCCTRANSNUM-1] != 100 {
			clog.Error("Wrong format of transaction percentage string %s; Sum should be 100\n", string(data))
		}

		transper[i] = string(data)
	}

}
