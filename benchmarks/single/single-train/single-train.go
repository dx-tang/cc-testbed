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
var wl = flag.String("wl", "../single.txt", "workload to be used")
var tp = flag.String("tp", "0:100", "Percetage of Each Transaction")
var out = flag.String("out", "data.out", "output file path")
var trainOut = flag.String("train", "train.out", "training set")
var prof = flag.Bool("prof", false, "whether perform CPU profile")
var sr = flag.Int("sr", 500, "Sample Rate")
var tc = flag.String("tc", "train.conf", "configuration for training")
var np = flag.Bool("np", false, "Whether not test partition")
var isPart = flag.Bool("p", true, "Whether partition index")
var isTest = flag.Bool("test", false, "Whether test or train")

var cr []float64
var mp []int
var ps []float64
var contention []float64
var tlen []int
var rr []int

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
	testbed.WLTYPE = testbed.SINGLEWL

	testbed.InitGlobalBuffer()

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
	partGenPool := make(map[float64][]testbed.PartGen)
	var single *testbed.SingelWorkload = nil
	var coord *testbed.Coordinator = nil
	//var curMode int

	var ft [][]*testbed.Feature = make([][]*testbed.Feature, testbed.ADAPTIVE)
	for i := 0; i < testbed.ADAPTIVE; i++ {
		ft[i] = make([]*testbed.Feature, 3)
		for j := 0; j < 3; j++ {
			ft[i][j] = &testbed.Feature{}
		}
	}

	var totalTests int
	if !*isTest && !*np { // Training for Partition
		totalTests = len(mp) * len(ps) * len(contention) * len(tlen) * len(rr)
	} else {
		totalTests = len(cr) * len(mp) * len(ps) * len(contention) * len(tlen) * len(rr)
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

		// Prune
		if tmpTlen == 1 {
			if curCR != 0 || tmpMP != 1 || tmpPS != 0 {
				continue
			}
		} else {
			if !*isTest && !*np {
				if tmpMP == 1 && tmpPS != 0 {
					continue
				}
			} else {
				if curCR == 0 || tmpMP == 1 {
					if curCR != 0 || tmpMP != 1 || tmpPS != 0 {
						continue
					}
				}
			}
		}

		if tmpMP > tmpTlen {
			continue
		}

		for {
			if single == nil {
				single = testbed.NewSingleWL(*wl, nParts, isPartition, nWorkers, tmpContention, *tp, float64(curCR), tmpTlen, tmpRR, tmpMP, tmpPS, testbed.PARTITION)
				coord = testbed.NewCoordinator(nWorkers, single.GetStore(), single.GetTableCount(), testbed.PARTITION, *sr, nil, -1, testbed.SINGLEWL, single)
			} else {
				basic := single.GetBasicWL()
				keyGens, ok := keyGenPool[tmpContention]
				if !ok {
					keyGens = basic.NewKeyGen(tmpContention)
					keyGenPool[tmpContention] = keyGens
				}
				basic.SetKeyGen(keyGens)

				if isPartition {
					single.ResetConf(*tp, float64(curCR), tmpMP, tmpTlen, tmpRR, tmpPS)
				} else {
					partGens, ok1 := partGenPool[tmpPS]
					if !ok1 {
						partGens = basic.NewPartGen(tmpPS)
						partGenPool[tmpPS] = partGens
					}
					basic.SetPartGen(partGens)
					single.ResetConf(*tp, float64(curCR), tmpMP, tmpTlen, tmpRR, testbed.NOPARTSKEW)
				}
			}
			clog.Info("CR %v MP %v PS %v Contention %v Tlen %v RR %v \n", curCR, tmpMP, tmpPS, tmpContention, tmpTlen, tmpRR)

			oneTest(single, coord, ft, nWorkers)

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

			// One Test Finished
			f.WriteString(fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t", count, curCR, tmpMP, tmpPS, tmpContention, tmpTlen, tmpRR))
			if (ft[0][1].Txn-ft[1][1].Txn)/ft[0][1].Txn < PERFDIFF {
				f.WriteString(fmt.Sprintf("%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%v\t%v\n", ft[0][1].PartConf, ft[0][1].PartVar, ft[0][1].RecAvg, ft[0][1].Latency, ft[0][1].ReadRate, ft[0][1].ConfRate, ft[0][1].Mode, ft[1][1].Mode))

			} else {
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

func oneTest(single *testbed.SingelWorkload, coord *testbed.Coordinator, ft [][]*testbed.Feature, nWorkers int) {
	for a := 0; a < 3; a++ {
		for j := testbed.PARTITION; j < testbed.ADAPTIVE; j++ {

			if *np {
				if j == testbed.PARTITION {
					continue
				}
			}

			if !*isPart && !*np {
				if j == testbed.PARTITION {
					single.ResetPart(nWorkers, true)
				} else {
					single.ResetPart(1, false)
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
					gen := single.GetTransGen(n)
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
							if t.GetTXN() != -1 {
								gen.ReleaseOneTrans(t)
							}
						}
					}
					w.Finish()
					for !tq.IsEmpty() {
						gen.ReleaseOneTrans(tq.Dequeue())
					}
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
