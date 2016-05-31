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
var prof = flag.Bool("prof", false, "whether perform CPU profile")
var sr = flag.Int("sr", 500, "Sample Rate")
var tc = flag.String("tc", "train.conf", "configuration for training")
var trainMode = flag.Int("tm", 0, "Training Mode: 0 for Part; 1 for OCC-Part; 2 for OCC-NoPart; 3 for Index Merge/Partition; 4 for Testing")

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

	if strings.Compare(*tc, "") == 0 {
		clog.Error("Training Configuration not specified\n")
	}

	if *testbed.Report {
		clog.Error("Report not Needed for Training\n")
	}

	runtime.GOMAXPROCS(*testbed.NumPart)
	nWorkers := *testbed.NumPart
	testbed.WLTYPE = testbed.SMALLBANKWL

	testbed.InitGlobalBuffer()

	if *prof {
		f, err := os.Create("smallbank.prof")
		if err != nil {
			clog.Error(err.Error())
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var fPart, fMerge, fWhole *os.File
	var err error

	tm := *trainMode

	if tm == testbed.TRAINPART || tm == testbed.TRAINOCCPART {
		fPart, err = os.OpenFile("train-part.out", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			clog.Error("Open File Error %s\n", err.Error())
		}
		defer fPart.Close()
	} else if tm == testbed.TRAINOCCPURE {
		fMerge, err = os.OpenFile("train-merge.out", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			clog.Error("Open File Error %s\n", err.Error())
		}
	} else {
		fWhole, err = os.OpenFile("train-whole.out", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			clog.Error("Open File Error %s\n", err.Error())
		}
	}

	outDetail := false

	var occFile, lockFile, pccFile *os.File

	if outDetail {
		occFile, err = os.OpenFile("occ.out", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			clog.Error("Open File Error %s\n", err.Error())
		}

		lockFile, err = os.OpenFile("2pl.out", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			clog.Error("Open File Error %s\n", err.Error())
		}

		pccFile, err = os.OpenFile("pcc.out", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			clog.Error("Open File Error %s\n", err.Error())
		}

		defer occFile.Close()
		defer lockFile.Close()
		defer pccFile.Close()
	}

	if tm < testbed.TRAINPART || tm > testbed.TESTING {
		clog.Error("Training Mode %v Error: 0 for Part; 1 for OCC-Part; 2 for OCC-NoPart; 3 for Index Merge/Partition; 4 for Testing", tm)
	}

	nParts := nWorkers
	var isPartition bool
	if tm == testbed.TRAINOCCPURE {
		nParts = 1
		isPartition = false
	} else {
		isPartition = true
	}

	var double bool
	if tm >= testbed.TRAININDEX {
		double = true
		clog.Info("Using Double Tables")
	} else {
		double = false
	}

	typeAR := make([]int, 0, testbed.LOCKSHARE+1)

	clog.Info("Number of workers %v \n", nWorkers)
	clog.Info("Adaptive CC Training\n")

	ParseTrainConf(*tc)
	keyGenPool := make(map[float64][][]testbed.KeyGen)
	partGenPool := make(map[float64][]testbed.PartGen)
	var sb *testbed.SBWorkload = nil
	var coord *testbed.Coordinator = nil

	var ft [][]*testbed.Feature = make([][]*testbed.Feature, testbed.TOTALCC)
	for i := 0; i < testbed.TOTALCC; i++ {
		ft[i] = make([]*testbed.Feature, 3)
		for j := 0; j < 3; j++ {
			ft[i][j] = &testbed.Feature{}
		}
	}

	var totalTests int
	if tm == testbed.TRAINPART { // Training for Partition
		totalTests = len(ps) * len(contention) * len(transper)
	} else if tm == testbed.TRAININDEX {
		totalTests = len(cr) * len(contention) * len(transper)
	} else {
		totalTests = len(cr) * len(ps) * len(contention) * len(transper)
	}

	count := 0
	for k := 0; k < totalTests; k++ {
		startCR := 0
		endCR := 50
		curCR := 0

		startPS := float64(0)
		endPS := float64(0.3)
		curPS := float64(0)

		d := k
		r := 0
		if tm != testbed.TRAINPART {
			r = d % len(cr)
			curCR = int(cr[r])
			d = d / len(cr)
		}

		if tm != testbed.TRAININDEX {
			r = d % len(ps)
			curPS = ps[r]
			d = d / len(ps)
		}

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

			var sbTranPer [testbed.SBTRANSNUM]int
			if len(tp) != testbed.SBTRANSNUM {
				clog.Error("Wrong format of transaction percentage string %s\n", tp)
			}
			for i, str := range tp {
				per, err := strconv.Atoi(str)
				if err != nil {
					clog.Error("TransPercentage Format Error %s\n", str)
				}
				if i != 0 {
					sbTranPer[i] = sbTranPer[i-1] + per
				} else {
					sbTranPer[i] = per
				}
			}

			if sbTranPer[testbed.SBTRANSNUM-1] != 100 {
				clog.Error("Wrong format of transaction percentage string %s; Sum should be 100\n", tp)
			}

			if sb == nil {
				sb = testbed.NewSmallBankWL(*wl, nParts, isPartition, nWorkers, tmpContention, sbTranPer, float64(curCR), curPS, testbed.PARTITION, double)
				coord = testbed.NewCoordinator(nWorkers, sb.GetStore(), sb.GetTableCount(), testbed.PARTITION, *sr, nil, -1, testbed.SMALLBANKWL, sb)
			} else {
				basic := sb.GetBasicWL()
				keyGens, ok := keyGenPool[tmpContention]
				if !ok {
					keyGens = basic.NewKeyGen(tmpContention)
					keyGenPool[tmpContention] = keyGens
				}

				basic.SetKeyGen(keyGens)

				if isPartition {
					sb.ResetConf(tmpTP, float64(curCR), curPS)
				} else {
					partGens, ok1 := partGenPool[curPS]
					if !ok1 {
						partGens = basic.NewPartGen(curPS)
						partGenPool[curPS] = partGens
					}
					basic.SetPartGen(partGens)
					sb.ResetConf(tmpTP, float64(curCR), testbed.NOPARTSKEW)
				}
			}
			clog.Info("CR %v PS %v Contention %v TransPer %v \n", curCR, curPS, tmpContention, tmpTP)

			typeAR = typeAR[0:0]

			oneTest(sb, coord, ft, nWorkers, tm, curPS, partGenPool)

			for z := 0; z < testbed.TOTALCC; z++ { // Find the middle one
				tmpFeature := ft[z]
				for x := 0; x < 2; x++ { // Insert Sorting
					tmp := tmpFeature[x]
					tmpI := x
					for y := x + 1; y < 3; y++ {
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

			if outDetail && tm != testbed.TESTING && tm != testbed.TRAININDEX {
				outF := &testbed.Feature{}
				for i := 0; i < testbed.TOTALCC; i++ {
					outF.Reset()

					outIndex := i

					if tm == testbed.TRAINOCCPART {
						if i < testbed.OCCPART || i > testbed.LOCKPART {
							continue
						}
					} else if tm == testbed.TRAINOCCPURE {
						if i < testbed.OCCSHARE || i > testbed.LOCKSHARE {
							continue
						}
						outIndex = i - 2
					} else if tm == testbed.TRAINPART {
						if i > testbed.LOCKPART {
							continue
						}
					}

					for _, f := range ft[i] {
						outF.Add(f)
					}
					outF.Avg(3)
					var outFile *os.File
					if outIndex == 0 {
						outFile = pccFile
					} else if outIndex == 1 {
						outFile = occFile
					} else {
						outFile = lockFile
					}
					outFile.WriteString(fmt.Sprintf("%v\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\n", curCR, ft[i][1].Txn, ft[i][1].AR, outF.HomeConfRate, outF.ConfRate, outF.Latency, outF.PartConf))
				}
			}

			// Sort the first three and last two respectively
			for x := testbed.PCC; x < testbed.LOCKPART; x++ { // Find the best one
				tmp := ft[x][1]
				tmpI := x
				for y := x + 1; y <= testbed.LOCKPART; y++ {
					if tmp.Txn < ft[y][1].Txn {
						tmp = ft[y][1]
						tmpI = y
					}
				}
				tmp = ft[x][1]
				ft[x][1] = ft[tmpI][1]
				ft[tmpI][1] = tmp
			}

			for x := testbed.OCCSHARE; x < testbed.LOCKSHARE; x++ { // Find the best one
				tmp := ft[x][1]
				tmpI := x
				for y := x + 1; y <= testbed.LOCKSHARE; y++ {
					if tmp.Txn < ft[y][1].Txn {
						tmp = ft[y][1]
						tmpI = y
					}
				}
				tmp = ft[x][1]
				ft[x][1] = ft[tmpI][1]
				ft[tmpI][1] = tmp
			}

			if tm != testbed.TESTING {
				for z := 0; z < 3*(testbed.LOCKPART+1); z++ {
					x := z / 3
					y := z % 3
					if !(x == 0 && y == 1) {
						ft[0][1].Add(ft[x][y])
					}
				}

				for z := 3 * testbed.OCCSHARE; z < 3*(testbed.LOCKSHARE+1); z++ {
					x := z / 3
					y := z % 3
					if !(x == testbed.OCCSHARE && y == 1) {
						ft[testbed.OCCSHARE][1].Add(ft[x][y])
					}
				}

				if tm == testbed.TRAINOCCPART {
					ft[0][1].Avg(float64(6))
				} else if tm == testbed.TRAINPART {
					ft[0][1].Avg(float64(9))
				} else if tm == testbed.TRAINOCCPURE {
					ft[testbed.OCCSHARE][1].Avg(float64(6))
				} else {
					if ft[0][1].Txn > ft[testbed.OCCSHARE][1].Txn {
						ft[0][1].Add(ft[testbed.OCCSHARE][1])
						ft[0][1].Avg(15)
					} else {
						ft[testbed.OCCSHARE][1].Add(ft[0][1])
						ft[testbed.OCCSHARE][1].Avg(15)
					}
				}

			}

			// Find the train type with similar performance difference
			if ft[0][1].Txn > ft[testbed.OCCSHARE][1].Txn {
				typeNum := len(typeAR)
				typeAR = typeAR[:typeNum+1]
				typeAR[typeNum] = ft[0][1].TrainType
				if (ft[0][1].Txn-ft[testbed.OCCSHARE][1].Txn)/ft[0][1].Txn < PERFDIFF {
					typeNum := len(typeAR)
					typeAR = typeAR[:typeNum+1]
					typeAR[typeNum] = ft[testbed.OCCSHARE][1].TrainType
				} else if (ft[0][1].Txn-ft[1][1].Txn)/ft[0][1].Txn < PERFDIFF {
					typeNum := len(typeAR)
					typeAR = typeAR[:typeNum+1]
					typeAR[typeNum] = ft[1][1].TrainType
				}
			} else {
				typeNum := len(typeAR)
				typeAR = typeAR[:typeNum+1]
				typeAR[typeNum] = ft[testbed.OCCSHARE][1].TrainType
				if (ft[testbed.OCCSHARE][1].Txn-ft[0][1].Txn)/ft[testbed.OCCSHARE][1].Txn < PERFDIFF {
					typeNum := len(typeAR)
					typeAR = typeAR[:typeNum+1]
					typeAR[typeNum] = ft[0][1].TrainType
				} else if (ft[testbed.OCCSHARE][1].Txn-ft[testbed.OCCSHARE+1][1].Txn)/ft[testbed.OCCSHARE][1].Txn < PERFDIFF {
					typeNum := len(typeAR)
					typeAR = typeAR[:typeNum+1]
					typeAR[typeNum] = ft[testbed.OCCSHARE+1][1].TrainType
				}
			}

			if tm == testbed.TRAINPART || tm == testbed.TRAINOCCPART {
				fPart.WriteString(fmt.Sprintf("%v\t%v\t%.4f\t%v\t%v\t", count, curCR, curPS, tmpContention, tmpTP))
				fPart.WriteString(fmt.Sprintf("%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f", ft[0][1].PartConf, ft[0][1].PartVar, ft[0][1].RecAvg, ft[0][1].Latency, ft[0][1].ReadRate, ft[0][1].HomeConfRate, ft[0][1].ConfRate))
				for _, trainType := range typeAR {
					fPart.WriteString(fmt.Sprintf("\t%v", trainType))
				}
				fPart.WriteString("\n")
			} else if tm == testbed.TRAINOCCPURE {
				fMerge.WriteString(fmt.Sprintf("%v\t%v\t%.4f\t%v\t%v\t", count, curCR, curPS, tmpContention, tmpTP))
				fMerge.WriteString(fmt.Sprintf("%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f", ft[testbed.OCCSHARE][1].PartConf, ft[testbed.OCCSHARE][1].PartVar, ft[testbed.OCCSHARE][1].RecAvg, ft[testbed.OCCSHARE][1].Latency, ft[testbed.OCCSHARE][1].ReadRate, ft[testbed.OCCSHARE][1].HomeConfRate, ft[testbed.OCCSHARE][1].ConfRate))
				for _, trainType := range typeAR {
					fMerge.WriteString(fmt.Sprintf("\t%v", trainType))
				}
				fMerge.WriteString("\n")
			} else {
				var tmpFeature *testbed.Feature
				if ft[0][1].Txn > ft[testbed.OCCSHARE][1].Txn {
					tmpFeature = ft[0][1]
				} else {
					tmpFeature = ft[testbed.OCCSHARE][1]
				}
				fWhole.WriteString(fmt.Sprintf("%v\t%v\t%.4f\t%v\t%v\t", count, curCR, curPS, tmpContention, tmpTP))
				fWhole.WriteString(fmt.Sprintf("%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f", tmpFeature.PartConf, tmpFeature.PartVar, tmpFeature.RecAvg, tmpFeature.Latency, tmpFeature.ReadRate, tmpFeature.HomeConfRate, tmpFeature.ConfRate))
				for _, trainType := range typeAR {
					fWhole.WriteString(fmt.Sprintf("\t%v", trainType))
				}
				fWhole.WriteString("\n")
			}

			win := typeAR[0]

			// One Test Finished
			for _, features := range ft {
				for _, tmp := range features {
					tmp.Reset()
				}
			}
			count++

			clog.Info("\n")

			if tm == testbed.TRAINPART && endCR-startCR > 3 {
				if win == testbed.PCC {
					startCR = curCR
				} else {
					endCR = curCR
				}
				curCR = (startCR + endCR) / 2
			} else if tm == testbed.TRAININDEX && endPS-startPS > 0.02 {
				if win <= testbed.LOCKPART { // Use Partition Index
					startPS = curPS
				} else {
					endPS = curPS
				}
				curPS = (startPS + endPS) / 2
			} else {
				break
			}
		}
	}
}

func oneTest(sb *testbed.SBWorkload, coord *testbed.Coordinator, ft [][]*testbed.Feature, nWorkers int, tm int, curPS float64, partGenPool map[float64][]testbed.PartGen) {

	// One Test
	for a := 0; a < 3; a++ {
		for j := 0; j < testbed.TOTALCC; j++ {

			ft[j][a].TrainType = j

			if tm == testbed.TRAINPART {
				if j > testbed.LOCKPART {
					continue
				}
			} else if tm == testbed.TRAINOCCPART {
				if j < testbed.OCCPART || j > testbed.LOCKPART {
					continue
				}
			} else if tm == testbed.TRAINOCCPURE {
				if j < testbed.OCCSHARE || j > testbed.LOCKSHARE {
					continue
				}
			}

			curMode := j
			if j == testbed.OCCSHARE {
				curMode = testbed.OCC
			} else if j == testbed.LOCKSHARE {
				curMode = testbed.LOCKING
			}

			if tm == testbed.TRAININDEX || tm == testbed.TESTING {
				if j == testbed.OCCSHARE {
					sb.Switch(*testbed.NumPart, false, testbed.NOPARTSKEW)
					basic := sb.GetBasicWL()
					partGens, ok1 := partGenPool[curPS]
					if !ok1 {
						partGens = basic.NewPartGen(curPS)
						partGenPool[curPS] = partGens
					}
					basic.SetPartGen(partGens)
					coord.ResetPart(false)
				}
			}

			ts := testbed.TID(0)
			var wg sync.WaitGroup
			coord.SetMode(curMode)
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
					end_time := time.Now().Add(time.Duration(*nsecs) * time.Second)
					w.NTotal += time.Duration(*nsecs) * time.Second
					w.Start()
					for {
						tm := time.Now()
						if !end_time.After(tm) {
							break
						}

						t = tq.Executable()
						if t == nil {
							t = gen.GenOneTrans(j)
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

			tmpFt := coord.GetFeature()

			ft[j][a].Set(tmpFt)

			coord.Reset()
		}
		if tm == testbed.TRAININDEX || tm == testbed.TESTING {
			sb.Switch(*testbed.NumPart, true, curPS)

			basic := sb.GetBasicWL()
			partGens, ok1 := partGenPool[testbed.NOPARTSKEW]
			if !ok1 {
				partGens = basic.NewPartGen(testbed.NOPARTSKEW)
				partGenPool[testbed.NOPARTSKEW] = partGens
			}
			basic.SetPartGen(partGens)

			coord.ResetPart(true)
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
