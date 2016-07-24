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
var prof = flag.Bool("prof", false, "whether perform CPU profile")
var sr = flag.Int("sr", 500, "Sample Rate")
var tc = flag.String("tc", "train.conf", "configuration for training")
var trainMode = flag.Int("tm", 0, "Training Mode: 0 for Part; 1 for OCC-Part; 2 for OCC-NoPart; 3 for Index Merge/Partition; 4 for Testing")

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

	var occFile, lockFile, pccFile, occPureFile, lockPureFile *os.File

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

		occPureFile, err = os.OpenFile("occpure.out", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			clog.Error("Open File Error %s\n", err.Error())
		}

		lockPureFile, err = os.OpenFile("2plpure.out", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			clog.Error("Open File Error %s\n", err.Error())
		}

		defer occFile.Close()
		defer lockFile.Close()
		defer pccFile.Close()
		defer occPureFile.Close()
		defer lockPureFile.Close()
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
	clog.Info("Adaptive CC Training %v \n", tm)

	ParseTrainConf(*tc)
	keyGenPool := make(map[float64][][]testbed.KeyGen)
	partGenPool := make(map[float64][]testbed.PartGen)
	var single *testbed.SingelWorkload = nil
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
		totalTests = len(mp) * len(ps) * len(contention) * len(tlen) * len(rr)
	} else if tm == testbed.TRAININDEX {
		totalTests = len(cr) * len(mp) * len(contention) * len(tlen) * len(rr)
	} else {
		totalTests = len(cr) * len(mp) * len(ps) * len(contention) * len(tlen) * len(rr)
	}

	count := 0
	for k := 0; k < totalTests; k++ {
		startCR := 0
		endCR := 50
		curCR := 0

		startPS := float64(0)
		endPS := float64(0.3)
		curPS := float64(0)

		//startContention := float64(0)
		//endContention := float64(1.5)
		curContention := float64(0)

		d := k
		r := 0
		if tm != testbed.TRAINPART {
			r = d % len(cr)
			curCR = int(cr[r])
			d = d / len(cr)
		}

		r = d % len(mp)
		tmpMP := mp[r]
		d = d / len(mp)

		if tm != testbed.TRAININDEX {
			r = d % len(ps)
			curPS = ps[r]
			d = d / len(ps)
		}

		r = d % len(contention)
		curContention = contention[r]
		d = d / len(contention)

		r = d % len(tlen)
		tmpTlen := tlen[r]
		d = d / len(tlen)

		tmpRR := rr[d]

		// Prune
		if tmpTlen == 1 {
			if curCR != 0 || tmpMP != 1 || curPS != 0 {
				continue
			}
		}

		if tmpMP > tmpTlen {
			continue
		}

		if tmpMP == 1 && curCR > 0 {
			continue
		}

		for {
			if single == nil {
				single = testbed.NewSingleWL(*wl, nParts, isPartition, nWorkers, curContention, *tp, float64(curCR), tmpTlen, tmpRR, tmpMP, curPS, testbed.PARTITION, double)
				coord = testbed.NewCoordinator(nWorkers, single.GetStore(), single.GetTableCount(), testbed.PARTITION, *sr, nil, -1, testbed.SINGLEWL, single)

				single.SetWorkers(coord)
			} else {
				basic := single.GetBasicWL()
				keyGens, ok := keyGenPool[curContention]
				if !ok {
					keyGens = basic.NewKeyGen(curContention)
					keyGenPool[curContention] = keyGens
				}
				basic.SetKeyGen(keyGens)

				if isPartition {
					single.ResetConf(*tp, float64(curCR), tmpMP, tmpTlen, tmpRR, curPS)
				} else {
					partGens, ok1 := partGenPool[curPS]
					if !ok1 {
						partGens = basic.NewPartGen(curPS)
						partGenPool[curPS] = partGens
					}
					basic.SetPartGen(partGens)
					single.ResetConf(*tp, float64(curCR), tmpMP, tmpTlen, tmpRR, testbed.NOPARTSKEW)
				}
			}
			clog.Info("CR %v MP %v PS %v Contention %v Tlen %v RR %v \n", curCR, tmpMP, curPS, curContention, tmpTlen, tmpRR)

			typeAR = typeAR[0:0]

			oneTest(single, coord, ft, nWorkers, tm, curPS, partGenPool)

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

			if outDetail {
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
					} else if outIndex == 2 {
						outFile = lockFile
					} else if outIndex == 3 {
						outFile = occPureFile
					} else {
						outFile = lockPureFile
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
				} else { // TRAININDEX
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
			if ft[0][1].Txn > ft[testbed.OCCSHARE][1].Txn { // Use Partitioned Index
				typeNum := len(typeAR)
				typeAR = typeAR[:typeNum+1]
				typeAR[typeNum] = ft[0][1].TrainType
				if tm == testbed.TRAININDEX && (ft[0][1].Txn-ft[testbed.OCCSHARE][1].Txn)/ft[0][1].Txn < PERFDIFF {
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
				if tm == testbed.TRAININDEX && (ft[testbed.OCCSHARE][1].Txn-ft[0][1].Txn)/ft[testbed.OCCSHARE][1].Txn < PERFDIFF {
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
				fPart.WriteString(fmt.Sprintf("%v\t%v\t%v\t%.4f\t%v\t%v\t%v\t", count, curCR, tmpMP, curPS, curContention, tmpTlen, tmpRR))
				fPart.WriteString(fmt.Sprintf("%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f", ft[0][1].PartConf, ft[0][1].PartVar, ft[0][1].RecAvg, ft[0][1].Latency, ft[0][1].ReadRate, ft[0][1].HomeConfRate, ft[0][1].ConfRate))
				for _, trainType := range typeAR {
					fPart.WriteString(fmt.Sprintf("\t%v", trainType))
				}
				fPart.WriteString("\n")
			} else if tm == testbed.TRAINOCCPURE {
				fMerge.WriteString(fmt.Sprintf("%v\t%v\t%v\t%.4f\t%v\t%v\t%v\t", count, curCR, tmpMP, curPS, curContention, tmpTlen, tmpRR))
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
				fWhole.WriteString(fmt.Sprintf("%v\t%v\t%v\t%.4f\t%v\t%v\t%v\t", count, curCR, tmpMP, curPS, curContention, tmpTlen, tmpRR))
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

func oneTest(single *testbed.SingelWorkload, coord *testbed.Coordinator, ft [][]*testbed.Feature, nWorkers int, tm int, curPS float64, partGenPool map[float64][]testbed.PartGen) {
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
					single.Switch(*testbed.NumPart, false, testbed.NOPARTSKEW)
					basic := single.GetBasicWL()
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
					gen := single.GetTransGen(n)
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

						//tm = time.Now()
						_, err := w.One(t)
						//w.NExecute += time.Since(tm)

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
			single.Switch(*testbed.NumPart, true, curPS)

			basic := single.GetBasicWL()
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
