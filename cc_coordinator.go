package testbed

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/totemtang/cc-testbed/classifier"
	"github.com/totemtang/cc-testbed/clog"
)

// Number of Loaders for Index Partitioning
// Number of Mergers for Index Merging
var (
	NLOADERS = flag.Int("loaders", 1, "Number of Loaders")
	NMERGERS = flag.Int("mergers", 1, "Number of Mergers")
)

type TestCase struct {
	padding1     [PADDING]byte
	CR           float64
	MP           int
	PS           float64
	Contention   float64
	Tlen         int
	RR           int
	SBTransper   [SBTRANSNUM]int
	TPCCTransPer [TPCCTRANSNUM]int
	padding2     [PADDING]byte
}

func parseTestFile(f string, workload int) ([]float64, []float64, []float64, []int, []int, []int, []string) {
	tf, err := os.OpenFile(f, os.O_RDONLY, 0600)
	if err != nil {
		clog.Error("Open File Error %s\n", err.Error())
	}
	defer tf.Close()

	reader := bufio.NewReader(tf)

	var data []byte
	var splits []string

	_, _, err = reader.ReadLine()
	if err != nil {
		clog.Error("Read Header Error %v", err.Error())
	}

	data, _, err = reader.ReadLine()
	if err != nil {
		clog.Error("Read Couter Error %v", err.Error())
	}

	count, _ := strconv.Atoi(string(data))
	cr := make([]float64, count)
	ps := make([]float64, count)
	contention := make([]float64, count)
	mp := make([]int, count)
	tlen := make([]int, count)
	rr := make([]int, count)
	transper := make([]string, count)

	for i := 0; i < count; i++ {
		data, _, err = reader.ReadLine()
		if err != nil {
			clog.Error("Read Line %v Error %v", i, err.Error())
		}
		splits = strings.Split(string(data), "\t")
		cr[i], _ = strconv.ParseFloat(splits[0], 64)
		ps[i], _ = strconv.ParseFloat(splits[1], 64)
		contention[i], _ = strconv.ParseFloat(splits[2], 64)
		if workload == SINGLEWL {
			mp[i], _ = strconv.Atoi(splits[3])
			tlen[i], _ = strconv.Atoi(splits[4])
			rr[i], _ = strconv.Atoi(splits[5])
		} else {
			transper[i] = splits[3]
		}
	}

	return cr, ps, contention, mp, tlen, rr, transper

}

func BuildTestCases(f string, workload int) []TestCase {
	cr, ps, contention, mp, tlen, rr, transper := parseTestFile(f, workload)
	testCases := make([]TestCase, len(cr))
	for i, _ := range testCases {
		tc := &testCases[i]
		tc.CR = cr[i]
		tc.Contention = contention[i]
		tc.PS = ps[i]
		if workload == SINGLEWL {
			tc.MP = mp[i]
			tc.Tlen = tlen[i]
			tc.RR = rr[i]
		} else if workload == SMALLBANKWL {
			tp := strings.Split(transper[i], ":")
			if len(tp) != SBTRANSNUM {
				clog.Error("Wrong format of transaction percentage string %s\n", transper[i])
			}
			for j, str := range tp {
				per, err := strconv.Atoi(str)
				if err != nil {
					clog.Error("TransPercentage Format Error %s\n", str)
				}
				if j != 0 {
					tc.SBTransper[j] = tc.SBTransper[j-1] + per
				} else {
					tc.SBTransper[j] = per
				}
			}

			if tc.SBTransper[SBTRANSNUM-1] != 100 {
				clog.Error("Wrong format of transaction percentage string %s; Sum should be 100\n", transper[i])
			}
		} else if workload == TPCCWL {
			tp := strings.Split(transper[i], ":")
			if len(tp) != TPCCTRANSNUM {
				clog.Error("Wrong format of transaction percentage string %s\n", transper[i])
			}
			for j, str := range tp {
				per, err := strconv.Atoi(str)
				if err != nil {
					clog.Error("TransPercentage Format Error %s\n", str)
				}
				if j != 0 {
					tc.TPCCTransPer[j] = tc.TPCCTransPer[j-1] + per
				} else {
					tc.TPCCTransPer[j] = per
				}
			}

			if tc.TPCCTransPer[TPCCTRANSNUM-1] != 100 {
				clog.Error("Wrong format of transaction percentage string %s; Sum should be 100\n", transper[i])
			}
		} else {
			clog.Error("Workload type %v Not Support", workload)
		}
	}

	return testCases
}

type Coordinator struct {
	padding0         [PADDING]byte
	Workers          []*Worker
	store            *Store
	NStats           []int64
	NGen             time.Duration
	NExecute         time.Duration
	NTotal           time.Duration
	NWait            time.Duration
	NLockAcquire     int64
	stat             *os.File
	mode             int
	feature          *Feature
	padding2         [PADDING]byte
	reports          []chan *ReportInfo
	changeACK        []chan bool
	indexStartACK    []chan bool
	indexActionACK   []chan bool
	indexDoneACK     []chan bool
	indexActions     []*IndexAction
	startLoader      int
	startMerger      int
	summary          *ReportInfo
	perTest          int
	TxnAR            []float64
	ModeAR           []int
	reportCount      int
	rc               int
	curTest          int
	clf              classifier.Classifier
	workload         int
	indexpart        bool
	isMerge          bool
	testCases        []TestCase
	tpccWL           *TPCCWorkload
	singleWL         *SingelWorkload
	sbWL             *SBWorkload
	keyGenPool       map[float64][][]KeyGen
	partGenPool      map[float64][]PartGen
	tpccPartPool     map[float64][]KeyGen
	indexChangeStart time.Time
	potentialType    int
	startWorker      int
	justReconfig     bool
	padding1         [PADDING]byte
}

const (
	PERSEC       = 1000000000
	PERMINISEC   = 1000
	REPORTPERIOD = 1000
)

func NewCoordinator(nWorkers int, store *Store, tableCount int, mode int, sampleRate int, testCases []TestCase, nsecs int, workload int, wl interface{}) *Coordinator {
	coordinator := &Coordinator{
		Workers:        make([]*Worker, nWorkers),
		store:          store,
		NStats:         make([]int64, LAST_STAT),
		mode:           mode,
		feature:        &Feature{},
		reports:        make([]chan *ReportInfo, nWorkers),
		changeACK:      make([]chan bool, nWorkers),
		indexStartACK:  make([]chan bool, nWorkers),
		indexActionACK: make([]chan bool, nWorkers),
		indexDoneACK:   make([]chan bool, nWorkers),
		indexActions:   make([]*IndexAction, nWorkers),
		startLoader:    nWorkers - *NLOADERS,
		startMerger:    nWorkers - *NMERGERS,
		summary:        NewReportInfo(*NumPart, tableCount),
		indexpart:      false,
		testCases:      testCases,
		curTest:        0,
		isMerge:        false,
		justReconfig:   false,
	}

	if *Report {
		coordinator.perTest = nsecs * PERMINISEC / REPORTPERIOD
		reportCount := len(testCases) * coordinator.perTest
		coordinator.reportCount = reportCount
		coordinator.TxnAR = make([]float64, reportCount+2*PADDINGINT64)
		coordinator.TxnAR = coordinator.TxnAR[PADDINGINT64 : reportCount+PADDINGINT64]
		coordinator.ModeAR = make([]int, reportCount+2*PADDINGINT)
		coordinator.ModeAR = coordinator.ModeAR[PADDINGINT : reportCount+PADDINGINT]

		coordinator.keyGenPool = make(map[float64][][]KeyGen)
		coordinator.partGenPool = make(map[float64][]PartGen)
		coordinator.tpccPartPool = make(map[float64][]KeyGen)
		coordinator.workload = workload
		if workload == SINGLEWL {
			if *SysType == ADAPTIVE {
				partFile := CLASSIFERPATH + "/" + SINGLEPARTTRAIN
				occFile := CLASSIFERPATH + "/" + SINGLEOCCTRAIN
				pureFile := CLASSIFERPATH + "/" + SINGLEPURETRAIN
				indexFile := CLASSIFERPATH + "/" + SINGLEINDEXTRAIN
				coordinator.clf = classifier.NewClassifier(CLASSIFERPATH, partFile, occFile, pureFile, indexFile, SINGLEWL)
			}
			coordinator.singleWL = wl.(*SingelWorkload)
			single := coordinator.singleWL
			basic := single.GetBasicWL()
			for i, _ := range testCases {
				tc := &testCases[i]
				keyGens, ok1 := coordinator.keyGenPool[tc.Contention]
				if !ok1 {
					keyGens = basic.NewKeyGen(tc.Contention)
					coordinator.keyGenPool[tc.Contention] = keyGens
				}

				partGens, ok2 := coordinator.partGenPool[tc.PS]
				if !ok2 {
					partGens = basic.NewPartGen(tc.PS)
					coordinator.partGenPool[tc.PS] = partGens
				}
			}
			partGens, ok := coordinator.partGenPool[NOPARTSKEW]
			if !ok {
				partGens = basic.NewPartGen(NOPARTSKEW)
				coordinator.partGenPool[NOPARTSKEW] = partGens
			}
		} else if workload == SMALLBANKWL {
			if *SysType == ADAPTIVE {
				partFile := CLASSIFERPATH + "/" + SBPARTTRAIN
				occFile := CLASSIFERPATH + "/" + SBOCCTRAIN
				pureFile := CLASSIFERPATH + "/" + SBPURETRAIN
				indexFile := CLASSIFERPATH + "/" + SBINDEXTRAIN
				coordinator.clf = classifier.NewClassifier(CLASSIFERPATH, partFile, occFile, pureFile, indexFile, SMALLBANKWL)
			}
			coordinator.sbWL = wl.(*SBWorkload)
			sb := coordinator.sbWL
			basic := sb.GetBasicWL()
			for i, _ := range testCases {
				tc := &testCases[i]
				keyGens, ok1 := coordinator.keyGenPool[tc.Contention]
				if !ok1 {
					keyGens = basic.NewKeyGen(tc.Contention)
					coordinator.keyGenPool[tc.Contention] = keyGens
				}

				partGens, ok2 := coordinator.partGenPool[tc.PS]
				if !ok2 {
					partGens = basic.NewPartGen(tc.PS)
					coordinator.partGenPool[tc.PS] = partGens
				}
			}
			partGens, ok := coordinator.partGenPool[NOPARTSKEW]
			if !ok {
				partGens = basic.NewPartGen(NOPARTSKEW)
				coordinator.partGenPool[NOPARTSKEW] = partGens
			}
		} else if workload == TPCCWL {
			if *SysType == ADAPTIVE {
				partFile := CLASSIFERPATH + "/" + TPCCPARTTRAIN
				occFile := CLASSIFERPATH + "/" + TPCCOCCTRAIN
				pureFile := CLASSIFERPATH + "/" + TPCCPURETRAIN
				indexFile := CLASSIFERPATH + "/" + TPCCINDEXTRAIN
				coordinator.clf = classifier.NewClassifier(CLASSIFERPATH, partFile, occFile, pureFile, indexFile, TPCCWL)
			}
			coordinator.tpccWL = wl.(*TPCCWorkload)
			tpccWL := coordinator.tpccWL
			for i, _ := range testCases {
				tc := &testCases[i]
				keyGens, ok1 := coordinator.keyGenPool[tc.Contention]
				if !ok1 {
					keyGens = tpccWL.NewKeyGen(tc.Contention)
					coordinator.keyGenPool[tc.Contention] = keyGens
				}

				partGens, ok2 := coordinator.tpccPartPool[tc.PS]
				if !ok2 {
					partGens = tpccWL.NewPartGen(tc.PS)
					coordinator.tpccPartPool[tc.PS] = partGens
				}
			}
			partGens, ok := coordinator.tpccPartPool[NOPARTSKEW]
			if !ok {
				partGens = tpccWL.NewPartGen(NOPARTSKEW)
				coordinator.tpccPartPool[NOPARTSKEW] = partGens
			}
		} else {
			clog.Error("Workload %v Not Supported", workload)
		}

		for i := range coordinator.Workers {
			coordinator.reports[i] = make(chan *ReportInfo, 1)
			coordinator.changeACK[i] = make(chan bool, 1)
			coordinator.indexStartACK[i] = make(chan bool, 1)
			coordinator.indexActionACK[i] = make(chan bool, 1)
			coordinator.indexDoneACK[i] = make(chan bool, 1)
			coordinator.indexActions[i] = &IndexAction{}
		}

	}

	for i := range coordinator.Workers {
		coordinator.Workers[i] = NewWorker(i, store, coordinator, tableCount, mode, sampleRate, workload)
	}

	return coordinator
}

func (coord *Coordinator) ResetPart(isPartition bool) {
	for _, w := range coord.Workers {
		w.st.reconf(isPartition)
	}
}

func (coord *Coordinator) process() {
	summary := coord.summary
	var ri *ReportInfo
	timeFile, err := os.OpenFile("timeFile.out", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		clog.Error("%v", err)
	}
	for {
		select {
		case ri = <-coord.reports[0]:

			setReport(ri, summary)

			for i := 1; i < len(coord.reports); i++ {
				ri = <-coord.reports[i]
				collectReport(ri, summary)
			}

			execTime := float64(*NumPart*REPORTPERIOD/PERMINISEC) - summary.genTime.Seconds()

			// Record Throughput and Mode
			coord.TxnAR[coord.rc] = float64(summary.txn-summary.aborts) / execTime
			//clog.Info("Summary %v; Exec Secs: %v", summary.txn, summary.execTime.Seconds())
			coord.ModeAR[coord.rc] = coord.mode

			clog.Info("Test %v Mode %v; Txn %.4f; Abort %.4f; Total %v; ExecTime %.4f", coord.rc, coord.ModeAR[coord.rc], coord.TxnAR[coord.rc], float64(summary.aborts)/float64(summary.txn), summary.txn, execTime)

			coord.rc++

			// Switch
			if *SysType == ADAPTIVE && coord.store.state == INDEX_NONE {
				if !coord.justReconfig {
					coord.predict(summary)
				} else {
					coord.justReconfig = false
				}
			}

			// Done
			if coord.rc == coord.reportCount {
				// Done with tests
				for i := 0; i < len(coord.Workers); i++ {
					coord.Workers[i].done <- true
				}
				return
			} else if coord.rc%coord.perTest == 0 {
				if *SysType != ADAPTIVE {
					clog.Info("False Switch")
					coord.switchCC(coord.mode)
				}

				coord.curTest++
				tc := &coord.testCases[coord.curTest]
				if coord.workload == SINGLEWL {
					single := coord.singleWL
					keyGens := coord.keyGenPool[tc.Contention]
					if single.basic.isPartition {
						partGens := coord.partGenPool[NOPARTSKEW]
						single.OnlineReconf(keyGens, partGens, tc.CR, tc.MP, tc.Tlen, tc.RR, tc.PS)
					} else {
						partGens := coord.partGenPool[tc.PS]
						single.OnlineReconf(keyGens, partGens, tc.CR, tc.MP, tc.Tlen, tc.RR, NOPARTSKEW)
					}
					clog.Info("CR %v MP %v PS %v Contention %v Tlen %v RR %v \n", tc.CR, tc.MP, tc.PS, tc.Contention, tc.Tlen, tc.RR)
				} else if coord.workload == SMALLBANKWL {
					sb := coord.sbWL
					keygens := coord.keyGenPool[tc.Contention]
					if sb.basic.isPartition {
						partGens := coord.partGenPool[NOPARTSKEW]
						sb.OnlineReconf(keygens, partGens, tc.CR, tc.SBTransper, tc.PS)
					} else {
						partGens := coord.partGenPool[tc.PS]
						sb.OnlineReconf(keygens, partGens, tc.CR, tc.SBTransper, NOPARTSKEW)
					}
					clog.Info("CR %v PS %v Contention %v TransPer %v \n", tc.CR, tc.PS, tc.Contention, tc.SBTransper)
				} else { // TPCCWL
					tpccWL := coord.tpccWL
					keygens := coord.keyGenPool[tc.Contention]
					if tpccWL.isPartition {
						partGens := coord.tpccPartPool[NOPARTSKEW]
						tpccWL.OnlineReconf(keygens, partGens, tc.CR, tc.TPCCTransPer, tc.PS)
					} else {
						partGens := coord.tpccPartPool[tc.PS]
						tpccWL.OnlineReconf(keygens, partGens, tc.CR, tc.TPCCTransPer, NOPARTSKEW)
					}
					clog.Info("CR %v PS %v Contention %v TransPer %v \n", tc.CR, tc.PS, tc.Contention, tc.TPCCTransPer)
				}
			}
		case <-coord.indexActionACK[coord.startWorker]:
			for i := coord.startWorker + 1; i < len(coord.indexActionACK); i++ {
				<-coord.indexActionACK[i]
			}
			// Now All Loaders done; Confirm this to all workers
			for i := 0; i < len(coord.Workers); i++ {
				coord.Workers[i].indexDone <- true
			}
			for i := 0; i < len(coord.indexDoneACK); i++ {
				<-coord.indexDoneACK[i]
			}

			coord.store.state = INDEX_NONE

			coord.justReconfig = true

			coord.store.secTables = coord.store.backTables

			timeFile.WriteString(fmt.Sprintf("%.4f\n", time.Since(coord.indexChangeStart).Seconds()))
			if coord.isMerge {
				clog.Info("Done with Index Merging: %.3f", time.Since(coord.indexChangeStart).Seconds())
			} else {
				clog.Info("Done with Index Partitioning: %.3f", time.Since(coord.indexChangeStart).Seconds())
			}

			if coord.workload == SINGLEWL {
				single := coord.singleWL
				if coord.isMerge {
					single.ResetPart(1, false)
					single.zp.Reconf(NOPARTSKEW)
					for i := 0; i < len(single.transGen); i++ {
						tg := single.transGen[i]
						tg.validProb = single.zp.GetProb(i)
						tg.timeInit = false
					}
					basic := single.GetBasicWL()
					partGens := basic.NewPartGen(coord.testCases[coord.curTest].PS)
					basic.SetPartGen(partGens)
					coord.ResetPart(false)
				} else {
					single.ResetPart(*NumPart, true)
					single.zp.Reconf(coord.testCases[coord.curTest].PS)
					for i := 0; i < len(single.transGen); i++ {
						tg := single.transGen[i]
						tg.validProb = single.zp.GetProb(i)
						tg.timeInit = false
					}
					basic := single.GetBasicWL()
					partGens := basic.NewPartGen(NOPARTSKEW)
					basic.SetPartGen(partGens)
					coord.ResetPart(true)
				}
			} else if coord.workload == SMALLBANKWL {
				sb := coord.sbWL
				if coord.isMerge {
					sb.ResetPart(1, false)
					sb.zp.Reconf(NOPARTSKEW)
					for i := 0; i < len(sb.transGen); i++ {
						tg := sb.transGen[i]
						tg.validProb = sb.zp.GetProb(i)
						tg.timeInit = false
					}
					basic := sb.GetBasicWL()
					partGens := basic.NewPartGen(coord.testCases[coord.curTest].PS)
					basic.SetPartGen(partGens)
					coord.ResetPart(false)
				} else {
					sb.ResetPart(*NumPart, true)
					sb.zp.Reconf(coord.testCases[coord.curTest].PS)
					for i := 0; i < len(sb.transGen); i++ {
						tg := sb.transGen[i]
						tg.validProb = sb.zp.GetProb(i)
						tg.timeInit = false
					}
					basic := sb.GetBasicWL()
					partGens := basic.NewPartGen(NOPARTSKEW)
					basic.SetPartGen(partGens)
					coord.ResetPart(true)
				}
			} else { // TPCCWL
				tpccWL := coord.tpccWL
				if coord.isMerge {
					tpccWL.ResetPart(1, false)
					tpccWL.zp.Reconf(NOPARTSKEW)
					for i := 0; i < len(tpccWL.transGen); i++ {
						tg := &tpccWL.transGen[i]
						tg.validProb = tpccWL.zp.GetProb(i)
						tg.timeInit = false
					}
					partGens := tpccWL.NewPartGen(coord.testCases[coord.curTest].PS)
					tpccWL.SetPartGens(partGens)
					coord.ResetPart(false)
				} else {
					tpccWL.ResetPart(*NumPart, true)
					tpccWL.zp.Reconf(coord.testCases[coord.curTest].PS)
					for i := 0; i < len(tpccWL.transGen); i++ {
						tg := &tpccWL.transGen[i]
						tg.validProb = tpccWL.zp.GetProb(i)
						tg.timeInit = false
					}
					partGens := tpccWL.NewPartGen(NOPARTSKEW)
					tpccWL.SetPartGens(partGens)
					coord.ResetPart(true)
				}
			}

			for i := 0; i < len(coord.Workers); i++ {
				coord.Workers[i].indexConfirm <- true
			}

			if !coord.isMerge && coord.potentialType == PARTITION {
				coord.switchCC(PARTITION)
			}
		}
	}
}

func (coord *Coordinator) predict(summary *ReportInfo) {
	// Compute Features
	txn := summary.txnSample

	var sum float64
	var sumpow float64

	var head int
	if coord.store.isPartition {
		head = 0
	} else {
		head = HEAD
	}

	for i, p := range summary.partStat {
		if i >= head {
			sum += float64(p)
		}
	}

	for i, p := range summary.partStat {
		if i >= head {
			sumpow += float64(p*p) / (sum * sum)
		}
	}

	n := float64(len(summary.partStat) - head)
	partVar := (sumpow/n - 1/(n*n)) * 1000

	var recAvg float64
	sum = float64(summary.readCount + summary.writeCount)
	recAvg = float64(sum) / float64(txn)

	rr := float64(summary.readCount) / float64(summary.readCount+summary.writeCount)

	if WLTYPE == TPCCWL {
		rr *= 10000
	}

	var confRate float64
	var homeConfRate float64

	for i, _ := range summary.homeConflicts {
		if summary.accessHomeCount[i] != 0 {
			homeConfRate += float64(summary.homeConflicts[i]*100) / float64(summary.accessHomeCount[i])
		}
	}
	if coord.store.isPartition {
		confRate = homeConfRate
	} else {
		for i, _ := range summary.conflicts {
			if summary.accessCount[i] != 0 {
				confRate += float64(summary.conflicts[i]*100) / float64(summary.accessCount[i])
			}
		}
	}

	latency := float64(summary.latency) / float64(summary.totalCount)

	if !coord.store.isPartition {
		if latency <= 350 {
			latency -= 80
		} else if latency <= 650 {
			if coord.mode == 1 {
				latency -= 80
			} else {
				latency -= 100
			}
		} else {
			if coord.mode == 1 {
				latency -= 100
			} else {
				latency -= 150
			}
		}
	}
	partVar = math.Sqrt(partVar)
	partConf := float64(summary.partAccess) / float64(summary.partSuccess)

	//clog.Info("%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\n", partAvg, partVar, partLenVar, recAvg, hitRate, rr, confRate)

	// Use Classifier to Predict Features
	//mode := coord.clf.Predict(partAvg, partVar, partLenVar, recAvg, hitRate, rr, confRate)
	curType := coord.mode
	if !coord.store.isPartition {
		curType += 2
	}
	execType := coord.clf.Predict(curType, partConf, partVar, recAvg, latency, rr, homeConfRate, confRate)
	clog.Info("Switching from %v to %v, Conf %.4f, Home %.4f, Latency %.4f, PConf %.4f, PVar %.4f\n", curType, execType, confRate, homeConfRate, latency, partConf, partVar)

	if execType > 2 { // Use Shared Index
		if coord.store.isPartition { // Start Merging
			coord.switchCC(execType - 2)
			coord.indexReorganize(true)
		} else {
			coord.switchCC(execType - 2)
		}
	} else { // Use Partitioned Index
		if !coord.store.isPartition { // Start Partitioning
			coord.indexReorganize(false)
			if execType != 0 {
				coord.switchCC(execType)
			}
			coord.potentialType = execType
		} else {
			coord.switchCC(execType)
		}
	}
}

func (coord *Coordinator) switchCC(mode int) {
	if *SysType == ADAPTIVE {
		if mode == coord.mode {
			return
		}
	}
	coord.mode = mode
	for i := 0; i < len(coord.Workers); i++ {
		coord.Workers[i].modeChange <- true
	}
	for i := 0; i < len(coord.Workers); i++ {
		<-coord.changeACK[i]
	}
	coord.store.SetMode(mode)
	for i := 0; i < len(coord.Workers); i++ {
		coord.Workers[i].modeChan <- coord.mode
	}
}

func (coord *Coordinator) indexReorganize(isMerge bool) {
	perWorker := 0
	residue := 0
	actionType := INDEX_ACTION_NONE
	coord.isMerge = isMerge
	if coord.isMerge {
		clog.Info("Starting Index Merging")
		actionType = INDEX_ACTION_MERGE
		coord.startWorker = coord.startMerger
		perWorker = *NumPart / *NMERGERS
		residue = *NumPart % *NMERGERS
	} else {
		clog.Info("Starting Index Partitioning")
		actionType = INDEX_ACTION_PARTITION
		coord.startWorker = coord.startLoader
		perWorker = *NumPart / *NLOADERS
		residue = *NumPart % *NLOADERS
	}
	coord.indexChangeStart = time.Now()
	store := coord.store
	// Begin Index Partitioning
	for i := 0; i < len(coord.Workers); i++ {
		coord.Workers[i].indexStart <- true
	}
	for i := 0; i < len(coord.indexStartACK); i++ {
		<-coord.indexStartACK[i]
	}
	// Get All ACK; All workers stop; Change State and switch pri/sec tables
	store.state = INDEX_CHANGING
	tmpTables := store.priTables
	store.priTables = store.secTables
	store.secTables = tmpTables
	store.isPartition = !store.isPartition

	for i := 0; i < len(coord.Workers); i++ {
		action := coord.indexActions[i]
		if i < coord.startWorker {
			action.actionType = INDEX_ACTION_NONE
		} else {
			iWorker := (i - coord.startWorker)
			action.actionType = actionType
			begin := iWorker * perWorker
			if iWorker < residue {
				begin += iWorker
			} else {
				begin += residue
			}
			end := begin + perWorker
			if iWorker < residue {
				end++
			}
			action.start = begin
			action.end = end
		}
		coord.Workers[i].indexAction <- action
	}
}

func setReport(ri *ReportInfo, summary *ReportInfo) {
	summary.execTime = ri.execTime
	summary.txn = ri.txn
	summary.aborts = ri.aborts
	summary.genTime = ri.genTime

	if *SysType == ADAPTIVE {
		summary.txnSample = ri.txnSample

		for i, ps := range ri.partStat {
			summary.partStat[i] = ps
		}

		for i, rs := range ri.recStat {
			summary.recStat[i] = rs
		}

		summary.readCount = ri.readCount
		summary.writeCount = ri.writeCount
		summary.totalCount = ri.totalCount

		for i, _ := range ri.accessCount {
			summary.accessCount[i] = ri.accessCount[i]
			summary.accessHomeCount[i] = ri.accessHomeCount[i]
			summary.conflicts[i] = ri.conflicts[i]
			summary.homeConflicts[i] = ri.homeConflicts[i]
		}

		summary.partAccess = ri.partAccess
		summary.partSuccess = ri.partSuccess

		summary.latency = ri.latency
	}
}

func collectReport(ri *ReportInfo, summary *ReportInfo) {
	summary.execTime += ri.execTime
	summary.txn += ri.txn
	summary.aborts += ri.aborts
	summary.genTime += ri.genTime

	if *SysType == ADAPTIVE {
		summary.txnSample += ri.txnSample

		for i, ps := range ri.partStat {
			summary.partStat[i] += ps
		}

		for i, rs := range ri.recStat {
			summary.recStat[i] += rs
		}

		summary.readCount += ri.readCount
		summary.writeCount += ri.writeCount
		summary.totalCount += ri.totalCount

		for i, _ := range ri.accessCount {
			summary.accessCount[i] += ri.accessCount[i]
			summary.accessHomeCount[i] += ri.accessHomeCount[i]
			summary.conflicts[i] += ri.conflicts[i]
			summary.homeConflicts[i] += ri.homeConflicts[i]
		}

		summary.partAccess += ri.partAccess
		summary.partSuccess += ri.partSuccess

		summary.latency += ri.latency
	}
}

func (coord *Coordinator) Start() {
	if *Report {
		go coord.process()
		for _, w := range coord.Workers {
			go w.run()
		}
	}
}

func (coord *Coordinator) Finish() {
	if !*Report {
		coord.gatherStats()
	}
}

func (coord *Coordinator) Final() {
	coord.clf.Finalize()
}

func (coord *Coordinator) Reset() {
	coord.NStats[NABORTS] = 0
	coord.NStats[NREADABORTS] = 0
	coord.NStats[NLOCKABORTS] = 0
	coord.NStats[NRCHANGEABORTS] = 0
	coord.NStats[NRWABORTS] = 0
	coord.NStats[NRLOCKABORTS] = 0
	coord.NStats[NWLOCKABORTS] = 0
	coord.NStats[NUPGRADEABORTS] = 0
	coord.NStats[NENOKEY] = 0
	coord.NStats[NTXN] = 0
	coord.NStats[NCROSSTXN] = 0
	coord.NGen = 0
	coord.NExecute = 0
	coord.NTotal = 0
	coord.NWait = 0
	coord.NLockAcquire = 0
	coord.mode = PARTITION
	coord.summary.Reset()

	for _, worker := range coord.Workers {
		worker.NStats[NABORTS] = 0
		worker.NStats[NREADABORTS] = 0
		worker.NStats[NLOCKABORTS] = 0
		worker.NStats[NRCHANGEABORTS] = 0
		worker.NStats[NRWABORTS] = 0
		worker.NStats[NRLOCKABORTS] = 0
		worker.NStats[NWLOCKABORTS] = 0
		worker.NStats[NUPGRADEABORTS] = 0
		worker.NStats[NENOKEY] = 0
		worker.NStats[NTXN] = 0
		worker.NStats[NCROSSTXN] = 0
		worker.NGen = 0
		worker.NExecute = 0
		worker.NTotal = 0
		worker.NWait = 0
		worker.NLockAcquire = 0
		worker.next = 0
		worker.epoch = 0
		worker.riMaster.Reset()
		worker.riReplica.Reset()
		worker.st.Reset()
		worker.finished = false
		worker.mode = PARTITION
	}
}

func (coord *Coordinator) SetMode(mode int) {
	coord.store.SetMode(mode)
	coord.mode = mode
	for _, w := range coord.Workers {
		w.SetMode(mode)
	}
}

func (coord *Coordinator) GetMode() int {
	return coord.mode
}

func (coord *Coordinator) gatherStats() {
	for _, worker := range coord.Workers {
		coord.NStats[NABORTS] += worker.NStats[NABORTS]
		coord.NStats[NREADABORTS] += worker.NStats[NREADABORTS]
		coord.NStats[NLOCKABORTS] += worker.NStats[NLOCKABORTS]
		coord.NStats[NRCHANGEABORTS] += worker.NStats[NRCHANGEABORTS]
		coord.NStats[NRWABORTS] += worker.NStats[NRWABORTS]
		coord.NStats[NRLOCKABORTS] += worker.NStats[NRLOCKABORTS]
		coord.NStats[NWLOCKABORTS] += worker.NStats[NWLOCKABORTS]
		coord.NStats[NUPGRADEABORTS] += worker.NStats[NUPGRADEABORTS]
		coord.NStats[NENOKEY] += worker.NStats[NENOKEY]
		coord.NStats[NTXN] += worker.NStats[NTXN]
		coord.NStats[NCROSSTXN] += worker.NStats[NCROSSTXN]
		coord.NGen += worker.NGen
		coord.NExecute += worker.NExecute
		coord.NTotal += worker.NTotal
		coord.NWait += worker.NWait
		coord.NLockAcquire += worker.NLockAcquire
	}
}

func (coord *Coordinator) PrintStats(f *os.File) {

	mode := coord.mode

	f.WriteString("================\n")
	f.WriteString("Print Statistics\n")
	f.WriteString("================\n")

	f.WriteString(fmt.Sprintf("Issue %v Transactions in Total\n", coord.NStats[NTXN]))
	f.WriteString(fmt.Sprintf("Transaction Generation Spends %v secs\n", float64(coord.NGen.Nanoseconds())/float64(PERSEC)))
	f.WriteString(fmt.Sprintf("Transaction Processing Spends %v secs\n", float64(coord.NTotal.Nanoseconds()-coord.NGen.Nanoseconds())/float64(PERSEC)))

	if *SysType == PARTITION || (*SysType == ADAPTIVE && mode == PARTITION) {
		f.WriteString(fmt.Sprintf("Cross Partition %v Transactions\n", coord.NStats[NCROSSTXN]))
		f.WriteString(fmt.Sprintf("Transaction Waiting Spends %v secs\n", float64(coord.NWait.Nanoseconds())/float64(PERSEC)))
		f.WriteString(fmt.Sprintf("Has Acquired %v Locks\n", coord.NLockAcquire))
		f.WriteString(fmt.Sprintf("Abort %v Transactions\n", coord.NStats[NABORTS]))
		r := ((float64)(coord.NStats[NABORTS]) / (float64)(coord.NStats[NTXN])) * 100
		f.WriteString(fmt.Sprintf("Abort Rate %.4f%% \n", r))

	} else if *SysType == OCC || (*SysType == ADAPTIVE && mode == OCC) {

		if coord.store.nParts > 1 {
			f.WriteString(fmt.Sprintf("Cross Partition %v Transactions\n", coord.NStats[NCROSSTXN]))
		}

		l := 100.0

		f.WriteString(fmt.Sprintf("Abort %v Transactions\n", coord.NStats[NABORTS]))

		r := ((float64)(coord.NStats[NABORTS]) / (float64)(coord.NStats[NTXN])) * 100
		f.WriteString(fmt.Sprintf("Abort Rate %.4f%% \n", r))

		r = ((float64)(coord.NStats[NREADABORTS]) / (float64)(coord.NStats[NABORTS])) * 100
		f.WriteString(fmt.Sprintf("Try Read Occupy %.4f%% Aborts \n", r))
		l -= r

		r = ((float64)(coord.NStats[NLOCKABORTS]) / (float64)(coord.NStats[NABORTS])) * 100
		f.WriteString(fmt.Sprintf("Try Lock Occupy %.4f%% Aborts \n", r))
		l -= r

		r = ((float64)(coord.NStats[NRCHANGEABORTS]) / (float64)(coord.NStats[NABORTS])) * 100
		f.WriteString(fmt.Sprintf("Read Dirty Data Occupy %.4f%% Aborts \n", r))
		l -= r

		r = ((float64)(coord.NStats[NRWABORTS]) / (float64)(coord.NStats[NABORTS])) * 100
		f.WriteString(fmt.Sprintf("Read Write Conflict Occupy %.4f%% Aborts \n", r))
		l -= r

		f.WriteString(fmt.Sprintf("Workload Occupy %.4f%% Aborts \n", l))

	} else if *SysType == LOCKING || (*SysType == ADAPTIVE && mode == LOCKING) {
		f.WriteString(fmt.Sprintf("Abort %v Transactions\n", coord.NStats[NABORTS]))
		r := ((float64)(coord.NStats[NABORTS]) / (float64)(coord.NStats[NTXN])) * 100
		f.WriteString(fmt.Sprintf("Abort Rate %.4f%% \n", r))

		l := 100.0

		r = ((float64)(coord.NStats[NRLOCKABORTS]) / (float64)(coord.NStats[NABORTS])) * 100
		f.WriteString(fmt.Sprintf("Read Lock Occupy %.4f%% Aborts \n", r))
		l -= r

		r = ((float64)(coord.NStats[NWLOCKABORTS]) / (float64)(coord.NStats[NABORTS])) * 100
		f.WriteString(fmt.Sprintf("Write Lock Occupy %.4f%% Aborts \n", r))
		l -= r

		r = ((float64)(coord.NStats[NUPGRADEABORTS]) / (float64)(coord.NStats[NABORTS])) * 100
		f.WriteString(fmt.Sprintf("Upgrade Occupy %.4f%% Aborts \n", r))
		l -= r

		f.WriteString(fmt.Sprintf("Workload Occupy %.4f%% Aborts \n", l))
	}

	f.WriteString("\n")

}

type Feature struct {
	padding1     [PADDING]byte
	PartAvg      float64
	PartVar      float64
	PartLenVar   float64
	PartConf     float64
	RecAvg       float64
	HitRate      float64
	Latency      float64
	ReadRate     float64
	HomeConfRate float64
	ConfRate     float64
	Txn          float64
	AR           float64
	Mode         int
	TrainType    int
	padding2     [PADDING]byte
}

func (f *Feature) Reset() {
	f.PartAvg = 0
	f.PartVar = 0
	f.PartLenVar = 0
	f.PartConf = 0
	f.RecAvg = 0
	//f.RecVar = 0
	f.HitRate = 0
	f.Latency = 0
	f.ReadRate = 0
	f.ConfRate = 0
	f.HomeConfRate = 0
	f.Txn = 0
	f.AR = 0
	f.Mode = 0
}

func (ft *Feature) Add(tmpFt *Feature) {
	ft.PartAvg += tmpFt.PartAvg
	ft.PartVar += tmpFt.PartVar
	ft.PartLenVar += tmpFt.PartLenVar
	ft.PartConf += tmpFt.PartConf
	ft.RecAvg += tmpFt.RecAvg
	//ft.RecVar += tmpFt.RecVar
	ft.HitRate += tmpFt.HitRate
	ft.Latency += tmpFt.Latency
	ft.ReadRate += tmpFt.ReadRate
	ft.ConfRate += tmpFt.ConfRate
	ft.HomeConfRate += tmpFt.HomeConfRate
	//ft.Txn += tmpFt.Txn
	//ft.AR += tmpFt.AR
}

func (ft *Feature) Set(tmpFt *Feature) {
	ft.PartAvg = tmpFt.PartAvg
	ft.PartVar = tmpFt.PartVar
	ft.PartLenVar = tmpFt.PartLenVar
	ft.PartConf = tmpFt.PartConf
	ft.RecAvg = tmpFt.RecAvg
	//ft.RecVar = tmpFt.RecVar
	ft.HitRate = tmpFt.HitRate
	ft.Latency = tmpFt.Latency
	ft.ReadRate = tmpFt.ReadRate
	ft.ConfRate = tmpFt.ConfRate
	ft.HomeConfRate = tmpFt.HomeConfRate
	ft.Txn = tmpFt.Txn
	ft.AR = tmpFt.AR
	ft.Mode = tmpFt.Mode
}

func (ft *Feature) Avg(count float64) {
	ft.PartAvg /= count
	ft.PartVar /= count
	ft.PartLenVar /= count
	ft.PartConf /= count
	ft.RecAvg /= count
	//ft.RecVar /= count
	ft.HitRate /= count
	ft.Latency /= count
	ft.ReadRate /= count
	ft.ConfRate /= count
	ft.HomeConfRate /= count
	//ft.Txn /= count
	//ft.AR /= count
}

// Currently, we support 6 features
func (coord *Coordinator) GetFeature() *Feature {
	summary := coord.summary
	for _, w := range coord.Workers {
		master := w.riMaster

		summary.txn += w.riMaster.txn
		summary.aborts += w.riMaster.aborts

		summary.txnSample += w.riMaster.txnSample

		for j, ps := range master.partStat {
			summary.partStat[j] += ps
		}

		summary.partTotal += master.partTotal

		summary.partLenStat += master.partLenStat

		summary.readCount += master.readCount
		summary.writeCount += master.writeCount
		summary.totalCount += master.totalCount
		summary.hits += master.hits

		for i, _ := range master.accessCount {
			summary.accessCount[i] += master.accessCount[i]
			summary.accessHomeCount[i] += master.accessHomeCount[i]
			summary.conflicts[i] += master.conflicts[i]
			summary.homeConflicts[i] += master.homeConflicts[i]
		}

		summary.partAccess += master.partAccess
		summary.partSuccess += master.partSuccess

		summary.latency += master.latency
	}

	txn := summary.txnSample

	var sum float64
	var sumpow float64

	var head int
	if coord.store.isPartition {
		head = 0
	} else {
		head = HEAD
	}

	for i, p := range summary.partStat {
		if i >= head {
			sum += float64(p)
		}
	}

	for i, p := range summary.partStat {
		if i >= head {
			sumpow += float64(p*p) / (sum * sum)
		}
	}

	//clog.Info("%v", summary.partStat)

	//f.WriteString(fmt.Sprintf("%v %v %v\n", sum, sumpow, txn))

	//partAvg := float64(sum) / (float64(txn) * float64(len(summary.partStat)))
	partAvg := float64(summary.partTotal) / (float64(txn) * float64(len(summary.partStat)))
	//partVar := (float64(sumpow) / (float64(len(summary.partStat)))) / float64(txn*txn)
	n := float64(len(summary.partStat) - head)
	partVar := (sumpow/n - 1/(n*n)) * 1000

	//f.WriteString(fmt.Sprintf("%.3f %.3f\n", partAvg, partVar))
	partLenVar := float64(summary.partLenStat*txn)/float64(sum*sum) - 1

	var recAvg float64
	sum = float64(summary.readCount + summary.writeCount)
	recAvg = float64(sum) / float64(txn)

	rr := float64(summary.readCount) / float64(summary.readCount+summary.writeCount)
	if WLTYPE == TPCCWL {
		rr *= 10000
	}
	hitRate := float64(summary.hits*100) / float64(summary.readCount+summary.writeCount)
	var confRate float64
	var homeConfRate float64

	for i, _ := range summary.homeConflicts {
		if summary.accessHomeCount[i] != 0 {
			homeConfRate += float64(summary.homeConflicts[i]*100) / float64(summary.accessHomeCount[i])
		}
	}
	if coord.store.isPartition {
		confRate = homeConfRate
	} else {
		for i, _ := range summary.conflicts {
			if summary.accessCount[i] != 0 {
				confRate += float64(summary.conflicts[i]*100) / float64(summary.accessCount[i])
			}
		}
	}

	latency := float64(summary.latency) / float64(summary.totalCount)

	if !coord.store.isPartition {
		if WLTYPE != TPCCWL {
			if latency <= 350 {
				latency -= 80
			} else if latency <= 650 {
				if coord.mode == 1 {
					latency -= 80
				} else {
					latency -= 100
				}
			} else {
				if coord.mode == 1 {
					latency -= 100
				} else {
					latency -= 150
				}
			}
		} else {
			latency -= 40
		}

	}
	partVar = math.Sqrt(partVar)
	partConf := float64(summary.partAccess) / float64(summary.partSuccess)

	coord.feature.PartAvg = partAvg
	coord.feature.PartVar = partVar
	coord.feature.PartLenVar = partLenVar
	coord.feature.PartConf = partConf
	coord.feature.RecAvg = recAvg
	//coord.feature.RecVar = recVar
	coord.feature.HitRate = hitRate
	coord.feature.Latency = latency
	coord.feature.ReadRate = rr
	coord.feature.HomeConfRate = homeConfRate
	coord.feature.ConfRate = confRate
	coord.feature.Txn = float64(coord.NStats[NTXN]-coord.NStats[NABORTS]) / (coord.NTotal.Seconds() - coord.NGen.Seconds())
	coord.feature.AR = float64(coord.NStats[NABORTS]) / float64(coord.NStats[NTXN])
	coord.feature.Mode = coord.mode

	//clog.Info("Hits %v, Count %v, Conficts %v, Access %v", summary.hits, summary.readCount+summary.writeCount, summary.conflicts, summary.accessCount+summary.conflicts)
	//clog.Info(", float64(summary.hits*100)/float64(summary.readCount+summary.writeCount), float64(summary.conflicts*100)/float64(summary.accessCount+summary.conflicts))

	//clog.Info("ReadCount %v; WriteCount %v\n", summary.readCount, summary.writeCount)

	//clog.Info("PartAccess %v; PartSuccess %v", summary.partAccess, summary.partSuccess)
	//clog.Info("TXN %.4f, Abort Rate %.4f, Hits %.4f, Conficts %.4f, PartConf %.4f, Mode %v\n",
	ccType := coord.GetMode()
	if !coord.store.isPartition && coord.mode != PARTITION {
		ccType = ccType + 2
	}

	clog.Info("%.4f, %.4f, Conf %.4f, Home %.4f, Latency %.4f, Type %v, PConf %.4f, PVar %.4f\n",
		float64(coord.NStats[NTXN]-coord.NStats[NABORTS])/(coord.NTotal.Seconds()-coord.NGen.Seconds()), coord.feature.AR, coord.feature.ConfRate, coord.feature.HomeConfRate, latency, ccType, coord.feature.PartConf, coord.feature.PartVar)

	/*if coord.GetMode() == 0 {
		f, err := os.OpenFile("partconf.out", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			clog.Error("Open File Error %s\n", err.Error())
		}
		defer f.Close()
		f.WriteString(fmt.Sprintf("%.4f\t%.4f\n", float64(coord.NStats[NTXN]-coord.NStats[NABORTS])/coord.NExecute.Seconds(), coord.feature.PartConf))
	}*/

	//clog.Info("TXN %.4f, Abort Rate %.4f, Mode %v\n",
	//	float64(coord.NStats[NTXN]), coord.feature.AR, coord.GetMode())

	return coord.feature
}
