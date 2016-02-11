package testbed

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/totemtang/cc-testbed/clog"
)

const (
	HISTOGRAMLEN = 100
)

var Report = flag.Bool("report", true, "whether periodically report runtime information to coordinator")

type ReportInfo struct {
	padding0   [PADDING]byte
	execTime   time.Duration
	prevExec   time.Duration
	txn        int64
	aborts     int64
	prevTxn    int64
	prevAborts int64
	txnSample  int64
	partStat   []int64
	recStat    [][]int64
	readCount  int64
	writeCount int64
	sampleRate int
	padding1   [PADDING]byte
}

func (ri *ReportInfo) Reset() {
	ri.execTime = 0
	ri.prevExec = 0
	ri.txn = 0
	ri.aborts = 0
	ri.prevTxn = 0
	ri.prevAborts = 0

	for i, _ := range ri.partStat {
		ri.partStat[i] = 0
	}

	for i, _ := range ri.recStat {
		for j, _ := range ri.recStat[i] {
			ri.recStat[i][j] = 0
		}
	}

	ri.readCount = 0
	ri.writeCount = 0
	ri.txnSample = 0

}

func NewReportInfo(nParts int, tableCount int, sampleRate int) *ReportInfo {
	ri := &ReportInfo{}

	ri.txnSample = 0
	ri.partStat = make([]int64, 2*PADDINGINT64+nParts)
	ri.partStat = ri.partStat[PADDINGINT64 : PADDINGINT64+nParts]

	ri.recStat = make([][]int64, 2*PADDINGINT64+tableCount)
	ri.recStat = ri.recStat[PADDINGINT64 : PADDINGINT64+tableCount]
	for i := 0; i < tableCount; i++ {
		ri.recStat[i] = make([]int64, 2*PADDINGINT64+HISTOGRAMLEN)
		ri.recStat[i] = ri.recStat[i][PADDINGINT64 : PADDINGINT64+HISTOGRAMLEN]
	}

	ri.sampleRate = sampleRate

	return ri
}

type SampleTool struct {
	padding0     [PADDING]byte
	nParts       int
	tableCount   int
	IDToKeyRange [][]int64
	sampleCount  int
	sampleRate   int
	period       []int64
	offset       []int64
	offIndex     []int
	IDToKeys     []int64
	IDToKeyLen   []int
	padding1     [PADDING]byte
}

func NewSampleTool(nParts int, IDToKeyRange [][]int64, sampleRate int) *SampleTool {
	st := &SampleTool{
		nParts:       nParts,
		tableCount:   len(IDToKeyRange),
		IDToKeyRange: IDToKeyRange,
		sampleRate:   sampleRate,
	}

	st.period = make([]int64, 2*PADDINGINT64+st.tableCount)
	st.offset = make([]int64, 2*PADDINGINT64+st.tableCount)
	st.offIndex = make([]int, 2*PADDINGINT+st.tableCount)
	st.IDToKeys = make([]int64, 2*PADDINGINT64+st.tableCount)
	st.IDToKeyLen = make([]int, 2*PADDINGINT+st.tableCount)

	st.period = st.period[PADDINGINT64 : PADDINGINT64+st.tableCount]
	st.offset = st.offset[PADDINGINT64 : PADDINGINT64+st.tableCount]
	st.offIndex = st.offIndex[PADDINGINT : PADDINGINT+st.tableCount]
	st.IDToKeys = st.IDToKeys[PADDINGINT64 : PADDINGINT64+st.tableCount]
	st.IDToKeyLen = st.IDToKeyLen[PADDINGINT : PADDINGINT+st.tableCount]

	for i := 0; i < st.tableCount; i++ {
		keyLen := len(st.IDToKeyRange[i])
		var nKeys int64 = 1
		for j := 0; j < keyLen; j++ {
			nKeys *= st.IDToKeyRange[i][j]
		}
		st.period[i] = nKeys / HISTOGRAMLEN
		r := nKeys % HISTOGRAMLEN

		st.offset[i] = (st.period[i] + 1) * r
		st.offIndex[i] = int(r)
		st.IDToKeys[i] = nKeys
		st.IDToKeyLen[i] = keyLen
	}

	return st
}

func (st *SampleTool) oneSample(tableID int, key Key, ri *ReportInfo, isRead bool) {
	if st.sampleCount != 0 {
		return
	}

	var intKey int64 = int64(ParseKey(key, st.IDToKeyLen[tableID]-1))
	for i := st.IDToKeyLen[tableID] - 2; i >= 0; i-- {
		intKey *= st.IDToKeyRange[tableID][i]
		intKey += int64(ParseKey(key, i))
	}

	var index int64
	if intKey < st.offset[tableID] {
		index = intKey / (st.period[tableID] + 1)
	} else {
		index = int64(st.offIndex[tableID]) + (intKey-st.offset[tableID])/st.period[tableID]
	}

	ri.recStat[tableID][index]++

	if isRead {
		ri.readCount++
	} else {
		ri.writeCount++
	}
}

func (st *SampleTool) onePartSample(ap []int, ri *ReportInfo) {
	st.sampleCount++
	if st.sampleCount < st.sampleRate {
		return
	}
	st.sampleCount = 0
	ri.txnSample++

	for _, p := range ap {
		ri.partStat[p]++
	}
}

func (st *SampleTool) Reset() {
	st.sampleCount = 0
}

type Coordinator struct {
	padding0     [PADDING]byte
	Workers      []*Worker
	store        *Store
	NStats       []int64
	NGen         time.Duration
	NExecute     time.Duration
	NWait        time.Duration
	NLockAcquire int64
	stat         *os.File
	padding2     [PADDING]byte
	done         chan chan bool
	reports      []chan *ReportInfo
	changeACK    []chan bool
	summary      *ReportInfo
	padding1     [PADDING]byte
}

const (
	PERSEC       = 1000000000
	REPORTPERIOD = 1000
)

func NewCoordinator(nWorkers int, store *Store, tableCount int, mode int, stat string, sampleRate int, IDToKeyRange [][]int64) *Coordinator {
	coordinator := &Coordinator{
		Workers:   make([]*Worker, nWorkers),
		store:     store,
		NStats:    make([]int64, LAST_STAT),
		stat:      nil,
		done:      make(chan chan bool),
		reports:   make([]chan *ReportInfo, nWorkers),
		changeACK: make([]chan bool, nWorkers),
		summary:   NewReportInfo(store.nParts, tableCount, sampleRate),
	}

	if stat != "" {
		st, err := os.OpenFile(stat, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			clog.Error("Open File Error %s\n", err.Error())
		}
		coordinator.stat = st
	}

	for i := range coordinator.Workers {
		coordinator.Workers[i] = NewWorker(i, store, coordinator, tableCount, mode, sampleRate, IDToKeyRange)
		coordinator.reports[i] = make(chan *ReportInfo, 1)
		coordinator.changeACK[i] = make(chan bool)
	}

	if *Report {
		go coordinator.process()
	}

	return coordinator
}

func (coord *Coordinator) process() {
	summary := coord.summary
	st := coord.stat
	var ri *ReportInfo
	var index = PARTITION

	for {
		select {
		case ri = <-coord.reports[0]:

			summary.execTime = ri.execTime
			summary.txn = ri.txn
			summary.aborts = ri.aborts

			for i := 1; i < len(coord.reports); i++ {
				ri = <-coord.reports[i]
				summary.execTime += ri.execTime
				summary.txn += ri.txn
				summary.aborts += ri.aborts
			}

			if st != nil {
				clog.Info("%v\t%.f\t%.6f\n",
					index, float64(summary.txn-summary.aborts)/summary.execTime.Seconds(),
					float64(summary.aborts)/float64(summary.txn))
				//st.WriteString(fmt.Sprintf("%.f", float64(summary.txn)/summary.NExecute.Seconds()))
				//st.WriteString(fmt.Sprintf("\t%.6f\n", float64(coord.NStats[testbed.NABORTS])/float64(coord.NStats[testbed.NTXN])))
			}

			if *SysType == ADAPTIVE {
				index = (index + 1) % ADAPTIVE
				for i := 0; i < len(coord.Workers); i++ {
					coord.Workers[i].modeChange <- true
				}
				for i := 0; i < len(coord.Workers); i++ {
					<-coord.changeACK[i]
				}
				for i := 0; i < len(coord.Workers); i++ {
					coord.Workers[i].modeChan <- index
				}
			}
		case x := <-coord.done:
			for i := 0; i < len(coord.Workers); i++ {
				coord.Workers[i].done <- true
			}
			x <- true
			return
		}
	}
}

func (coord *Coordinator) Finish() {
	if *Report {
		coord.stat.Close()
		x := make(chan bool)
		coord.done <- x
		<-x
	}
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
	coord.NWait = 0
	coord.NLockAcquire = 0
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
		worker.NWait = 0
		worker.NLockAcquire = 0
		worker.next = 0
		worker.epoch = 0
		worker.riMaster.Reset()
		worker.riReplica.Reset()
		worker.st.Reset()
	}
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
		coord.NWait += worker.NWait
		coord.NLockAcquire += worker.NLockAcquire
	}
}

func (coord *Coordinator) PrintStats(f *os.File) {
	coord.gatherStats()

	f.WriteString("================\n")
	f.WriteString("Print Statistics\n")
	f.WriteString("================\n")

	f.WriteString(fmt.Sprintf("Issue %v Transactions in Total\n", coord.NStats[NTXN]))
	f.WriteString(fmt.Sprintf("Transaction Generation Spends %v secs\n", float64(coord.NGen.Nanoseconds())/float64(PERSEC)))
	f.WriteString(fmt.Sprintf("Transaction Processing Spends %v secs\n", float64(coord.NExecute.Nanoseconds())/float64(PERSEC)))

	if *SysType == PARTITION {
		f.WriteString(fmt.Sprintf("Cross Partition %v Transactions\n", coord.NStats[NCROSSTXN]))
		f.WriteString(fmt.Sprintf("Transaction Waiting Spends %v secs\n", float64(coord.NWait.Nanoseconds())/float64(PERSEC)))
		f.WriteString(fmt.Sprintf("Has Acquired %v Locks\n", coord.NLockAcquire))
		f.WriteString(fmt.Sprintf("Abort %v Transactions\n", coord.NStats[NABORTS]))
		r := ((float64)(coord.NStats[NABORTS]) / (float64)(coord.NStats[NTXN])) * 100
		f.WriteString(fmt.Sprintf("Abort Rate %.4f%% \n", r))

	} else if *SysType == OCC {

		if *PhyPart {
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

	} else if *SysType == LOCKING {
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

// Currently, we have supported 5 features
//
func (coord *Coordinator) PrintTraining(f *os.File) {
	summary := coord.summary
	for _, w := range coord.Workers {
		master := w.riMaster

		summary.readCount += master.readCount
		summary.writeCount += master.writeCount

		for j, ps := range master.partStat {
			summary.partStat[j] += ps
		}

		for j, _ := range master.recStat {
			for k, rs := range master.recStat[j] {
				summary.recStat[j][k] += rs
			}
		}

		summary.txn += w.riMaster.txn
		summary.aborts += w.riMaster.aborts
		summary.txnSample += w.riMaster.txnSample
	}

	txn := summary.txnSample

	var sum int64
	var sumpow int64
	for _, p := range summary.partStat {
		sum += p
		sumpow += p * p
	}

	//f.WriteString(fmt.Sprintf("%v %v %v\n", sum, sumpow, txn))

	partAvg := float64(sum) / (float64(txn) * float64(len(summary.partStat)))
	//partVar := (float64(sumpow) / (float64(len(summary.partStat)))) / float64(txn*txn)
	partVar := float64(sumpow*int64(len(summary.partStat)))/float64(sum*sum) - 1
	//f.WriteString(fmt.Sprintf("%.3f %.3f\n", partAvg, partVar))

	var recAvg float64
	var recVar float64

	for i := 0; i < len(summary.recStat); i++ {
		sum = 0
		sumpow = 0
		for _, r := range summary.recStat[i] {
			sum += r
			sumpow += r * r
		}

		tmpAvg := float64(sum) / float64(txn*HISTOGRAMLEN)
		tmpVar := float64(sumpow*HISTOGRAMLEN)/float64(sum*sum) - 1
		recAvg += tmpAvg
		recVar += tmpVar * tmpAvg
	}

	recVar /= recAvg

	rr := float64(summary.readCount) / float64(summary.readCount+summary.writeCount)

	f.WriteString(fmt.Sprintf("%.3f\t %.3f\t %.3f\t %.3f\t %.3f\t %v\n", partAvg, partVar, recAvg, recVar, rr, coord.Workers[0].mode))

}
