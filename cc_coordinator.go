package testbed

import (
	"fmt"
	"os"
	"time"

	"github.com/totemtang/cc-testbed/clog"
)

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
	mode         int
	feature      *Feature
	padding2     [PADDING]byte
	done         chan chan bool
	reports      []chan *ReportInfo
	changeACK    []chan bool
	summary      *ReportInfo
	txnAr        []int64
	padding1     [PADDING]byte
}

const (
	PERSEC       = 1000000000
	REPORTPERIOD = 1000
)

func NewCoordinator(nWorkers int, store *Store, tableCount int, mode int, stat string, sampleRate int, IDToKeyRange [][]int64, reportCount int) *Coordinator {
	coordinator := &Coordinator{
		Workers:   make([]*Worker, nWorkers),
		store:     store,
		NStats:    make([]int64, LAST_STAT),
		stat:      nil,
		mode:      mode,
		feature:   &Feature{},
		done:      make(chan chan bool),
		reports:   make([]chan *ReportInfo, nWorkers),
		changeACK: make([]chan bool, nWorkers),
		summary:   NewReportInfo(store.nParts, tableCount),
	}

	if *Report {
		coordinator.txnAr = make([]int64, reportCount+2*PADDINGINT64)
		coordinator.txnAr = coordinator.txnAr[PADDINGINT64:PADDINGINT64]
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
	} else {
		coord.gatherStats()
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
	if *Report {
		coord.txnAr = coord.txnAr[:0]
	}

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

func (coord *Coordinator) SetMode(mode int) {
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
	f.WriteString(fmt.Sprintf("Transaction Processing Spends %v secs\n", float64(coord.NExecute.Nanoseconds())/float64(PERSEC)))

	if *SysType == PARTITION || (*SysType == ADAPTIVE && mode == PARTITION) {
		f.WriteString(fmt.Sprintf("Cross Partition %v Transactions\n", coord.NStats[NCROSSTXN]))
		f.WriteString(fmt.Sprintf("Transaction Waiting Spends %v secs\n", float64(coord.NWait.Nanoseconds())/float64(PERSEC)))
		f.WriteString(fmt.Sprintf("Has Acquired %v Locks\n", coord.NLockAcquire))
		f.WriteString(fmt.Sprintf("Abort %v Transactions\n", coord.NStats[NABORTS]))
		r := ((float64)(coord.NStats[NABORTS]) / (float64)(coord.NStats[NTXN])) * 100
		f.WriteString(fmt.Sprintf("Abort Rate %.4f%% \n", r))

	} else if *SysType == OCC || (*SysType == ADAPTIVE && mode == OCC) {

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
	padding1   [PADDING]byte
	PartAvg    float64
	PartVar    float64
	PartLenVar float64
	RecAvg     float64
	HitRate    float64
	ReadRate   float64
	ConfRate   float64
	Txn        float64
	AR         float64
	Mode       int
	padding2   [PADDING]byte
}

func (f *Feature) Reset() {
	f.PartAvg = 0
	f.PartVar = 0
	f.PartLenVar = 0
	f.RecAvg = 0
	//f.RecVar = 0
	f.HitRate = 0
	f.ReadRate = 0
	f.ConfRate = 0
	f.Txn = 0
	f.AR = 0
	f.Mode = 0
}

func (ft *Feature) Add(tmpFt *Feature) {
	ft.PartAvg += tmpFt.PartAvg
	ft.PartVar += tmpFt.PartVar
	ft.PartLenVar += tmpFt.PartLenVar
	ft.RecAvg += tmpFt.RecAvg
	//ft.RecVar += tmpFt.RecVar
	ft.HitRate += tmpFt.HitRate
	ft.ReadRate += tmpFt.ReadRate
	ft.ConfRate += tmpFt.ConfRate
	ft.Txn += tmpFt.Txn
	ft.AR += tmpFt.AR
}

func (ft *Feature) Set(tmpFt *Feature) {
	ft.PartAvg = tmpFt.PartAvg
	ft.PartVar = tmpFt.PartVar
	ft.PartLenVar = tmpFt.PartLenVar
	ft.RecAvg = tmpFt.RecAvg
	//ft.RecVar = tmpFt.RecVar
	ft.HitRate = tmpFt.HitRate
	ft.ReadRate = tmpFt.ReadRate
	ft.ConfRate = tmpFt.ConfRate
	ft.Txn = tmpFt.Txn
	ft.AR = tmpFt.AR
	ft.Mode = tmpFt.Mode
}

func (ft *Feature) Avg(count float64) {
	ft.PartAvg /= count
	ft.PartVar /= count
	ft.PartLenVar /= count
	ft.RecAvg /= count
	//ft.RecVar /= count
	ft.HitRate /= count
	ft.ReadRate /= count
	ft.ConfRate /= count
	ft.Txn /= count
	ft.AR /= count
}

// Currently, we support 5 features
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

		summary.partLenStat += master.partLenStat

		/*for j, _ := range master.recStat {
			for k, rs := range master.recStat[j] {
				summary.recStat[j][k] += rs
			}
		}*/

		for i, rs := range master.recStat {
			summary.recStat[i] += rs
		}

		summary.readCount += master.readCount
		summary.writeCount += master.writeCount
		summary.hits += master.hits

		summary.accessCount += master.accessCount
		summary.conflicts += master.conflicts
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
	partLenVar := float64(summary.partLenStat*txn)/float64(sum*sum) - 1

	//var recVar float64

	/*
		for i := 0; i < len(summary.recStat); i++ {
			sum = 0
			sumpow = 0
			for _, r := range summary.recStat[i] {
				sum += r
				sumpow += r * r
			}

			if sum == 0 {
				continue
			}
			//clog.Info("Sum %v; SumPow %v\n", sum, sumpow)

			tmpAvg := float64(sum) / float64(txn*HISTOGRAMLEN)
			tmpVar := float64(sumpow*HISTOGRAMLEN)/float64(sum*sum) - 1
			recAvg += tmpAvg
			recVar += tmpVar * tmpAvg
		}
	*/
	//recVar /= recAvg

	var recAvg float64
	sum = 0
	for _, rs := range summary.recStat {
		sum += rs
	}
	recAvg = float64(sum) / float64(txn)

	rr := float64(summary.readCount) / float64(summary.readCount+summary.writeCount)
	hitRate := float64(summary.hits*100) / float64(summary.readCount+summary.writeCount)
	var confRate float64
	if summary.conflicts != 0 {
		confRate = float64(summary.conflicts*100) / float64(summary.accessCount+summary.conflicts)
	}
	/*
		f.WriteString(fmt.Sprintf("%.3f\t %.3f\t %.3f\t %.3f\t %.3f\t %v\t ", partAvg, partVar, recAvg, recVar, rr, coord.Workers[0].mode))
		f.WriteString(fmt.Sprintf("%.4f\t %.4f\n",
			float64(coord.NStats[NTXN]-coord.NStats[NABORTS])/coord.NExecute.Seconds(), float64(coord.NStats[NABORTS])/float64(coord.NStats[NTXN])))
	*/
	coord.feature.PartAvg = partAvg
	coord.feature.PartVar = partVar
	coord.feature.PartLenVar = partLenVar
	coord.feature.RecAvg = recAvg
	//coord.feature.RecVar = recVar
	coord.feature.HitRate = hitRate
	coord.feature.ReadRate = rr
	coord.feature.ConfRate = confRate
	coord.feature.Txn = float64(coord.NStats[NTXN]-coord.NStats[NABORTS]) / coord.NExecute.Seconds()
	coord.feature.AR = float64(coord.NStats[NABORTS]) / float64(coord.NStats[NTXN])
	coord.feature.Mode = coord.mode

	clog.Info("Hits %v, Count %v, Conficts %v, Access %v", summary.hits, summary.readCount+summary.writeCount, summary.conflicts, summary.accessCount+summary.conflicts)

	//clog.Info("ReadCount %v; WriteCount %v\n", summary.readCount, summary.writeCount)

	clog.Info("TXN %.4f, Abort Rate %.4f, Mode %v\n",
		float64(coord.NStats[NTXN]-coord.NStats[NABORTS])/coord.NExecute.Seconds(), coord.feature.AR, coord.GetMode())

	return coord.feature
}
