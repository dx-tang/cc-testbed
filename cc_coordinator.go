package testbed

import (
	"fmt"
	"os"
	"time"

	"github.com/totemtang/cc-testbed/classifier"
	"github.com/totemtang/cc-testbed/clog"
)

const (
	CLASSIFERPATH   = "/home/totemtang/Multicore-CC/workspace/src/github.com/totemtang/cc-testbed/classifier"
	SINGLEPARTTRAIN = "single-part-train.out"
	SINGLEOCCTRAIN  = "single-occ-train.out"
	SBPARTTRAIN     = "sb-part-train.out"
	SBOCCTRAIN      = "sb-occ-train.out"
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
	reports      []chan *ReportInfo
	changeACK    []chan bool
	summary      *ReportInfo
	perTest      int
	TxnAR        []float64
	ModeAR       []int
	rc           int
	clf          classifier.Classifier
	workload     int
	padding1     [PADDING]byte
}

const (
	PERSEC       = 1000000000
	PERMINISEC   = 1000
	REPORTPERIOD = 2000
)

func NewCoordinator(nWorkers int, store *Store, tableCount int, mode int, sampleRate int, IDToKeyRange [][]int64, tests int, nsecs int, workload int) *Coordinator {
	coordinator := &Coordinator{
		Workers:   make([]*Worker, nWorkers),
		store:     store,
		NStats:    make([]int64, LAST_STAT),
		mode:      mode,
		feature:   &Feature{},
		reports:   make([]chan *ReportInfo, nWorkers),
		changeACK: make([]chan bool, nWorkers),
		summary:   NewReportInfo(store.nParts, tableCount),
	}

	if *Report {
		coordinator.perTest = nsecs * PERMINISEC / REPORTPERIOD
		reportCount := tests * coordinator.perTest
		coordinator.TxnAR = make([]float64, reportCount+2*PADDINGINT64)
		coordinator.TxnAR = coordinator.TxnAR[PADDINGINT64 : reportCount+PADDINGINT64]
		coordinator.ModeAR = make([]int, reportCount+2*PADDINGINT)
		coordinator.ModeAR = coordinator.ModeAR[PADDINGINT : reportCount+PADDINGINT]

		coordinator.workload = workload
		if workload == SINGLEWL {
			partTS := CLASSIFERPATH + "/" + SINGLEPARTTRAIN
			occTS := CLASSIFERPATH + "/" + SINGLEOCCTRAIN
			coordinator.clf = classifier.NewSingleClassifier(CLASSIFERPATH, partTS, occTS)
		} else if workload == SMALLBANKWL {
			partTS := CLASSIFERPATH + "/" + SBPARTTRAIN
			occTS := CLASSIFERPATH + "/" + SBOCCTRAIN
			coordinator.clf = classifier.NewSBClassifier(CLASSIFERPATH, partTS, occTS)
		} else {
			clog.Error("Workload %v Not Supported", workload)
		}
	}

	for i := range coordinator.Workers {
		coordinator.Workers[i] = NewWorker(i, store, coordinator, tableCount, mode, sampleRate, IDToKeyRange)
		coordinator.reports[i] = make(chan *ReportInfo, 1)
		coordinator.changeACK[i] = make(chan bool, 1)
	}

	return coordinator
}

func (coord *Coordinator) process() {
	summary := coord.summary
	var ri *ReportInfo
	for {
		select {
		case ri = <-coord.reports[0]:

			summary.execTime = ri.execTime
			summary.txn = ri.txn
			summary.aborts = ri.aborts

			if *SysType == ADAPTIVE {
				summary.txnSample = ri.txnSample

				for i, ps := range ri.partStat {
					summary.partStat[i] = ps
				}

				summary.partLenStat = ri.partLenStat

				for i, rs := range ri.recStat {
					summary.recStat[i] = rs
				}

				summary.readCount = ri.readCount
				summary.writeCount = ri.writeCount
				//summary.hits = ri.hits

				summary.accessCount = ri.accessCount
				summary.conflicts = ri.conflicts

				summary.latency = ri.latency
			}

			for i := 1; i < len(coord.reports); i++ {
				ri = <-coord.reports[i]
				summary.execTime += ri.execTime
				summary.txn += ri.txn
				summary.aborts += ri.aborts

				if *SysType == ADAPTIVE {
					summary.txnSample += ri.txnSample

					for i, ps := range ri.partStat {
						summary.partStat[i] += ps
					}

					summary.partLenStat += ri.partLenStat

					for i, rs := range ri.recStat {
						summary.recStat[i] += rs
					}

					summary.readCount += ri.readCount
					summary.writeCount += ri.writeCount
					//summary.hits += ri.hits

					summary.accessCount += ri.accessCount
					summary.conflicts += ri.conflicts

					summary.latency += ri.latency
				}
			}

			// Record Throughput and Mode
			coord.TxnAR[coord.rc] = float64(summary.txn-summary.aborts) / summary.execTime.Seconds()
			//clog.Info("Summary %v; Exec Secs: %v", summary.txn, summary.execTime.Seconds())
			coord.ModeAR[coord.rc] = coord.mode

			clog.Info("Mode %v; Txn %.4f; Abort %.4f", coord.ModeAR[coord.rc], coord.TxnAR[coord.rc], float64(summary.aborts)/float64(summary.txn))

			coord.rc++

			// Done
			if coord.rc%coord.perTest == 0 {
				// Done with tests
				for i := 0; i < len(coord.Workers); i++ {
					coord.Workers[i].done <- true
				}
				return
			}

			// Switch
			if *SysType == ADAPTIVE {
				//for _, ps := range summary.partStat {
				//	clog.Info("%v ", ps)
				//}
				// Compute Features
				txn := summary.txnSample

				var sum int64
				var sumpow int64
				for _, p := range summary.partStat {
					sum += p
					sumpow += p * p
				}

				partAvg := float64(sum) / (float64(txn) * float64(len(summary.partStat)))
				partVar := float64(sumpow*int64(len(summary.partStat)))/float64(sum*sum) - 1
				partLenVar := float64(summary.partLenStat*txn)/float64(sum*sum) - 1

				var recAvg float64
				sum = 0
				for _, rs := range summary.recStat {
					sum += rs
				}
				recAvg = float64(sum) / float64(txn)

				rr := float64(summary.readCount) / float64(summary.readCount+summary.writeCount)
				//hitRate := float64(summary.hits*100) / float64(summary.readCount+summary.writeCount)
				latency := float64(summary.latency) / float64(summary.accessCount)
				var confRate float64
				if summary.conflicts != 0 {
					confRate = float64(summary.conflicts*100) / float64(summary.accessCount+summary.conflicts)
				}

				//clog.Info("%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\n", partAvg, partVar, partLenVar, recAvg, hitRate, rr, confRate)

				// Use Classifier to Predict Features
				//mode := coord.clf.Predict(partAvg, partVar, partLenVar, recAvg, hitRate, rr, confRate)
				mode := coord.clf.Predict(partAvg, partVar, partLenVar, recAvg, latency, rr, confRate)
				var change bool = false
				if mode != coord.mode {
					if !(mode == 3 && coord.mode != 0) {
						change = true
						if mode == 3 {
							// Prefer 2PL
							coord.mode = 2
						} else {
							coord.mode = mode
						}
					}
				}
				if change {
					for i := 0; i < len(coord.Workers); i++ {
						coord.Workers[i].modeChange <- true
					}
					for i := 0; i < len(coord.Workers); i++ {
						<-coord.changeACK[i]
					}
					for i := 0; i < len(coord.Workers); i++ {
						coord.Workers[i].modeChan <- coord.mode
					}
				}
			}
		}
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
	padding1   [PADDING]byte
	PartAvg    float64
	PartVar    float64
	PartLenVar float64
	PartConf   float64
	RecAvg     float64
	HitRate    float64
	Latency    float64
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
	f.PartConf = 0
	f.RecAvg = 0
	//f.RecVar = 0
	f.HitRate = 0
	f.Latency = 0
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
	ft.PartConf += tmpFt.PartConf
	ft.RecAvg += tmpFt.RecAvg
	//ft.RecVar += tmpFt.RecVar
	ft.HitRate += tmpFt.HitRate
	ft.Latency += tmpFt.Latency
	ft.ReadRate += tmpFt.ReadRate
	ft.ConfRate += tmpFt.ConfRate
	ft.Txn += tmpFt.Txn
	ft.AR += tmpFt.AR
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
	ft.Txn /= count
	ft.AR /= count
}

// Currently, we support 8 features
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

		for i, rs := range master.recStat {
			summary.recStat[i] += rs
		}

		summary.readCount += master.readCount
		summary.writeCount += master.writeCount
		summary.hits += master.hits

		summary.accessCount += master.accessCount
		summary.conflicts += master.conflicts

		summary.partAccess += master.partAccess
		summary.partSuccess += master.partSuccess

		summary.latency += master.latency
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
		confRate = float64(summary.conflicts*100) / float64(summary.accessCount)
	}

	latency := float64(summary.latency) / float64(summary.accessCount)
	/*
		f.WriteString(fmt.Sprintf("%.3f\t %.3f\t %.3f\t %.3f\t %.3f\t %v\t ", partAvg, partVar, recAvg, recVar, rr, coord.Workers[0].mode))
		f.WriteString(fmt.Sprintf("%.4f\t %.4f\n",
			float64(coord.NStats[NTXN]-coord.NStats[NABORTS])/coord.NExecute.Seconds(), float64(coord.NStats[NABORTS])/float64(coord.NStats[NTXN])))
	*/
	coord.feature.PartAvg = partAvg
	coord.feature.PartVar = partVar
	coord.feature.PartLenVar = partLenVar
	//coord.feature.PartConf = float64(summary.partAccess) / float64(summary.partSuccess)
	coord.feature.RecAvg = recAvg
	//coord.feature.RecVar = recVar
	coord.feature.HitRate = hitRate
	coord.feature.Latency = latency
	coord.feature.ReadRate = rr
	coord.feature.ConfRate = confRate
	coord.feature.Txn = float64(coord.NStats[NTXN]-coord.NStats[NABORTS]) / coord.NExecute.Seconds()
	coord.feature.AR = float64(coord.NStats[NABORTS]) / float64(coord.NStats[NTXN])
	coord.feature.Mode = coord.mode

	//clog.Info("Hits %v, Count %v, Conficts %v, Access %v", summary.hits, summary.readCount+summary.writeCount, summary.conflicts, summary.accessCount+summary.conflicts)
	//clog.Info(", float64(summary.hits*100)/float64(summary.readCount+summary.writeCount), float64(summary.conflicts*100)/float64(summary.accessCount+summary.conflicts))

	//clog.Info("ReadCount %v; WriteCount %v\n", summary.readCount, summary.writeCount)

	//clog.Info("PartAccess %v; PartSuccess %v", summary.partAccess, summary.partSuccess)
	//clog.Info("TXN %.4f, Abort Rate %.4f, Hits %.4f, Conficts %.4f, PartConf %.4f, Mode %v\n",
	//	float64(coord.NStats[NTXN]-coord.NStats[NABORTS])/coord.NExecute.Seconds(), coord.feature.AR, coord.feature.HitRate, coord.feature.ConfRate, coord.feature.PartConf, coord.GetMode())

	clog.Info("TXN %.4f, Abort Rate %.4f, Conficts %.4f, Latency %.4f, Mode %v\n",
		float64(coord.NStats[NTXN]-coord.NStats[NABORTS])/coord.NExecute.Seconds(), coord.feature.AR, coord.feature.ConfRate, latency, coord.GetMode())

	//clog.Info("TXN %.4f, Abort Rate %.4f, Mode %v\n",
	//	float64(coord.NStats[NTXN]), coord.feature.AR, coord.GetMode())

	return coord.feature
}
