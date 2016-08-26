package testbed

import (
	"errors"
	"runtime/debug"
	//"sync"
	"time"

	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/spinlock"
)

const (
	NABORTS = iota
	NREADABORTS
	NLOCKABORTS
	NRCHANGEABORTS
	NRWABORTS
	NINDEXABORTS
	NRLOCKABORTS
	NWLOCKABORTS
	NUPGRADEABORTS
	NENOKEY
	NTXN
	NCROSSTXN
	LAST_STAT
)

var (
	FINISHED = errors.New("Finished")
)

type TransactionFunc func(Trans, ETransaction) (Value, error)

// INDEX CHANGE ACTION
const (
	INDEX_ACTION_MERGE = iota
	INDEX_ACTION_PARTITION
	INDEX_ACTION_NONE
)

type IndexAction struct {
	padding1   [PADDING]byte
	actionType int
	start      int
	end        int
	padding2   [PADDING]byte
}

type Worker struct {
	padding [PADDING]byte
	spinlock.Spinlock
	ID           int
	next         TID
	epoch        TID
	store        *Store
	coord        *Coordinator
	E            ETransaction
	ExecPool     []ETransaction
	txns         []TransactionFunc
	NStats       []int64
	NGen         time.Duration
	NExecute     time.Duration
	NTotal       time.Duration
	NWait        time.Duration
	NCrossWait   time.Duration
	NLockAcquire int64
	start        time.Time
	end          time.Time
	finished     bool
	mode         int
	needLock     bool
	done         chan bool
	modeChange   chan bool
	modeChan     chan int
	indexStart   chan bool
	indexAction  chan *IndexAction
	indexDone    chan bool
	indexConfirm chan bool
	actionTrans  chan *IndexAction
	riMaster     *ReportInfo
	riReplica    *ReportInfo
	st           *SampleTool
	iaAR         []IndexAlloc
	preIAAR      []IndexAlloc
	partitioner  []Partitioner
	padding2     [PADDING]byte
}

func (w *Worker) Register(fn int, transaction TransactionFunc) {
	w.txns[fn] = transaction
}

func NewWorker(id int, s *Store, c *Coordinator, tableCount int, mode int, sampleRate int, workload int) *Worker {
	w := &Worker{
		ID:           id,
		store:        s,
		coord:        c,
		txns:         make([]TransactionFunc, LAST_TXN),
		NStats:       make([]int64, LAST_STAT),
		finished:     false,
		mode:         mode,
		done:         make(chan bool, 1),
		modeChange:   make(chan bool, 1),
		modeChan:     make(chan int, 1),
		indexStart:   make(chan bool, 1),
		indexAction:  make(chan *IndexAction, 1),
		indexDone:    make(chan bool, 1),
		indexConfirm: make(chan bool, 1),
		actionTrans:  make(chan *IndexAction, 1),
	}

	/*w.st = NewSampleTool(s.nParts, sampleRate, s)
	w.riMaster = NewReportInfo(s.nParts, tableCount)
	w.riReplica = NewReportInfo(s.nParts, tableCount)*/

	w.st = NewSampleTool(*NumPart, sampleRate, s, w)
	w.riMaster = NewReportInfo(*NumPart, tableCount)
	w.riReplica = NewReportInfo(*NumPart, tableCount)

	if mode == PARTITION {
		w.needLock = true
	} else {
		w.needLock = false
	}

	w.ExecPool = make([]ETransaction, ADAPTIVE-PARTITION)
	w.ExecPool[PARTITION] = StartPTransaction(w, tableCount)
	w.ExecPool[OCC] = StartOTransaction(w, tableCount)
	w.ExecPool[LOCKING] = StartLTransaction(w, tableCount)

	if *SysType == PARTITION {
		w.E = w.ExecPool[mode]
	} else if *SysType == OCC {
		w.E = w.ExecPool[mode]
	} else if *SysType == LOCKING {
		w.E = w.ExecPool[mode]
	} else if *SysType == ADAPTIVE {
		w.E = w.ExecPool[mode]
	} else {
		clog.Error("System Type %v Not Supported Yet\n", *SysType)
	}

	if workload == TPCCWL {
		w.iaAR = make([]IndexAlloc, tableCount)
		w.iaAR[NEWORDER] = &NewOrderIndexAlloc{}
		w.iaAR[NEWORDER].OneAllocate()
		w.iaAR[ORDER] = &OrderIndexAlloc{}
		w.iaAR[ORDER].OneAllocate()
		w.iaAR[HISTORY] = &HistoryIndexAlloc{}
		w.iaAR[HISTORY].OneAllocate()
		w.iaAR[ORDERLINE] = &OrderLineIndexAlloc{}
		w.iaAR[ORDERLINE].OneAllocate()

		loaders := *NLOADERS
		if id < loaders {
			perWorker := *NumPart / loaders
			residue := *NumPart % loaders
			numWorkers := perWorker
			tmpID := id - (*NumPart - loaders)
			if tmpID < residue {
				numWorkers += 1
			}
			w.preIAAR = make([]IndexAlloc, tableCount)
			w.preIAAR[NEWORDER] = &NewOrderIndexAlloc{}
			w.preIAAR[NEWORDER].OneAllocateSize(NEWORDER_INDEX_ORIGINAL * numWorkers)
			w.preIAAR[ORDER] = &OrderIndexAlloc{}
			w.preIAAR[ORDER].OneAllocateSize(ORDER_INDEX_ORIGINAL * numWorkers)
			w.preIAAR[HISTORY] = &HistoryIndexAlloc{}
			w.preIAAR[HISTORY].OneAllocateSize(HISTORY_INDEX_ORIGINAL * numWorkers)
			w.preIAAR[ORDERLINE] = &OrderLineIndexAlloc{}
			w.preIAAR[ORDERLINE].OneAllocateSize(ORDERLINE_INDEX_ORIGINAL * numWorkers)
		}
	} else if workload == SINGLEWL {
		if *Report {
			w.partitioner = c.singleWL.GetPartitioner(w.ID)
		}
	} else {
		if *Report {
			w.partitioner = c.sbWL.GetPartitioner(w.ID)
		}
	}

	// SmallBank Workload
	w.Register(AMALGAMATE, Amalgamate)
	w.Register(SENDPAYMENT, SendPayment)
	w.Register(BALANCE, Balance)
	w.Register(WRITECHECK, WriteCheck)
	w.Register(DEPOSITCHECKING, DepositChecking)
	w.Register(TRANSACTIONSAVINGS, TransactionSavings)
	w.Register(ADDONE, AddOne)
	w.Register(UPDATEINT, UpdateInt)

	// TPCC Workload
	w.Register(TPCC_NEWORDER, NewOrder)
	w.Register(TPCC_PAYMENT_ID, Payment)
	w.Register(TPCC_PAYMENT_LAST, Payment)
	w.Register(TPCC_ORDERSTATUS_ID, OrderStatus)
	w.Register(TPCC_ORDERSTATUS_LAST, OrderStatus)
	w.Register(TPCC_STOCKLEVEL, StockLevel)

	return w
}

func (w *Worker) run() {
	coord := w.coord
	duration := time.Duration(REPORTPERIOD) * time.Millisecond
	tm := time.NewTicker(duration).C
	for {
		select {
		case <-tm: // Report Information within One Period
			w.Lock()
			replica := w.riMaster
			w.riMaster = w.riReplica
			w.riReplica = replica
			if *SysType == ADAPTIVE {
				//replica.execTime = w.NExecute - w.riMaster.prevExec
				replica.genTime = w.NGen - w.riMaster.prevGen
				replica.txn = w.NStats[NTXN] - w.riMaster.prevTxn
				replica.aborts = w.NStats[NABORTS] - w.riMaster.prevAborts

				//replica.prevExec = w.NExecute
				replica.prevGen = w.NGen
				replica.prevTxn = w.NStats[NTXN]
				replica.prevAborts = w.NStats[NABORTS]

				w.coord.reports[w.ID] <- replica
			} else {
				//replica.execTime = w.NExecute - w.riMaster.prevExec
				replica.genTime = w.NGen - w.riMaster.prevGen
				replica.txn = w.NStats[NTXN] - w.riMaster.prevTxn
				replica.aborts = w.NStats[NABORTS] - w.riMaster.prevAborts

				//replica.prevExec = w.NExecute
				replica.prevGen = w.NGen
				replica.prevTxn = w.NStats[NTXN]
				replica.prevAborts = w.NStats[NABORTS]

				w.coord.reports[w.ID] <- replica
			}
			w.riMaster.Reset()
			w.Unlock()
		case <-w.indexStart:
			w.Lock()
			coord.indexStartACK[w.ID] <- true
			action := <-w.indexAction
			w.Unlock()
			if action.actionType == INDEX_ACTION_MERGE {
				w.actionTrans <- action
			} else if action.actionType == INDEX_ACTION_PARTITION {
				// Notify the worker to do IndexPartition
				w.actionTrans <- action
			}
		case <-w.indexDone:
			w.Lock()
			coord.indexDoneACK[w.ID] <- true
			<-w.indexConfirm
			w.Unlock()
		}
	}
}

func (w *Worker) Start() {
}

func (w *Worker) Finish() {

}

func (w *Worker) Reset() {

}

func (w *Worker) SetMode(mode int) {
	//if *SysType != ADAPTIVE {
	//	return
	//}
	w.mode = mode
	w.E = w.ExecPool[mode]
	if mode == PARTITION {
		w.needLock = true
	} else {
		w.needLock = false
	}
}

func (w *Worker) doTxn(t Trans) (Value, error) {
	txn := t.GetTXN()
	if txn >= LAST_TXN {
		debug.PrintStack()
		clog.Error("Unknown transaction number %v\n", txn)
	}
	w.NStats[NTXN]++

	w.E.Reset(t)

	x, err := w.txns[txn](t, w.E)

	if err == EABORT {
		w.NStats[NABORTS]++
		return nil, err
	} else if err == ENOKEY {
		clog.Info("txn %v", txn)
		w.NStats[NENOKEY]++
		return nil, err
	} else if err == ELACKBALANCE {
		w.NStats[NABORTS]++
	} else if err == ENEGSAVINGS {
		w.NStats[NABORTS]++
	}

	//return x, err
	return x, nil
}

func (w *Worker) One(t Trans) (Value, error) {
	//w.start = time.Now()
	var ap []int

	select {
	case <-w.done:
		return nil, FINISHED
	case <-w.modeChange:
		w.coord.changeACK[w.ID] <- true
		w.mode = <-w.modeChan
		w.E = w.ExecPool[w.mode]
	case action := <-w.actionTrans:
		s := w.coord.store
		if action.actionType == INDEX_ACTION_MERGE {
			//s.IndexMerge(w.iaAR, action.start, action.end, w.partitioner)
			s.IndexMerge(w.preIAAR, action.start, action.end, w.partitioner)
		} else {
			//s.IndexPartition(w.iaAR, action.start, action.end, w.partitioner)
			s.IndexPartition(w.preIAAR, action.start, action.end, w.partitioner)
		}
		w.coord.indexActionACK[w.ID] <- true
	default:
	}

	if t.GetTXN() == -1 { // Dummy Trans
		return nil, nil
	}

	w.Lock()

	if w.needLock {
		// Acquire all locks
		s := w.store

		ap = t.GetAccessParts()
		w.NLockAcquire += int64(len(ap))

		if len(ap) > 1 {
			w.NStats[NCROSSTXN]++
		}

		for _, p := range ap {
			s.spinLock[p].Lock()
		}
	}

	if *SysType == ADAPTIVE {
		if t.isHome() {
			w.st.sampleCount++
			if w.st.sampleCount >= w.st.sampleRate {
				//w.riMaster.txnSample++
				w.st.sampleCount = 0
				w.st.onePartSample(t.GetAccessParts(), w.riMaster, w.ID)
			}
		}
		w.st.trueCounter++
		if w.st.trueCounter == PARTVARRATE {
			w.st.trueCounter = 0
			if w.st.isPartition {
				w.riMaster.partStat[w.ID]++
			} else {
				//for _, p := range t.GetAccessParts() {
				//	w.riMaster.partStat[p]++
				//}
				w.riMaster.partStat[t.getHome()]++
			}
		}
	}

	r, err := w.doTxn(t)

	if w.needLock {
		s := w.store
		for _, p := range ap {
			s.spinLock[p].Unlock()
		}
	}

	w.Unlock()

	//w.NExecute += time.Since(w.start)

	return r, err
}

func (w *Worker) Store() *Store {
	return w.store
}

func (w *Worker) ResetTID(bigger TID) {
	big := bigger >> 16
	if big < w.next {
		clog.Error("%v How is supposedly bigger TID %v smaller than %v\n", w.ID, big, w.next)
	}
	w.next = TID(big + 1)
}

func (w *Worker) nextTID() TID {
	w.next++
	x := uint64(w.next<<16) | uint64(w.ID)<<8 | uint64(w.next%CHUNKS)
	return TID(x)
}

func (w *Worker) commitTID() TID {
	return w.nextTID() | w.epoch
}
