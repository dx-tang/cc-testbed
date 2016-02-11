package testbed

import (
	"runtime/debug"
	"sync"
	"time"

	"github.com/totemtang/cc-testbed/clog"
)

const (
	NABORTS = iota
	NREADABORTS
	NLOCKABORTS
	NRCHANGEABORTS
	NRWABORTS
	NRLOCKABORTS
	NWLOCKABORTS
	NUPGRADEABORTS
	NENOKEY
	NTXN
	NCROSSTXN
	LAST_STAT
)

type TransactionFunc func(Trans, ETransaction) (Value, error)

type Worker struct {
	padding [PADDING]byte
	sync.RWMutex
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
	NWait        time.Duration
	NCrossWait   time.Duration
	NLockAcquire int64
	start        time.Time
	end          time.Time
	mode         int
	done         chan bool
	modeChange   chan bool
	modeChan     chan int
	riMaster     *ReportInfo
	riReplica    *ReportInfo
	st           *SampleTool
	padding2     [PADDING]byte
}

func (w *Worker) Register(fn int, transaction TransactionFunc) {
	w.txns[fn] = transaction
}

func NewWorker(id int, s *Store, c *Coordinator, tableCount int, mode int, sampleRate int, IDToKeyRange [][]int64) *Worker {
	w := &Worker{
		ID:         id,
		store:      s,
		coord:      c,
		txns:       make([]TransactionFunc, LAST_TXN),
		NStats:     make([]int64, LAST_STAT),
		mode:       mode,
		done:       make(chan bool),
		modeChange: make(chan bool),
		modeChan:   make(chan int),
	}

	kr := make([][]int64, len(IDToKeyRange))
	for i := 0; i < len(IDToKeyRange); i++ {
		kr[i] = make([]int64, len(IDToKeyRange[i]))
		for j := 0; j < len(IDToKeyRange[i]); j++ {
			kr[i][j] = IDToKeyRange[i][j]
		}
	}
	w.st = NewSampleTool(s.nParts, kr, sampleRate)
	w.riMaster = NewReportInfo(s.nParts, tableCount, sampleRate)
	w.riReplica = NewReportInfo(s.nParts, tableCount, sampleRate)

	if *SysType == PARTITION {
		w.E = StartPTransaction(w, tableCount)
	} else if *SysType == OCC {
		w.E = StartOTransaction(w, tableCount)
	} else if *SysType == LOCKING {
		w.E = StartLTransaction(w, tableCount)
	} else if *SysType == ADAPTIVE {
		w.ExecPool = make([]ETransaction, ADAPTIVE-PARTITION)
		w.ExecPool[PARTITION] = StartPTransaction(w, tableCount)
		w.ExecPool[OCC] = StartOTransaction(w, tableCount)
		w.ExecPool[LOCKING] = StartLTransaction(w, tableCount)
		w.E = w.ExecPool[mode]
	} else {
		clog.Error("System Type %v Not Supported Yet\n", *SysType)
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

	if *Report {
		go w.run()
	}

	return w
}

func (w *Worker) run() {
	coord := w.coord
	duration := time.Duration(REPORTPERIOD) * time.Millisecond
	tm := time.NewTicker(duration).C
	for {
		select {
		case <-w.done:
			return
		case <-w.modeChange:
			w.Lock()
			coord.changeACK[w.ID] <- true
			w.mode = <-w.modeChan
			w.E = w.ExecPool[w.mode]
			w.Unlock()
		case <-tm: // Report Information within One Period
			w.Lock()
			replica := w.riMaster
			w.riMaster = w.riReplica
			w.riReplica = replica
			if *SysType == ADAPTIVE {
				replica.execTime = w.NExecute - w.riMaster.prevExec
				replica.txn = w.NStats[NTXN] - w.riMaster.prevTxn
				replica.aborts = w.NStats[NABORTS] - w.riMaster.prevAborts

				replica.prevExec = w.NExecute
				replica.prevTxn = w.NStats[NTXN]
				replica.prevAborts = w.NStats[NABORTS]

				w.coord.reports[w.ID] <- replica
			} else {
				replica.execTime = w.NExecute - w.riMaster.prevExec
				replica.txn = w.NStats[NTXN] - w.riMaster.prevTxn
				replica.aborts = w.NStats[NABORTS] - w.riMaster.prevAborts

				replica.prevExec = w.NExecute
				replica.prevTxn = w.NStats[NTXN]
				replica.prevAborts = w.NStats[NABORTS]

				w.coord.reports[w.ID] <- replica
			}
			w.Unlock()
		}
	}
}

func (w *Worker) Start() {
}

func (w *Worker) Finish() {

}

func (w *Worker) SetMode(mode int) {
	if *SysType != ADAPTIVE {
		clog.Error("None Adaptive Mode Not Support Mode Change\n")
	}
	w.mode = mode
	w.E = w.ExecPool[mode]
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
	w.start = time.Now()
	var ap []int

	w.Lock()

	if *SysType == ADAPTIVE {
		w.st.onePartSample(t.GetAccessParts(), w.riMaster)
	}

	if (*SysType == ADAPTIVE && w.mode == PARTITION) || *SysType == PARTITION {
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

	r, err := w.doTxn(t)

	if (*SysType == ADAPTIVE && w.mode == PARTITION) || *SysType == PARTITION {
		s := w.store
		for _, p := range ap {
			s.spinLock[p].Unlock()
		}
	}

	w.NExecute += time.Since(w.start)

	w.Unlock()

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
