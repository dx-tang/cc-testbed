package testbed

import (
	"runtime/debug"
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
	padding      [PADDING]byte
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
	reportInfo   ReportInfo
	padding2     [PADDING]byte
}

func (w *Worker) Register(fn int, transaction TransactionFunc) {
	w.txns[fn] = transaction
}

func NewWorker(id int, s *Store, c *Coordinator, tableCount int, mode int) *Worker {
	w := &Worker{
		ID:         id,
		store:      s,
		coord:      c,
		txns:       make([]TransactionFunc, LAST_TXN),
		NStats:     make([]int64, LAST_STAT),
		mode:       mode,
		reportInfo: ReportInfo{},
	}

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

	return w
}

func (w *Worker) Start() {
	w.start = time.Now()
	w.end = w.start.Add(time.Duration(REPORTPERIOD) * time.Millisecond)
}

func (w *Worker) Finish() {
	close(w.coord.reports[w.ID])
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
	if !w.end.After(w.start) {
		if *SysType == ADAPTIVE {
			/*w.reportInfo.NExecute = w.NExecute - w.reportInfo.NExecute
			w.reportInfo.txn = w.NStats[NTXN] - w.reportInfo.txn
			w.reportInfo.aborts = w.NStats[NABORTS] - w.reportInfo.aborts*/
			w.reportInfo.execTime = w.NExecute - w.reportInfo.prevExec
			w.reportInfo.txn = w.NStats[NTXN] - w.reportInfo.prevTxn
			w.reportInfo.aborts = w.NStats[NABORTS] - w.reportInfo.prevAborts

			w.reportInfo.prevExec = w.NExecute
			w.reportInfo.prevTxn = w.NStats[NTXN]
			w.reportInfo.prevAborts = w.NStats[NABORTS]

			//clog.Info("Sys Type %v aborts %v total %v\n", w.E.GetType(), w.reportInfo.aborts, w.NStats[NABORTS])

			w.coord.reports[w.ID] <- &w.reportInfo
			w.mode = <-w.coord.mode[w.ID]
			w.E = w.ExecPool[w.mode]

		} else {
			w.reportInfo.execTime = w.NExecute - w.reportInfo.prevExec
			w.reportInfo.txn = w.NStats[NTXN] - w.reportInfo.prevTxn
			w.reportInfo.aborts = w.NStats[NABORTS] - w.reportInfo.prevAborts

			w.reportInfo.prevExec = w.NExecute
			w.reportInfo.prevTxn = w.NStats[NTXN]
			w.reportInfo.prevAborts = w.NStats[NABORTS]
			w.coord.reports[w.ID] <- &w.reportInfo
		}

		w.end = time.Now().Add(time.Duration(REPORTPERIOD) * time.Millisecond)
	}

	if (*SysType == ADAPTIVE && w.mode == PARTITION) || *SysType == PARTITION {
		s := w.store
		// Acquire all locks

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
