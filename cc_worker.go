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
	E            ETransaction
	txns         []TransactionFunc
	NStats       []int64
	NGen         time.Duration
	NExecute     time.Duration
	NWait        time.Duration
	NCrossWait   time.Duration
	NLockAcquire int64
	padding2     [PADDING]byte
}

func (w *Worker) Register(fn int, transaction TransactionFunc) {
	w.txns[fn] = transaction
}

func NewWorker(id int, s *Store, tableCount int) *Worker {
	w := &Worker{
		ID:     id,
		store:  s,
		txns:   make([]TransactionFunc, LAST_TXN),
		NStats: make([]int64, LAST_STAT),
	}

	if *SysType == PARTITION {
		w.E = StartPTransaction(w, tableCount)
	} else if *SysType == OCC {
		w.E = StartOTransaction(w, tableCount)
	} else if *SysType == LOCKING {
		w.E = StartLTransaction(w, tableCount)
	} else {
		clog.Error("OCC and 2PL not supported yet")
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
	var ap []int
	if *SysType == PARTITION {
		s := w.store
		//tm := time.Now()
		// Acquire all locks

		ap = t.GetAccessParts()
		w.NLockAcquire += int64(len(ap))

		if len(ap) > 1 {
			w.NStats[NCROSSTXN]++
		}

		for _, p := range ap {
			//s.store[p].Lock()
			//s.locks[p].Lock()
			s.spinLock[p].Lock()
			//s.locks[p].custLock.Lock()
		}

		//w.NWait += time.Since(tm)
		//if len(q.accessParts) > 1 {
		//	w.NCrossWait += time.Since(tm)
		//}
	}

	r, err := w.doTxn(t)

	if *SysType == PARTITION {
		s := w.store
		for _, p := range ap {
			//s.store[p].Unlock()
			//s.locks[p].Unlock()
			s.spinLock[p].Unlock()
			//s.locks[p].custLock.Unlock()
		}
	}

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
