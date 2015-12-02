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
	NENOKEY
	NTXN
	NCROSSTXN
	NREADKEYS
	NWRITEKEYS
	LAST_STAT
)

type TransactionFunc func(*Query, ETransaction) (*Result, error)

type Worker struct {
	padding      [128]byte
	ID           int
	next         TID
	epoch        TID
	store        *Store
	E            ETransaction
	txns         []TransactionFunc
	NStats       []int64
	NGen         time.Duration
	NExecute     time.Duration
	NExecSqrt    time.Duration
	NWait        time.Duration
	NCrossWait   time.Duration
	NLockAcquire int64
	padding2     [128]byte
}

func (w *Worker) Register(fn int, transaction TransactionFunc) {
	w.txns[fn] = transaction
}

func NewWorker(id int, s *Store) *Worker {
	w := &Worker{
		ID:     id,
		store:  s,
		txns:   make([]TransactionFunc, LAST_TXN),
		NStats: make([]int64, LAST_STAT),
	}

	if *SysType == PARTITION {
		w.E = StartPTransaction(w)
	} else if *SysType == OCC {
		w.E = StartOTransaction(w)
	} else {
		clog.Error("OCC and 2PL not supported yet")
	}

	w.Register(ADD_ONE, AddOneTXN)
	w.Register(RANDOM_UPDATE_INT, UpdateIntTXN)
	w.Register(RANDOM_UPDATE_STRING, UpdateStringTXN)

	return w
}

func (w *Worker) doTxn(q *Query) (*Result, error) {
	if q.TXN >= LAST_TXN {
		debug.PrintStack()
		clog.Error("Unknown transaction number %v\n", q.TXN)
	}
	//w.NStats[NTXN]++

	if len(q.accessParts) > 1 {
		w.NStats[NCROSSTXN]++
	}

	w.E.Reset(q)

	x, err := w.txns[q.TXN](q, w.E)

	if err == EABORT {
		w.NStats[NABORTS]++
		return nil, err
	} else if err == ENOKEY {
		w.NStats[NENOKEY]++
		return nil, err
	}

	w.NStats[NREADKEYS] += int64(len(q.rKeys))
	w.NStats[NWRITEKEYS] += int64(len(q.wKeys))

	return x, err
}

func (w *Worker) One(q *Query) (*Result, error) {
	if *SysType == PARTITION {
		s := w.store
		w.NLockAcquire += int64(len(q.accessParts))
		tm := time.Now()
		// Acquire all locks
		for _, p := range q.accessParts {
			s.store[p].Lock()
		}

		w.NWait += time.Since(tm)
		if len(q.accessParts) > 1 {
			w.NCrossWait += time.Since(tm)
		}
	}

	r, err := w.doTxn(q)

	if *SysType == PARTITION {
		s := w.store
		for _, p := range q.accessParts {
			s.store[p].Unlock()
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
