package testbed

import (
	"runtime/debug"

	"github.com/totemtang/cc-testbed/clog"
)

const (
	NABORTS = iota
	NENOKEY
	NTXN
	NCROSSTXN
	NREADKEYS
	NWRITEKEYS
	LAST_STAT
)

type TransactionFunc func(*Query, ETransaction) (*Result, error)

type Worker struct {
	padding  [128]byte
	ID       int
	store    *Store
	E        ETransaction
	txns     []TransactionFunc
	NStats   []int64
	padding2 [128]byte
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
	w.NStats[NTXN]++

	if len(q.accessParts) > 1 {
		w.NStats[NCROSSTXN]++
	}

	w.NStats[NREADKEYS] += int64(len(q.rKeys))
	w.NStats[NWRITEKEYS] += int64(len(q.wKeys))

	w.E.Reset()

	x, err := w.txns[q.TXN](q, w.E)

	if err == EABORT {
		w.NStats[NABORTS]++
		return nil, err
	} else if err == ENOKEY {
		w.NStats[NENOKEY]++
		return nil, err
	}

	w.E.Commit()

	return x, err
}

func (w *Worker) One(q *Query) (*Result, error) {
	s := w.store
	// Acquire all locks
	for i := range q.accessParts {
		s.store[i].Lock()
	}
	r, err := w.doTxn(q)
	for i := range q.accessParts {
		s.store[i].Unlock()
	}
	return r, err
}

func (w *Worker) Store() *Store {
	return w.store
}
