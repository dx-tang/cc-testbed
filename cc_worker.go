package testbed

import (
	"runtime/debug"

	"github.com/totemtang/cc-testbed/clog"
)

type TransactionFunc func(*Query, ETransaction) (*Result, error)

type Worker struct {
	padding  [128]byte
	ID       int
	store    *Store
	E        ETransaction
	txns     []TransactionFunc
	padding2 [128]byte
}

func (w *Worker) Register(fn int, transaction TransactionFunc) {
	w.txns[fn] = transaction
}

func NewWorker(id int, s *Store) *Worker {
	w := &Worker{
		ID:    id,
		store: s,
		txns:  make([]TransactionFunc, LAST_TXN),
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
	w.E.Reset()

	x, err := w.txns[q.TXN](q, w.E)

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
