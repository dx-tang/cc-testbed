package testbed

import (
	"runtime/debug"
	"time"

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

type RWSets struct {
	rKeys  []Key
	rTIDs  []TID
	wKeys  []Key
	wValue WValue
}

type TransactionFunc func(*Query, ETransaction) (*Result, error)

type Worker struct {
	padding      [128]byte
	ID           int
	store        *Store
	E            ETransaction
	txns         []TransactionFunc
	NStats       []int64
	NGen         time.Duration
	NExecute     time.Duration
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

	//GenerateRWSets(w.Store(), q)

	x, err := w.txns[q.TXN](q, w.E)

	if err == EABORT {
		w.NStats[NABORTS]++
		return nil, err
	} else if err == ENOKEY {
		w.NStats[NENOKEY]++
		return nil, err
	}

	w.E.Commit(q)

	return x, err
}

func (w *Worker) One(q *Query) (*Result, error) {
	s := w.store

	w.NLockAcquire += int64(len(q.accessParts))
	tm := time.Now()
	// Acquire all locks
	for _, p := range q.accessParts {
		s.store[p].Lock()
	}
	//for i := len(q.accessParts) - 1; i >= 0; i-- {
	//	s.store[q.accessParts[i]].Lock()
	//}

	w.NWait += time.Since(tm)
	if len(q.accessParts) > 1 {
		w.NCrossWait += time.Since(tm)
	}
	r, err := w.doTxn(q)

	for _, p := range q.accessParts {
		s.store[p].Unlock()
	}

	//for i := len(q.accessParts) - 1; i >= 0; i-- {
	//	s.store[q.accessParts[i]].Unlock()
	//}

	return r, err
}

func (w *Worker) Store() *Store {
	return w.store
}

func GenerateRWSets(s *Store, q *Query) {
	rwSets := &RWSets{
		rKeys: make([]Key, len(q.rKeys)+len(q.wKeys)),
		//rTIDs: make([]TID, len(q.rKeys)+len(q.wKeys)),
		wKeys: make([]Key, len(q.wKeys)),
	}

	//for i := range rwSets.rKeys {
	//	rwSets.rKeys[i] = q.rKeys[i]
	//	br := s.GetRecord(q.rKeys[i], q.partitioner.GetPartition(q.rKeys[i]))
	//rwSets.rTIDs[i] = br.GetTID()
	//}

	/*for i := range rwSets.wKeys {
		rwSets.wKeys[i] = q.wKeys[i]
	}*/

	q.rwSets = rwSets
}
