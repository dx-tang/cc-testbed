package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
)

type ETransaction interface {
	Reset(q *Query)
	Read(k Key, partNum int) (Record, error)
	WriteInt64(k Key, intValue int64, partNum int) error
	WriteString(k Key, sa *StrAttr, partNum int) error
	Abort() TID
	Commit() TID
	Store() *Store
	Worker() *Worker
}

// Partition Transaction Implementation
type PTransaction struct {
	padding0 [128]byte
	w        *Worker
	s        *Store
	padding  [128]byte
}

func StartPTransaction(w *Worker) *PTransaction {
	tx := &PTransaction{
		w: w,
		s: w.store,
	}
	return tx
}

func (p *PTransaction) Reset(q *Query) {

}

func (p *PTransaction) Read(k Key, partNum int) (Record, error) {
	r := p.s.GetRecord(k, partNum)
	if r == nil {
		return nil, ENOKEY
	}
	return r, nil
}

func (p *PTransaction) WriteInt64(k Key, intValue int64, partNum int) error {
	s := p.s
	success := s.SetRecord(k, intValue, partNum)
	if !success {
		return ENOKEY
	}
	return nil
}

func (p *PTransaction) WriteString(k Key, sa *StrAttr, partNum int) error {
	s := p.s
	success := s.SetRecord(k, sa, partNum)
	if !success {
		return ENOKEY
	}
	return nil
}

func (p *PTransaction) Abort() TID {
	return 0
}

func (p *PTransaction) Commit() TID {
	return 1
}

func (p *PTransaction) Store() *Store {
	return p.s
}

func (p *PTransaction) Worker() *Worker {
	return p.w
}

type WriteKey struct {
	key     Key
	partNum int
	v       Value
	locked  bool
	rec     Record
}

type ReadKey struct {
	key  Key
	last TID
	rec  Record
}

// Silo OCC Transaction Implementation
type OTransaction struct {
	padding0 [128]byte
	w        *Worker
	s        *Store
	rKeys    []ReadKey
	wKeys    []WriteKey
	maxSeen  TID
	padding  [128]byte
}

func StartOTransaction(w *Worker) *OTransaction {
	tx := &OTransaction{
		w: w,
		s: w.store,
	}
	return tx
}

func (o *OTransaction) Reset(q *Query) {
	o.rKeys = make([]ReadKey, 0, len(q.rKeys)+len(q.wKeys))
	o.wKeys = make([]WriteKey, 0, len(q.wKeys))
}

func (o *OTransaction) Read(k Key, partNum int) (Record, error) {
	r := o.s.GetRecord(k, partNum)
	if r == nil {
		return nil, ENOKEY
	}

	ok, tid := r.IsUnlocked()

	if !ok {
		return nil, EABORT
	}

	n := len(o.rKeys)
	o.rKeys = o.rKeys[0 : n+1]
	o.rKeys[n].key = k
	o.rKeys[n].last = tid
	o.rKeys[n].rec = r

	if tid > o.maxSeen {
		o.maxSeen = tid
	}

	return r, nil
}

func (o *OTransaction) WriteInt64(k Key, intValue int64, partNum int) error {
	r := o.Store().GetRecord(k, partNum)

	if r == nil {
		return ENOKEY
	}

	// Read this record
	ok, tid := r.IsUnlocked()

	if !ok {
		return EABORT
	}

	n := len(o.rKeys)
	o.rKeys = o.rKeys[0 : n+1]
	o.rKeys[n].key = k
	o.rKeys[n].last = tid
	o.rKeys[n].rec = r

	if tid > o.maxSeen {
		o.maxSeen = tid
	}

	// Store this write
	n = len(o.wKeys)
	o.wKeys = o.wKeys[0 : n+1]
	o.wKeys[n].key = k
	o.wKeys[n].v = intValue
	o.wKeys[n].partNum = partNum
	o.wKeys[n].locked = false
	o.wKeys[n].rec = r

	return nil
}

func (o *OTransaction) WriteString(k Key, sa *StrAttr, partNum int) error {
	r := o.Store().GetRecord(k, partNum)

	if r == nil {
		return ENOKEY
	}

	// Read this record
	ok, tid := r.IsUnlocked()

	if !ok {
		return EABORT
	}

	n := len(o.rKeys)
	o.rKeys = o.rKeys[0 : n+1]
	o.rKeys[n].key = k
	o.rKeys[n].last = tid
	o.rKeys[n].rec = r

	if tid > o.maxSeen {
		o.maxSeen = tid
	}

	// Store this write
	n = len(o.wKeys)
	o.wKeys = o.wKeys[0 : n+1]
	o.wKeys[n].key = k
	o.wKeys[n].v = sa
	o.wKeys[n].partNum = partNum
	o.wKeys[n].locked = false
	o.rKeys[n].rec = r

	return nil
}

func (o *OTransaction) Abort() TID {
	for i, _ := range o.wKeys {
		wk := &o.wKeys[i]
		if wk.locked {
			wk.rec.Unlock(0)
		}
	}
	return 0
}

func (o *OTransaction) Commit() TID {

	// Phase 1: Lock all write keys
	for i := range o.wKeys {
		wk := &o.wKeys[i]

		var former TID
		var ok bool
		if ok, former = wk.rec.Lock(); !ok {
			return o.Abort()
		}
		wk.locked = true
		if former > o.maxSeen {
			o.maxSeen = former
		}
	}

	tid := o.w.commitTID()
	if tid <= o.maxSeen {
		o.w.ResetTID(o.maxSeen)
		tid = o.w.commitTID()
		if tid < o.maxSeen {
			clog.Error("%v MaxSeen %v, reset TID but %v<%v", o.w.ID, o.maxSeen, tid, o.maxSeen)
		}
	}

	// Phase 2: Check conflicts
	for i := range o.rKeys {
		rk := &o.rKeys[i]

		//verify whether TID has changed
		var ok1, ok2 bool
		ok1, tid = rk.rec.IsUnlocked()
		if tid != rk.last {
			return o.Abort()
		}

		// Check whether read key is not in wKeys
		ok2 = true
		for j := range o.wKeys {
			wk := o.wKeys[j]
			if rk.key == wk.key {
				ok2 = false
			}
		}

		if ok1 && ok2 {
			return o.Abort()
		}
	}

	// Phase 3: Apply all writes
	for i := range o.wKeys {
		wk := &o.wKeys[i]
		wk.rec.UpdateValue(wk.v)
		wk.rec.Unlock(tid)
	}
	return tid
}

func (o *OTransaction) Store() *Store {
	return o.s
}

func (o *OTransaction) Worker() *Worker {
	return o.w
}
