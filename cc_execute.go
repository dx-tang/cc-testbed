package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
)

type ETransaction interface {
	Reset(q *Query)
	Read(k Key, partNum int, force bool) (Record, error)
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

func (p *PTransaction) Read(k Key, partNum int, force bool) (Record, error) {
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
	partNum int
	v       Value
	locked  bool
	rec     Record
}

type ReadKey struct {
	last TID
	rec  Record
}

// Silo OCC Transaction Implementation
type OTransaction struct {
	padding0    [128]byte
	w           *Worker
	s           *Store
	rKeys       map[Key]*ReadKey
	wKeys       map[Key]*WriteKey
	dummyRecord *DRecord
	maxSeen     TID
	padding     [128]byte
}

func StartOTransaction(w *Worker) *OTransaction {
	tx := &OTransaction{
		w:           w,
		s:           w.store,
		dummyRecord: &DRecord{},
	}
	return tx
}

func (o *OTransaction) Reset(q *Query) {
	o.rKeys = make(map[Key]*ReadKey, len(q.rKeys)+len(q.wKeys))
	o.wKeys = make(map[Key]*WriteKey, len(q.wKeys))
}

func (o *OTransaction) Read(k Key, partNum int, force bool) (Record, error) {
	if !force {
		wk, ok := o.wKeys[k]
		if ok {
			ok, _ = wk.rec.IsUnlocked()
			if !ok {
				return nil, EABORT
			}
			o.dummyRecord.UpdateValue(wk.v)
			return o.dummyRecord, nil
		}
	}

	r := o.s.GetRecord(k, partNum)
	if r == nil {
		return nil, ENOKEY
	}

	var ok bool
	var tid TID
	ok, tid = r.IsUnlocked()

	if !ok {
		return nil, EABORT
	}

	_, ok = o.rKeys[k]

	if !ok {
		// Store this key
		readKey := &ReadKey{
			last: tid,
			rec:  r,
		}
		o.rKeys[k] = readKey
	}

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

	_, ok = o.rKeys[k]

	if !ok {
		// Store this key
		readKey := &ReadKey{
			last: tid,
			rec:  r,
		}
		o.rKeys[k] = readKey
	}

	if tid > o.maxSeen {
		o.maxSeen = tid
	}

	// Store this key
	writeKey := &WriteKey{
		partNum: partNum,
		v:       intValue,
		locked:  false,
		rec:     r,
	}
	o.wKeys[k] = writeKey

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

	_, ok = o.rKeys[k]

	if !ok {
		// Store this key
		readKey := &ReadKey{
			last: tid,
			rec:  r,
		}
		o.rKeys[k] = readKey
	}

	if tid > o.maxSeen {
		o.maxSeen = tid
	}

	// Store this key
	writeKey := &WriteKey{
		partNum: partNum,
		v:       sa,
		locked:  false,
		rec:     r,
	}
	o.wKeys[k] = writeKey

	return nil
}

func (o *OTransaction) Abort() TID {
	for _, wk := range o.wKeys {
		if wk.locked {
			wk.rec.Unlock(0)
		}
	}
	return 0
}

func (o *OTransaction) Commit() TID {

	// Phase 1: Lock all write keys
	for _, wk := range o.wKeys {
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
	for k, rk := range o.rKeys {

		//verify whether TID has changed
		var ok1, ok2 bool
		var tmpTID TID
		ok1, tmpTID = rk.rec.IsUnlocked()
		if tmpTID != rk.last {
			return o.Abort()
		}

		// Check whether read key is not in wKeys
		_, ok2 = o.wKeys[k]

		if !ok1 && !ok2 {
			return o.Abort()
		}
	}

	// Phase 3: Apply all writes
	for _, wk := range o.wKeys {
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
