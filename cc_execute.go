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
	padding0 [64]byte
	w        *Worker
	s        *Store
	newValue int64
	padding  [64]byte
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
	r := p.s.GetRecord(k, partNum)
	pr := r.(*PRecord)
	//success := s.SetRecord(k, intValue, partNum)
	p.newValue = intValue
	success := pr.UpdateValue(&p.newValue)
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
	padding1 [64]byte
	k        Key
	partNum  int
	intVal   int64
	v        Value
	locked   bool
	rec      Record
	padding2 [64]byte
}

type ReadKey struct {
	padding1 [64]byte
	k        Key
	last     TID
	rec      Record
	padding2 [64]byte
}

// Silo OCC Transaction Implementation
type OTransaction struct {
	padding0 [64]byte
	w        *Worker
	s        *Store
	//rKeys       map[Key]*ReadKey
	//wKeys       map[Key]*WriteKey
	rKeys       []ReadKey
	wKeys       []WriteKey
	dummyRecord *DRecord
	maxSeen     TID
	padding     [64]byte
}

func StartOTransaction(w *Worker) *OTransaction {
	tx := &OTransaction{
		w:           w,
		s:           w.store,
		rKeys:       make([]ReadKey, 0, 100),
		wKeys:       make([]WriteKey, 0, 100),
		dummyRecord: &DRecord{},
	}
	return tx
}

func (o *OTransaction) Reset(q *Query) {
	//o.rKeys = make(map[Key]*ReadKey, len(q.rKeys)+len(q.wKeys))
	//o.wKeys = make(map[Key]*WriteKey, len(q.wKeys))
	o.rKeys = o.rKeys[:0]
	o.wKeys = o.wKeys[:0]
}

func (o *OTransaction) Read(k Key, partNum int, force bool) (Record, error) {
	if !force {
		for i := 0; i < len(o.wKeys); i++ {
			wk := &o.wKeys[i]
			if wk.k == k {
				ok, _ := wk.rec.IsUnlocked()
				if !ok {
					o.w.NStats[NREADABORTS]++
					return nil, EABORT
				}
				o.dummyRecord.UpdateValue(&wk.intVal)
				return o.dummyRecord, nil
			}
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
		o.w.NStats[NREADABORTS]++
		return nil, EABORT
	}

	//_, ok = o.rKeys[k]
	ok = false
	for j := 0; j < len(o.rKeys); j++ {
		rk := &o.rKeys[j]
		if rk.k == k {
			ok = true
			break
		}
	}

	if !ok {
		// Store this key
		/*readKey := &ReadKey{
			last: tid,
			rec:  r,
		}
		o.rKeys[k] = readKey*/
		n := len(o.rKeys)
		o.rKeys = o.rKeys[0 : n+1]
		o.rKeys[n].k = k
		o.rKeys[n].last = tid
		o.rKeys[n].rec = r
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
		o.w.NStats[NREADABORTS]++
		return EABORT
	}

	//_, ok = o.rKeys[k]
	ok = false
	for j := 0; j < len(o.rKeys); j++ {
		rk := &o.rKeys[j]
		if rk.k == k {
			ok = true
			break
		}
	}

	if !ok {
		// Store this key
		/*readKey := &ReadKey{
			last: tid,
			rec:  r,
		}
		o.rKeys[k] = readKey*/
		n := len(o.rKeys)
		o.rKeys = o.rKeys[0 : n+1]
		o.rKeys[n].k = k
		o.rKeys[n].last = tid
		o.rKeys[n].rec = r
	}

	if tid > o.maxSeen {
		o.maxSeen = tid
	}

	// Store this key
	/*
		writeKey := &WriteKey{
			partNum: partNum,
			v:       intValue,
			locked:  false,
			rec:     r,
		}
		o.wKeys[k] = writeKey*/
	n := len(o.wKeys)
	o.wKeys = o.wKeys[0 : n+1]
	o.wKeys[n].k = k
	o.wKeys[n].partNum = partNum
	o.wKeys[n].intVal = intValue
	o.wKeys[n].locked = false
	o.wKeys[n].rec = r

	return nil
}

func (o *OTransaction) WriteString(k Key, sa *StrAttr, partNum int) error {
	return nil
	/*
		r := o.Store().GetRecord(k, partNum)

		if r == nil {
			return ENOKEY
		}

		// Read this record
		ok, tid := r.IsUnlocked()

		if !ok {
			o.w.NStats[NREADABORTS]++
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
	*/
}

func (o *OTransaction) Abort() TID {
	/*for _, wk := range o.wKeys {
		if wk.locked {
			wk.rec.Unlock(0)
		}
	}*/
	for i := 0; i < len(o.wKeys); i++ {
		wk := &o.wKeys[i]
		if wk.locked {
			wk.rec.Unlock(0)
		}
	}
	return 0
}

func (o *OTransaction) Commit() TID {

	// Phase 1: Lock all write keys
	//for _, wk := range o.wKeys {
	for i := 0; i < len(o.wKeys); i++ {
		wk := &o.wKeys[i]
		var former TID
		var ok bool
		if ok, former = wk.rec.Lock(); !ok {
			o.w.NStats[NLOCKABORTS]++
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
	//for k, rk := range o.rKeys {
	for i := 0; i < len(o.rKeys); i++ {
		k := o.rKeys[i].k
		rk := &o.rKeys[i]
		//verify whether TID has changed
		var ok1, ok2 bool
		var tmpTID TID
		ok1, tmpTID = rk.rec.IsUnlocked()
		if tmpTID != rk.last {
			o.w.NStats[NRCHANGEABORTS]++
			return o.Abort()
		}

		// Check whether read key is not in wKeys
		//_, ok2 = o.wKeys[k]
		ok2 = false
		for j := 0; j < len(o.wKeys); j++ {
			wk := &o.wKeys[j]
			if wk.k == k {
				ok2 = true
				break
			}
		}

		if !ok1 && !ok2 {
			o.w.NStats[NRWABORTS]++
			return o.Abort()
		}
	}

	// Phase 3: Apply all writes
	for i, _ := range o.wKeys {
		wk := &o.wKeys[i]
		//wk.rec.UpdateValue(wk.v)
		wk.rec.UpdateValue(&wk.intVal)
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
