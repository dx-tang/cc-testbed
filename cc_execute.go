package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
)

const (
	MAXTABLENUM    = 20
	MAXCOLUMN      = 20
	MAXTRACKINGKEY = 100
)

type ETransaction interface {
	Reset(t Trans)
	ReadValue(tableID int, k Key, partNum int, colNum int) (Value, error)
	WriteValue(tableID int, k Key, partNum int, value Value, colNum int) error
	Abort() TID
	Commit() TID
	Store() *Store
	Worker() *Worker
}

// Partition Transaction Implementation
type PTransaction struct {
	padding0 [PADDING]byte
	w        *Worker
	s        *Store
	padding  [PADDING]byte
}

func StartPTransaction(w *Worker) *PTransaction {
	tx := &PTransaction{
		w: w,
		s: w.store,
	}
	return tx
}

func (p *PTransaction) Reset(t Trans) {

}

func (p *PTransaction) ReadValue(tableID int, k Key, partNum int, colNum int) (Value, error) {
	s := p.s
	v := s.GetValueByID(tableID, k, partNum, colNum)
	if v == nil {
		return nil, ENOKEY
	}
	return v, nil
}

func (p *PTransaction) WriteValue(tableID int, k Key, partNum int, value Value, colNum int) error {
	s := p.s
	success := s.SetValueByID(tableID, k, partNum, value, colNum)
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
	padding1 [PADDING]byte
	k        Key
	partNum  int
	vals     []Value
	cols     []int
	locked   bool
	rec      Record
	padding2 [PADDING]byte
}

type ReadKey struct {
	padding1 [PADDING]byte
	k        Key
	last     TID
	rec      Record
	padding2 [PADDING]byte
}

type TrackTable struct {
	padding1 [PADDING]byte
	tableID  int
	rKeys    []ReadKey
	wKeys    []WriteKey
	padding2 [PADDING]byte
}

// Silo OCC Transaction Implementation
type OTransaction struct {
	padding0    [PADDING]byte
	w           *Worker
	s           *Store
	tt          []TrackTable
	dummyRecord *DRecord
	maxSeen     TID
	padding     [PADDING]byte
}

func StartOTransaction(w *Worker) *OTransaction {
	tx := &OTransaction{
		w:           w,
		s:           w.store,
		tt:          make([]TrackTable, MAXTABLENUM),
		dummyRecord: &DRecord{},
	}

	for j := 0; j < len(tx.tt); j++ {
		t := &tx.tt[j]
		t.rKeys = make([]ReadKey, 0, 100)
		t.wKeys = make([]WriteKey, 100)
		for i := 0; i < len(t.wKeys); i++ {
			wk := &t.wKeys[i]
			wk.vals = make([]Value, 0, 10+2*PADDINGINT64)
			wk.vals = wk.vals[PADDINGINT64:PADDINGINT64]
			wk.cols = make([]int, 0, 10+2*PADDINGINT)
			wk.cols = wk.cols[PADDINGINT:PADDINGINT]
		}
		t.wKeys = t.wKeys[:0]
	}

	tx.tt = tx.tt[:0]

	return tx
}

func (o *OTransaction) Reset(t Trans) {

	for j := 0; j < len(o.tt); j++ {
		t := &o.tt[j]
		t.rKeys = t.rKeys[:0]
		for i := 0; i < len(t.wKeys); i++ {
			wk := &t.wKeys[i]
			wk.vals = wk.vals[:0]
			wk.cols = wk.cols[:0]
		}
		t.wKeys = t.wKeys[:0]
	}

	o.tt = o.tt[:0]

}

func (o *OTransaction) ReadValue(tableID int, k Key, partNum int, colNum int) (Value, error) {

	r := o.s.GetRecByID(tableID, k, partNum)
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

	ok = false
	for i := 0; i < len(o.tt); i++ {
		t := &o.tt[i]
		if t.tableID == tableID {
			for j := 0; j < len(t.rKeys); j++ {
				rk := &t.rKeys[j]
				if rk.k == k {
					ok = true
					break
				}
			}
			if !ok {
				n := len(t.rKeys)
				t.rKeys = t.rKeys[0 : n+1]
				t.rKeys[n].k = k
				t.rKeys[n].last = tid
				t.rKeys[n].rec = r
				ok = true
			}
		}
	}

	if !ok {
		// Store this key
		n := len(o.tt)
		o.tt = o.tt[0 : n+1]
		o.tt[n].tableID = tableID
		o.tt[n].rKeys = o.tt[n].rKeys[0:1]
		o.tt[n].rKeys[0].k = k
		o.tt[n].rKeys[0].last = tid
		o.tt[n].rKeys[0].rec = r
	}

	if tid > o.maxSeen {
		o.maxSeen = tid
	}

	return r.GetValue(colNum), nil
}

func (o *OTransaction) WriteValue(tableID int, k Key, partNum int, value Value, colNum int) error {

	r := o.s.GetRecByID(tableID, k, partNum)

	if r == nil {
		return ENOKEY
	}

	// Read this record
	ok, tid := r.IsUnlocked()

	if !ok {
		o.w.NStats[NREADABORTS]++
		return EABORT
	}

	var t *TrackTable
	ok = false
	for i := 0; i < len(o.tt); i++ {
		t = &o.tt[i]
		if t.tableID == tableID {
			for j := 0; j < len(t.rKeys); j++ {
				rk := &t.rKeys[j]
				if rk.k == k {
					ok = true
					break
				}
			}
			if !ok {
				n := len(t.rKeys)
				t.rKeys = t.rKeys[0 : n+1]
				t.rKeys[n].k = k
				t.rKeys[n].last = tid
				t.rKeys[n].rec = r
				ok = true
			}
			break
		}
	}

	if !ok {
		// Store this key
		n := len(o.tt)
		o.tt = o.tt[0 : n+1]
		t = &o.tt[n]
		t.tableID = tableID
		t.rKeys = t.rKeys[0:1]
		t.rKeys[0].k = k
		t.rKeys[0].last = tid
		t.rKeys[0].rec = r
	}

	if tid > o.maxSeen {
		o.maxSeen = tid
	}

	ok = false
	for j := 0; j < len(t.wKeys); j++ {
		wk := &t.wKeys[j]
		if wk.k == k {
			ok = true
			n := len(wk.vals)
			wk.vals = wk.vals[0 : n+1]
			wk.vals[n] = value
			wk.cols = wk.cols[0 : n+1]
			wk.cols[n] = colNum
			break
		}
	}
	if !ok {
		n := len(t.wKeys)
		t.wKeys = t.wKeys[0 : n+1]
		t.wKeys[n].k = k
		t.wKeys[n].partNum = partNum
		t.wKeys[n].locked = false
		t.wKeys[n].rec = r

		t.wKeys[n].vals = t.wKeys[n].vals[0:1]
		t.wKeys[n].vals[0] = value
		t.wKeys[n].cols = t.wKeys[n].cols[0:1]
		t.wKeys[n].cols[0] = colNum
	}

	return nil
}

func (o *OTransaction) Abort() TID {
	for j := 0; j < len(o.tt); j++ {
		t := &o.tt[j]
		for i := 0; i < len(t.wKeys); i++ {
			wk := &t.wKeys[i]
			if wk.locked {
				wk.rec.Unlock(o.maxSeen)
			}
		}
	}

	return 0
}

func (o *OTransaction) Commit() TID {

	// Phase 1: Lock all write keys
	//for _, wk := range o.wKeys {

	for j := 0; j < len(o.tt); j++ {
		t := &o.tt[j]
		for i := 0; i < len(t.wKeys); i++ {
			wk := &t.wKeys[i]
			var former TID
			var ok bool
			ok, former = wk.rec.Lock()
			if !ok {
				o.w.NStats[NLOCKABORTS]++
				return o.Abort()
			}
			wk.locked = true
			if former > o.maxSeen {
				o.maxSeen = former
			}
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
	for j := 0; j < len(o.tt); j++ {
		t := &o.tt[j]
		for i := 0; i < len(t.rKeys); i++ {
			k := t.rKeys[i].k
			rk := &t.rKeys[i]
			//verify whether TID has changed
			var ok1, ok2 bool
			var tmpTID TID
			ok1, tmpTID = rk.rec.IsUnlocked()
			if tmpTID != rk.last {
				o.w.NStats[NRCHANGEABORTS]++
				return o.Abort()
			}

			// Check whether read key is not in wKeys
			ok2 = false
			for p := 0; p < len(t.wKeys); p++ {
				wk := &t.wKeys[p]
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
	}

	// Phase 3: Apply all writes
	for p := 0; p < len(o.tt); p++ {
		t := &o.tt[p]
		for i, _ := range t.wKeys {
			wk := &t.wKeys[i]
			for j := 0; j < len(wk.vals); j++ {
				wk.rec.SetValue(wk.vals[j], wk.cols[j])
			}

			wk.rec.Unlock(tid)
			wk.locked = false
		}
	}

	return tid
}

func (o *OTransaction) Store() *Store {
	return o.s
}

func (o *OTransaction) Worker() *Worker {
	return o.w
}

type WriteRec struct {
	padding1 [PADDING]byte
	k        Key
	partNum  int
	rec      Record
	vals     []Value
	cols     []int
	padding2 [PADDING]byte
}

type ReadRec struct {
	padding1 [PADDING]byte
	k        Key
	rec      Record
	exist    bool
	padding2 [PADDING]byte
}

type RecTable struct {
	padding1 [PADDING]byte
	rRecs    []ReadRec
	wRecs    []WriteRec
	padding2 [PADDING]byte
}

type LTransaction struct {
	padding0 [PADDING]byte
	w        *Worker
	s        *Store
	rt       []RecTable
	padding  [PADDING]byte
}

func StartLTransaction(w *Worker, nTables int) *LTransaction {
	tx := &LTransaction{
		w:  w,
		s:  w.store,
		rt: make([]RecTable, nTables),
	}

	for i := 0; i < len(tx.rt); i++ {
		t := &tx.rt[i]
		t.rRecs = make([]ReadRec, 0, MAXTRACKINGKEY)
		t.wRecs = make([]WriteRec, MAXTRACKINGKEY)
		for j, _ := range t.wRecs {
			wr := &t.wRecs[j]
			wr.vals = make([]Value, 0, MAXCOLUMN+2*PADDINGINT64)
			wr.vals = wr.vals[PADDINGINT64:PADDINGINT64]
			wr.cols = make([]int, 0, MAXCOLUMN+2*PADDINGINT)
			wr.cols = wr.cols[PADDINGINT:PADDINGINT]
		}
		t.wRecs = t.wRecs[0:0]
	}

	return tx
}

func (l *LTransaction) getWriteRec() *WriteRec {
	return nil
}

func (l *LTransaction) Reset(t Trans) {
}

func (l *LTransaction) ReadValue(tableID int, k Key, partNum int, colNum int) (Value, error) {

	var ok bool = false
	var wr *WriteRec
	var rr *ReadRec
	w := l.w

	rt := &l.rt[tableID]
	for i, _ := range rt.wRecs {
		if rt.wRecs[i].k == k {
			wr = &rt.wRecs[i]
			ok = true
			break
		}
	}
	// Has been Locked
	if ok {
		return wr.rec.GetValue(colNum), nil
	}

	var rec Record
	ok = false
	for i, _ := range rt.rRecs {
		if rt.rRecs[i].k == k {
			rr = &rt.rRecs[i]
			ok = true
			break
		}
	}
	// Has been RLocked
	if ok {
		return rr.rec.GetValue(colNum), nil
	}

	// Try RLock
	rec = l.s.GetRecByID(tableID, k, partNum)
	if rec == nil {
		l.Abort()
		return nil, ENOKEY
	}

	if !rec.RLock() {
		//clog.Info("Worker %v: Trans %v RLock Table %v; Key %v Failed\n", w.ID, w.NStats[NTXN], tableID, ParseKey(k, 0))
		/*
			clog.Info("Current Table %v; Key %v; NTXN %v\n", tableID, ParseKey(k, 0), w.NStats[NTXN])
			for i := 0; i < len(l.rt); i++ {
				t := &l.rt[i]
				clog.Info("Read Records of %v\n", i)
				for j := 0; j < len(t.rRecs); j++ {
					clog.Info("%v", ParseKey(t.rRecs[j].k, 0))
				}
				clog.Info("Write Records of %v\n", i)
				for j := 0; j < len(t.wRecs); j++ {
					clog.Info("%v", ParseKey(t.wRecs[j].k, 0))
				}
			}
			clog.Error("\n")
		*/
		w.NStats[NRLOCKABORTS]++
		l.Abort()
		return nil, EABORT
	}

	//clog.Info("Worker %v: Trans %v RLock Table %v; Key %v Success\n", w.ID, w.NStats[NTXN], tableID, ParseKey(k, 0))

	// Success, Record it
	n := len(rt.rRecs)
	rt.rRecs = rt.rRecs[:n+1]
	rt.rRecs[n].k = k
	rt.rRecs[n].rec = rec
	rt.rRecs[n].exist = true

	return rec.GetValue(colNum), nil
}

func (l *LTransaction) WriteValue(tableID int, k Key, partNum int, value Value, colNum int) error {

	var ok bool = false
	var wr *WriteRec
	var rr *ReadRec
	w := l.w

	rt := &l.rt[tableID]
	//wr, ok = rt.wRecs[k]
	for i, _ := range rt.wRecs {
		if rt.wRecs[i].k == k {
			wr = &rt.wRecs[i]
			ok = true
			break
		}
	}

	// Has been Locked
	if ok {
		n := len(wr.vals)
		wr.vals = wr.vals[0 : n+1]
		wr.vals[n] = value
		wr.cols = wr.cols[0 : n+1]
		wr.cols[n] = colNum
		return nil
	}

	var rec Record
	ok = false
	for i, _ := range rt.rRecs {
		if rt.rRecs[i].k == k {
			rr = &rt.rRecs[i]
			ok = true
			break
		}
	}
	// Has been RLocked
	if ok {
		rr.exist = false
		if rr.rec.Upgrade() {
			//clog.Info("Worker %v: Trans %v Upgrade table %v; Key %v Success\n", w.ID, w.NStats[NTXN], tableID, ParseKey(k, 0))
			n := len(rt.wRecs)
			rt.wRecs = rt.wRecs[0 : n+1]
			wr := &rt.wRecs[n]
			wr.k = k
			wr.partNum = partNum
			wr.rec = rr.rec
			wr.vals = wr.vals[0:1]
			wr.vals[0] = value
			wr.cols = wr.cols[0:1]
			wr.cols[0] = colNum
			return nil
		} else {
			//clog.Info("Worker %v: Trans %v Upgrade table %v; Key %v Failed\n", w.ID, w.NStats[NTXN], tableID, ParseKey(k, 0))
			w.NStats[NUPGRADEABORTS]++
			l.Abort()
			return EABORT
		}
	}

	rec = l.s.GetRecByID(tableID, k, partNum)
	if rec == nil {
		l.Abort()
		return ENOKEY
	}

	if rec.WLock() {
		n := len(rt.wRecs)
		rt.wRecs = rt.wRecs[0 : n+1]
		wr := &rt.wRecs[n]
		wr.k = k
		wr.partNum = partNum
		wr.rec = rec
		wr.vals = wr.vals[0:1]
		wr.vals[0] = value
		wr.cols = wr.cols[0:1]
		wr.cols[0] = colNum
		return nil
	} else {
		w.NStats[NWLOCKABORTS]++
		l.Abort()
		return EABORT
	}

}

func (l *LTransaction) Abort() TID {
	//w := l.w
	for i := 0; i < len(l.rt); i++ {
		t := &l.rt[i]
		for j, _ := range t.rRecs {
			rr := &t.rRecs[j]
			if rr.exist {
				//clog.Info("Worker %v: Trans %v RUnlock Table %v; Key %v\n", w.ID, w.NStats[NTXN], i, ParseKey(rr.k, 0))
				rr.rec.RUnlock()
			}
		}
		t.rRecs = t.rRecs[:0]
		for j, _ := range t.wRecs {
			wr := &t.wRecs[j]
			wr.vals = wr.vals[:0]
			wr.cols = wr.cols[:0]
			//clog.Info("Worker %v: Trans %v WUnlock Table %v; Key %v\n", w.ID, w.NStats[NTXN], i, ParseKey(wr.k, 0))
			wr.rec.WUnlock()
		}
		t.wRecs = t.wRecs[:0]
	}
	return 0
}

func (l *LTransaction) Commit() TID {
	//w := l.w

	for i := 0; i < len(l.rt); i++ {
		t := &l.rt[i]
		for j, _ := range t.rRecs {
			rr := &t.rRecs[j]
			if rr.exist {
				//clog.Info("Worker %v: Trans %v RUnlock Table %v; Key %v\n", w.ID, w.NStats[NTXN], i, ParseKey(rr.k, 0))
				rr.rec.RUnlock()
			}
		}
		t.rRecs = t.rRecs[:0]
		for j, _ := range t.wRecs {
			wr := &t.wRecs[j]
			for j := 0; j < len(wr.vals); j++ {
				wr.rec.SetValue(wr.vals[j], wr.cols[j])
			}
			wr.vals = wr.vals[:0]
			wr.cols = wr.cols[:0]
			//clog.Info("Worker %v: Trans %v WUnlock Table %v; Key %v\n", w.ID, w.NStats[NTXN], i, ParseKey(wr.k, 0))
			wr.rec.WUnlock()
		}
		t.wRecs = t.wRecs[:0]
	}

	return 1
}

func (l *LTransaction) Store() *Store {
	return l.s
}

func (l *LTransaction) Worker() *Worker {
	return l.w
}
