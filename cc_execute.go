package testbed

import (
	"flag"
	"github.com/totemtang/cc-testbed/clog"
)

var Spec = flag.Bool("spec", false, "Whether Speculatively Indicate MayWrite")

const (
	MAXTABLENUM    = 20
	MAXCOLUMN      = 20
	MAXTRACKINGKEY = 100
)

type ETransaction interface {
	Reset(t Trans)
	ReadValue(tableID int, k Key, partNum int, colNum int, trial int) (Value, bool, error)
	WriteValue(tableID int, k Key, partNum int, value Value, colNum int, trial int) error
	MayWrite(tableID int, k Key, partNum int, trial int) error
	Abort() TID
	Commit() TID
	Store() *Store
	Worker() *Worker
	GetType() int
}

type PTrackTable struct {
	padding1 [PADDING]byte
	wRecs    []WriteRec
	padding2 [PADDING]byte
}

// Partition Transaction Implementation
type PTransaction struct {
	padding0 [PADDING]byte
	w        *Worker
	s        *Store
	st       *SampleTool
	tt       []PTrackTable
	padding  [PADDING]byte
}

func StartPTransaction(w *Worker, tableCount int) *PTransaction {
	tx := &PTransaction{
		w:  w,
		s:  w.store,
		st: w.st,
		tt: make([]PTrackTable, tableCount, MAXTABLENUM),
	}

	for i := 0; i < len(tx.tt); i++ {
		t := &tx.tt[i]
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

func (p *PTransaction) Reset(t Trans) {

}

func (p *PTransaction) ReadValue(tableID int, k Key, partNum int, colNum int, trial int) (Value, bool, error) {
	if *SysType == ADAPTIVE {
		p.st.oneSample(tableID, k, partNum, p.s, p.w.riMaster, true)
	}

	t := &p.tt[tableID]
	for i := 0; i < len(t.wRecs); i++ {
		wr := &t.wRecs[i]
		if wr.k == k {
			for p := len(wr.cols) - 1; p >= 0; p-- {
				if colNum == wr.cols[p] {
					return wr.vals[p], false, nil
				}
			}
			break
		}
	}

	s := p.s
	v := s.GetValueByID(tableID, k, partNum, colNum)
	if v == nil {
		return nil, true, ENOKEY
	}
	return v, true, nil
}

func (p *PTransaction) WriteValue(tableID int, k Key, partNum int, value Value, colNum int, trial int) error {
	if *SysType == ADAPTIVE {
		p.st.oneSample(tableID, k, partNum, p.s, p.w.riMaster, false)
	}

	t := &p.tt[tableID]
	for i := 0; i < len(t.wRecs); i++ {
		wr := &t.wRecs[i]
		if wr.k == k {
			n := len(wr.cols)
			wr.cols = wr.cols[0 : n+1]
			wr.cols[n] = colNum
			wr.vals = wr.vals[0 : n+1]
			wr.vals[n] = value
			return nil
		}
	}

	s := p.s
	r := s.GetRecByID(tableID, k, partNum)
	if r == nil {
		return ENOKEY
	}

	n := len(t.wRecs)
	t.wRecs = t.wRecs[0 : n+1]
	t.wRecs[n].k = k
	t.wRecs[n].partNum = partNum
	t.wRecs[n].rec = r
	t.wRecs[n].cols = t.wRecs[n].cols[0:1]
	t.wRecs[n].cols[0] = colNum
	t.wRecs[n].vals = t.wRecs[n].vals[0:1]
	t.wRecs[n].vals[0] = value
	return nil

}

func (p *PTransaction) MayWrite(tableID int, k Key, partNum int, trial int) error {
	return nil
}

func (p *PTransaction) Abort() TID {
	for i := 0; i < len(p.tt); i++ {
		t := &p.tt[i]
		for j := 0; j < len(t.wRecs); j++ {
			wr := &t.wRecs[j]
			wr.vals = wr.vals[:0]
			wr.cols = wr.cols[:0]
		}
		t.wRecs = t.wRecs[0:0]
	}
	return 0
}

func (p *PTransaction) Commit() TID {
	for i := 0; i < len(p.tt); i++ {
		t := &p.tt[i]
		for j := 0; j < len(t.wRecs); j++ {
			wr := &t.wRecs[j]
			for p := 0; p < len(wr.cols); p++ {
				wr.rec.SetValue(wr.vals[p], wr.cols[p])
			}
			wr.vals = wr.vals[:0]
			wr.cols = wr.cols[:0]
		}
		t.wRecs = t.wRecs[0:0]
	}
	return 1
}

func (p *PTransaction) Store() *Store {
	return p.s
}

func (p *PTransaction) Worker() *Worker {
	return p.w
}

func (p *PTransaction) GetType() int {
	return PARTITION
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
	padding0 [PADDING]byte
	w        *Worker
	s        *Store
	st       *SampleTool
	tt       []TrackTable
	maxSeen  TID
	padding  [PADDING]byte
}

func StartOTransaction(w *Worker, tableCount int) *OTransaction {
	tx := &OTransaction{
		w:  w,
		s:  w.store,
		st: w.st,
		tt: make([]TrackTable, tableCount, MAXTABLENUM),
	}

	for j := 0; j < len(tx.tt); j++ {
		t := &tx.tt[j]
		t.tableID = j
		t.rKeys = make([]ReadKey, 0, MAXTRACKINGKEY)
		t.wKeys = make([]WriteKey, MAXTRACKINGKEY)
		for i := 0; i < len(t.wKeys); i++ {
			wk := &t.wKeys[i]
			wk.vals = make([]Value, 0, MAXCOLUMN+2*PADDINGINT64)
			wk.vals = wk.vals[PADDINGINT64:PADDINGINT64]
			wk.cols = make([]int, 0, MAXCOLUMN+2*PADDINGINT)
			wk.cols = wk.cols[PADDINGINT:PADDINGINT]
		}
		t.wKeys = t.wKeys[0:0]
	}

	//tx.tt = tx.tt[:0]

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

	//o.tt = o.tt[:0]

}

func (o *OTransaction) ReadValue(tableID int, k Key, partNum int, colNum int, trial int) (Value, bool, error) {
	if *SysType == ADAPTIVE {
		o.st.oneSample(tableID, k, partNum, o.s, o.w.riMaster, true)
	}

	var ok bool
	var tid TID
	var t *TrackTable
	var r Record

	t = &o.tt[tableID]

	for j := 0; j < len(t.wKeys); j++ {
		wk := &t.wKeys[j]
		if wk.k == k {
			r = wk.rec
			for p := len(wk.cols) - 1; p >= 0; p-- {
				if colNum == wk.cols[p] {
					return wk.vals[p], false, nil
				}
			}
			break
		}
	}

	ok = false
	for j := 0; j < len(t.rKeys); j++ {
		rk := &t.rKeys[j]
		if rk.k == k {
			ok, _ = rk.rec.IsUnlocked()
			if !ok {
				o.w.NStats[NREADABORTS]++
				return nil, true, EABORT
			}
			return rk.rec.GetValue(colNum), true, nil
		}
	}

	if r == nil {
		r = o.s.GetRecByID(tableID, k, partNum)
		if r == nil {
			return nil, true, ENOKEY
		}
	}

	ok, tid = r.IsUnlocked()

	if tid > o.maxSeen {
		o.maxSeen = tid
	}

	if !ok {
		o.w.NStats[NREADABORTS]++
		return nil, true, EABORT
	}

	n := len(t.rKeys)
	t.rKeys = t.rKeys[0 : n+1]
	t.rKeys[n].k = k
	t.rKeys[n].last = tid
	t.rKeys[n].rec = r

	return r.GetValue(colNum), true, nil
}

func (o *OTransaction) WriteValue(tableID int, k Key, partNum int, value Value, colNum int, trial int) error {
	if *SysType == ADAPTIVE {
		o.st.oneSample(tableID, k, partNum, o.s, o.w.riMaster, false)
	}

	var ok bool
	//var tid TID
	var t *TrackTable
	var r Record

	ok = false
	t = &o.tt[tableID]
	for j := 0; j < len(t.wKeys); j++ {
		wk := &t.wKeys[j]
		if wk.k == k {
			ok = true
			r = wk.rec
			n := len(wk.vals)
			wk.vals = wk.vals[0 : n+1]
			wk.vals[n] = value
			wk.cols = wk.cols[0 : n+1]
			wk.cols[n] = colNum
			break
		}
	}

	if !ok {
		for j := 0; j < len(t.rKeys); j++ {
			rk := &t.rKeys[j]
			if rk.k == k {
				r = rk.rec
				break
			}
		}

		if r == nil {
			r = o.s.GetRecByID(tableID, k, partNum)

			if r == nil {
				return ENOKEY
			}
		}

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

	// Read this record
	/*ok, tid = r.IsUnlocked()

	if !ok {
		o.w.NStats[NREADABORTS]++
		return EABORT
	}

	if tid > o.maxSeen {
		o.maxSeen = tid
	}*/

	return nil
}

func (o *OTransaction) MayWrite(tableID int, k Key, partNum int, trial int) error {
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

func (o *OTransaction) GetType() int {
	return OCC
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
	st       *SampleTool
	rt       []RecTable
	padding  [PADDING]byte
}

func StartLTransaction(w *Worker, nTables int) *LTransaction {
	tx := &LTransaction{
		w:  w,
		s:  w.store,
		st: w.st,
		rt: make([]RecTable, nTables, MAXTABLENUM),
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

func (l *LTransaction) ReadValue(tableID int, k Key, partNum int, colNum int, trial int) (Value, bool, error) {
	if *SysType == ADAPTIVE {
		l.st.oneSample(tableID, k, partNum, l.s, l.w.riMaster, true)
	}

	var ok bool = false
	var wr *WriteRec
	var rr *ReadRec
	w := l.w

	rt := &l.rt[tableID]
	for i, _ := range rt.wRecs {
		if rt.wRecs[i].k == k {
			wr = &rt.wRecs[i]
			ok = true
			for p := len(wr.cols) - 1; p >= 0; p-- {
				if colNum == wr.cols[p] {
					return wr.vals[p], false, nil
				}
			}
			break
		}
	}
	// Has been Locked
	if ok {
		return wr.rec.GetValue(colNum), true, nil
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
		return rr.rec.GetValue(colNum), true, nil
	}

	// Try RLock
	rec = l.s.GetRecByID(tableID, k, partNum)
	if rec == nil {
		l.Abort()
		return nil, true, ENOKEY
	}

	if !rec.RLock(trial) {
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
		return nil, true, EABORT
	}

	//clog.Info("Worker %v: Trans %v RLock Table %v; Key %v Success\n", w.ID, w.NStats[NTXN], tableID, ParseKey(k, 0))

	// Success, Record it
	n := len(rt.rRecs)
	rt.rRecs = rt.rRecs[:n+1]
	rt.rRecs[n].k = k
	rt.rRecs[n].rec = rec
	rt.rRecs[n].exist = true

	return rec.GetValue(colNum), true, nil
}

func (l *LTransaction) WriteValue(tableID int, k Key, partNum int, value Value, colNum int, trial int) error {
	if *SysType == ADAPTIVE {
		l.st.oneSample(tableID, k, partNum, l.s, l.w.riMaster, false)
	}

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
		if rr.rec.Upgrade(trial) {
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

	if rec.WLock(trial) {
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

func (l *LTransaction) MayWrite(tableID int, k Key, partNum int, trial int) error {
	if !*Spec {
		return nil
	}
	var ok bool = false
	var rr *ReadRec
	w := l.w

	rt := &l.rt[tableID]
	//wr, ok = rt.wRecs[k]
	for i, _ := range rt.wRecs {
		if rt.wRecs[i].k == k {
			ok = true
			return nil
		}
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
		if rr.rec.Upgrade(trial) {
			//clog.Info("Worker %v: Trans %v Upgrade table %v; Key %v Success\n", w.ID, w.NStats[NTXN], tableID, ParseKey(k, 0))
			n := len(rt.wRecs)
			rt.wRecs = rt.wRecs[0 : n+1]
			wr := &rt.wRecs[n]
			wr.k = k
			wr.partNum = partNum
			wr.rec = rr.rec
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

	if rec.WLock(trial) {
		n := len(rt.wRecs)
		rt.wRecs = rt.wRecs[0 : n+1]
		wr := &rt.wRecs[n]
		wr.k = k
		wr.partNum = partNum
		wr.rec = rec
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
			for p := 0; p < len(wr.vals); p++ {
				wr.rec.SetValue(wr.vals[p], wr.cols[p])
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

func (l *LTransaction) GetType() int {
	return LOCKING
}
