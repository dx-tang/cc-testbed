package testbed

import (
	"flag"
	"github.com/totemtang/cc-testbed/clog"
)

var occ_wait bool = true

var Spec = flag.Bool("spec", false, "Whether Speculatively Indicate MayWrite")

const (
	MAXTABLENUM    = 20
	MAXCOLUMN      = 20
	MAXTRACKINGKEY = 100
)

type ETransaction interface {
	Reset(t Trans)
	ReadValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq) (Value, bool, error)
	WriteValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq) error
	MayWrite(tableID int, k Key, partNum int, req *LockReq) error
	Abort(req *LockReq) TID
	Commit(req *LockReq) TID
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

func (p *PTransaction) ReadValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq) (Value, bool, error) {
	if *SysType == ADAPTIVE {
		if p.st.sampleCount == 0 {
			p.st.oneSample(tableID, p.w.riMaster, true)
		}

		if p.st.state == 0 {
			p.st.oneSampleConf(tableID, k, partNum, p.s, p.w.riMaster)
		} else {
			p.st.sampleAccess++
			if p.st.sampleAccess >= RECSR {
				p.st.oneSampleConf(tableID, k, partNum, p.s, p.w.riMaster)
				p.st.sampleAccess = 0
			}
		}
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
	err := s.GetValueByID(tableID, k, partNum, val, colNum)
	if err != nil {
		return nil, true, err
	}
	return val, true, nil
}

func (p *PTransaction) WriteValue(tableID int, k Key, partNum int, value Value, colNum int, req *LockReq) error {
	if *SysType == ADAPTIVE {
		if p.st.sampleCount == 0 {
			p.st.oneSample(tableID, p.w.riMaster, false)
		}

		if p.st.state == 0 {
			p.st.oneSampleConf(tableID, k, partNum, p.s, p.w.riMaster)
		} else {
			p.st.sampleAccess++
			if p.st.sampleAccess >= RECSR {
				p.st.oneSampleConf(tableID, k, partNum, p.s, p.w.riMaster)
				p.st.sampleAccess = 0
			}
		}
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

func (p *PTransaction) MayWrite(tableID int, k Key, partNum int, req *LockReq) error {
	return nil
}

func (p *PTransaction) Abort(req *LockReq) TID {
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

func (p *PTransaction) Commit(req *LockReq) TID {
	//s := p.Store()
	for i := 0; i < len(p.tt); i++ {
		t := &p.tt[i]
		for j := 0; j < len(t.wRecs); j++ {
			wr := &t.wRecs[j]
			//rec := s.GetRecByID(i, wr.k, wr.partNum)
			for p := 0; p < len(wr.cols); p++ {
				wr.rec.SetValue(wr.vals[p], wr.cols[p])
				//rec.SetValue(wr.vals[p], wr.cols[p])
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

func (o *OTransaction) ReadValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq) (Value, bool, error) {
	if *SysType == ADAPTIVE {
		if o.st.sampleCount == 0 {
			o.st.oneSample(tableID, o.w.riMaster, true)
		}

		if o.st.state == 0 {
			o.st.oneSampleConf(tableID, k, partNum, o.s, o.w.riMaster)
		} else {
			o.st.sampleAccess++
			if o.st.sampleAccess >= RECSR {
				o.st.oneSampleConf(tableID, k, partNum, o.s, o.w.riMaster)
				o.st.sampleAccess = 0
			}
		}
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
			rk.rec.GetValue(val, colNum)
			return val, true, nil
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

	r.GetValue(val, colNum)
	return val, true, nil
}

func (o *OTransaction) WriteValue(tableID int, k Key, partNum int, value Value, colNum int, req *LockReq) error {
	if *SysType == ADAPTIVE {
		if o.st.sampleCount == 0 {
			o.st.oneSample(tableID, o.w.riMaster, false)
		}

		if o.st.state == 0 {
			o.st.oneSampleConf(tableID, k, partNum, o.s, o.w.riMaster)
		} else {
			o.st.sampleAccess++
			if o.st.sampleAccess >= RECSR {
				o.st.sampleAccess = 0
				o.st.oneSampleConf(tableID, k, partNum, o.s, o.w.riMaster)
			}
		}
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

func (o *OTransaction) MayWrite(tableID int, k Key, partNum int, req *LockReq) error {
	return nil
}

func (o *OTransaction) Abort(req *LockReq) TID {
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

func Compare(k1 Key, k2 Key) int {
	for i, _ := range k1 {
		if k1[i] < k2[i] {
			return -1
		} else if k1[i] > k2[i] {
			return 1
		}
	}
	return 0
}

func (o *OTransaction) Commit(req *LockReq) TID {

	// Phase 1: Lock all write keys
	//for _, wk := range o.wKeys {

	for j := 0; j < len(o.tt); j++ {
		t := &o.tt[j]

		if occ_wait { // If wait on writes, sort by keys
			for i := 1; i < len(t.wKeys); i++ {
				wk := t.wKeys[i]
				p := i - 1
				for p >= 0 && Compare(wk.k, t.wKeys[p].k) < 0 {
					t.wKeys[p+1] = t.wKeys[p]
					p--
				}
				t.wKeys[p+1] = wk
			}
		}

		for i := 0; i < len(t.wKeys); i++ {
			wk := &t.wKeys[i]
			var former TID
			var ok bool

			if occ_wait {
				for !ok {
					ok, former = wk.rec.Lock()
				}
			} else {
				ok, former = wk.rec.Lock()
				if !ok {
					o.w.NStats[NLOCKABORTS]++
					return o.Abort(req)
				}
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
				return o.Abort(req)
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
				return o.Abort(req)
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

func (l *LTransaction) ReadValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq) (Value, bool, error) {
	if *SysType == ADAPTIVE {
		if l.st.sampleCount == 0 {
			l.st.oneSample(tableID, l.w.riMaster, true)
		}

		if l.st.state == 0 {
			l.st.oneSampleConf(tableID, k, partNum, l.s, l.w.riMaster)
		} else {
			l.st.sampleAccess++
			if l.st.sampleAccess >= RECSR {
				l.st.oneSampleConf(tableID, k, partNum, l.s, l.w.riMaster)
				l.st.sampleAccess = 0
			}
		}
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
		wr.rec.GetValue(val, colNum)
		return val, true, nil
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
		//return rr.rec.GetValue(colNum), true, nil
		rr.rec.GetValue(val, colNum)
		return val, true, nil
	}

	// Try RLock
	rec = l.s.GetRecByID(tableID, k, partNum)
	if rec == nil {
		l.Abort(req)
		return nil, true, ENOKEY
	}

	if !rec.RLock(req) {
		w.NStats[NRLOCKABORTS]++
		l.Abort(req)
		return nil, true, EABORT
	}

	//clog.Info("Worker %v: Trans %v RLock Table %v; Key %v Success\n", w.ID, w.NStats[NTXN], tableID, ParseKey(k, 0))

	// Success, Record it
	n := len(rt.rRecs)
	rt.rRecs = rt.rRecs[:n+1]
	rt.rRecs[n].k = k
	rt.rRecs[n].rec = rec
	rt.rRecs[n].exist = true

	rec.GetValue(val, colNum)
	return val, true, nil
}

func (l *LTransaction) WriteValue(tableID int, k Key, partNum int, value Value, colNum int, req *LockReq) error {
	if *SysType == ADAPTIVE {
		if l.st.sampleCount == 0 {
			l.st.oneSample(tableID, l.w.riMaster, false)
		}

		if l.st.state == 0 {
			l.st.oneSampleConf(tableID, k, partNum, l.s, l.w.riMaster)
		} else {
			l.st.sampleAccess++
			if l.st.sampleAccess >= RECSR {
				l.st.oneSampleConf(tableID, k, partNum, l.s, l.w.riMaster)
				l.st.sampleAccess = 0
			}
		}
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
		if rr.rec.Upgrade(req) {
			rr.exist = false
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
			if *NoWait {
				rr.exist = false
			} else {
				rr.exist = true
			}
			//clog.Info("Worker %v: Trans %v Upgrade table %v; Key %v Failed\n", w.ID, w.NStats[NTXN], tableID, ParseKey(k, 0))
			w.NStats[NUPGRADEABORTS]++
			l.Abort(req)
			return EABORT
		}
	}

	rec = l.s.GetRecByID(tableID, k, partNum)
	if rec == nil {
		l.Abort(req)
		return ENOKEY
	}

	if rec.WLock(req) {
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
		l.Abort(req)
		return EABORT
	}

}

func (l *LTransaction) MayWrite(tableID int, k Key, partNum int, req *LockReq) error {
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
		if rr.rec.Upgrade(req) {
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
			l.Abort(req)
			return EABORT
		}
	}

	rec = l.s.GetRecByID(tableID, k, partNum)
	if rec == nil {
		l.Abort(req)
		return ENOKEY
	}

	if rec.WLock(req) {
		n := len(rt.wRecs)
		rt.wRecs = rt.wRecs[0 : n+1]
		wr := &rt.wRecs[n]
		wr.k = k
		wr.partNum = partNum
		wr.rec = rec
		return nil
	} else {
		w.NStats[NWLOCKABORTS]++
		l.Abort(req)
		return EABORT
	}
}

func (l *LTransaction) Abort(req *LockReq) TID {
	//w := l.w
	for i := 0; i < len(l.rt); i++ {
		t := &l.rt[i]
		for j, _ := range t.rRecs {
			rr := &t.rRecs[j]
			if rr.exist {
				//clog.Info("Worker %v: Trans %v RUnlock Table %v; Key %v\n", w.ID, w.NStats[NTXN], i, ParseKey(rr.k, 0))
				rr.rec.RUnlock(req)
			}
		}
		t.rRecs = t.rRecs[:0]
		for j, _ := range t.wRecs {
			wr := &t.wRecs[j]
			wr.vals = wr.vals[:0]
			wr.cols = wr.cols[:0]
			//clog.Info("Worker %v: Trans %v WUnlock Table %v; Key %v\n", w.ID, w.NStats[NTXN], i, ParseKey(wr.k, 0))
			wr.rec.WUnlock(req)
		}
		t.wRecs = t.wRecs[:0]
	}
	return 0
}

func (l *LTransaction) Commit(req *LockReq) TID {
	//w := l.w

	for i := 0; i < len(l.rt); i++ {
		t := &l.rt[i]
		for j, _ := range t.rRecs {
			rr := &t.rRecs[j]
			if rr.exist {
				//clog.Info("Worker %v: Trans %v RUnlock Table %v; Key %v\n", w.ID, w.NStats[NTXN], i, ParseKey(rr.k, 0))
				rr.rec.RUnlock(req)
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
			wr.rec.WUnlock(req)
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
