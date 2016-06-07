package testbed

import (
	"flag"
	"github.com/totemtang/cc-testbed/clog"
	"time"
)

var Spec = flag.Bool("spec", false, "Whether Speculatively Indicate MayWrite")

const (
	MAXTABLENUM    = 20
	MAXCOLUMN      = 100
	MAXTRACKINGKEY = 100
)

const (
	WAREHOUSEWEIGHT = 100000
	DISTRICTWEIGHT  = 10000
)

type ETransaction interface {
	Reset(t Trans)
	ReadValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq, isHome bool) (Record, Value, bool, error)
	WriteValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq, isDelta bool, isHome bool, inputRec Record) error
	MayWrite(tableID int, k Key, partNum int, req *LockReq) error
	InsertRecord(tableID int, k Key, partNum int, rec Record) error
	DeleteRecord(tableID int, k Key, partNum int) (Record, error)
	GetKeysBySecIndex(tableID int, k Key, partNum int, val Value) error
	GetRecord(tableID int, k Key, partNum int, req *LockReq, isHome bool) (Record, error)
	Abort(req *LockReq) TID
	Commit(req *LockReq) TID
	Store() *Store
	Worker() *Worker
	GetType() int
}

type InsertRec struct {
	padding1 [PADDING]byte
	k        Key
	partNum  int
	rec      Record
	padding2 [PADDING]byte
}

type DeleteRec struct {
	padding1 [PADDING]byte
	k        Key
	partNum  int
	padding2 [PADDING]byte
}

type PTrackTable struct {
	padding1 [PADDING]byte
	wRecs    []WriteRec
	iRecs    []InsertRec
	dRecs    []DeleteRec
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
			wr.vals = make([]Value, MAXCOLUMN+2*PADDINGINT64)
			wr.vals = wr.vals[PADDINGINT64:PADDINGINT64]
			wr.cols = make([]int, MAXCOLUMN+2*PADDINGINT)
			wr.cols = wr.cols[PADDINGINT:PADDINGINT]
			wr.isDelta = make([]bool, MAXCOLUMN+2*PADDINGBOOL)
			wr.isDelta = wr.isDelta[PADDINGBOOL:PADDINGBOOL]
		}
		t.wRecs = t.wRecs[0:0]

		t.iRecs = make([]InsertRec, MAXTRACKINGKEY)
		t.iRecs = t.iRecs[0:0]

		t.dRecs = make([]DeleteRec, MAXTRACKINGKEY)
		t.dRecs = t.dRecs[0:0]
	}

	return tx
}

func (p *PTransaction) Reset(t Trans) {

}

func (p *PTransaction) ReadValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq, isHome bool) (Record, Value, bool, error) {
	if *SysType == ADAPTIVE {
		if p.st.trueCounter == 0 { // Sample Latency
			tm := time.Now()
			_, _ = p.s.GetRecByID(tableID, k, partNum)
			p.w.riMaster.latency += time.Since(tm).Nanoseconds()
			if WLTYPE == TPCCWL && tableID == WAREHOUSE {
				p.w.riMaster.readCount += WAREHOUSEWEIGHT
			} else if WLTYPE == TPCCWL && tableID == DISTRICT {
				p.w.riMaster.readCount += DISTRICTWEIGHT
			} else {
				p.w.riMaster.readCount++
			}
		}
		if isHome {
			sample := &p.st.homeSample
			if sample.state == 0 { // Not Enough locks acquired
				p.st.oneSampleConf(tableID, k, partNum, p.s, p.w.riMaster, true)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					p.st.oneSampleConf(tableID, k, partNum, p.s, p.w.riMaster, true)
					sample.sampleAccess = 0
				}
			}
		}

		if !p.st.isPartition {
			sample := &p.st.allSample
			if sample.state == 0 { // Not Enough locks acquired
				p.st.oneSampleConf(tableID, k, partNum, p.s, p.w.riMaster, false)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					p.st.oneSampleConf(tableID, k, partNum, p.s, p.w.riMaster, false)
					sample.sampleAccess = 0
				}
			}
		}
	}

	t := &p.tt[tableID]
	for i := 0; i < len(t.wRecs); i++ {
		wr := &t.wRecs[i]
		if wr.k == k {
			for p := len(wr.cols) - 1; p >= 0; p-- {
				if colNum == wr.cols[p] {
					return wr.rec, wr.vals[p], false, nil
				}
			}
			break
		}
	}

	s := p.s
	//err := s.GetValueByID(tableID, k, partNum, val, colNum)
	rec, err := s.GetRecByID(tableID, k, partNum)
	if err != nil {
		return nil, nil, true, err
	}
	rec.GetValue(val, colNum)
	return rec, val, true, nil
}

func (p *PTransaction) WriteValue(tableID int, k Key, partNum int, value Value, colNum int, req *LockReq, isDelta bool, isHome bool, inputRec Record) error {
	if *SysType == ADAPTIVE {
		if p.st.trueCounter == 0 { // Sample Latency
			tm := time.Now()
			_, _ = p.s.GetRecByID(tableID, k, partNum)
			p.w.riMaster.latency += time.Since(tm).Nanoseconds()
			if WLTYPE == TPCCWL && tableID == WAREHOUSE {
				p.w.riMaster.writeCount += WAREHOUSEWEIGHT
			} else if WLTYPE == TPCCWL && tableID == DISTRICT {
				p.w.riMaster.writeCount += DISTRICTWEIGHT
			} else {
				p.w.riMaster.writeCount++
			}
		}
		if isHome {
			sample := &p.st.homeSample
			if sample.state == 0 { // Not Enough locks acquired
				p.st.oneSampleConf(tableID, k, partNum, p.s, p.w.riMaster, true)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					p.st.oneSampleConf(tableID, k, partNum, p.s, p.w.riMaster, true)
					sample.sampleAccess = 0
				}
			}
		}

		if !p.st.isPartition {
			sample := &p.st.allSample
			if sample.state == 0 { // Not Enough locks acquired
				p.st.oneSampleConf(tableID, k, partNum, p.s, p.w.riMaster, false)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					p.st.oneSampleConf(tableID, k, partNum, p.s, p.w.riMaster, false)
					sample.sampleAccess = 0
				}
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
			wr.isDelta = wr.isDelta[0 : n+1]
			wr.isDelta[n] = isDelta
			return nil
		}
	}

	s := p.s
	var err error
	var r Record
	if inputRec == nil {
		r, err = s.GetRecByID(tableID, k, partNum)
		if err != nil {
			return err
		}
	} else {
		r = inputRec
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
	t.wRecs[n].isDelta = t.wRecs[n].isDelta[0:1]
	t.wRecs[n].isDelta[0] = isDelta
	return nil

}

func (p *PTransaction) MayWrite(tableID int, k Key, partNum int, req *LockReq) error {
	return nil
}

func (p *PTransaction) InsertRecord(tableID int, k Key, partNum int, rec Record) error {

	s := p.s
	err := s.PrepareInsert(tableID, k, partNum)
	if err != nil {
		return err
	}

	t := &p.tt[tableID]
	n := len(t.iRecs)
	t.iRecs = t.iRecs[0 : n+1]
	t.iRecs[n].k = k
	t.iRecs[n].partNum = partNum
	t.iRecs[n].rec = rec

	return nil
}

func (p *PTransaction) DeleteRecord(tableID int, k Key, partNum int) (Record, error) {

	s := p.s
	rec, err := s.PrepareDelete(tableID, k, partNum)
	if err != nil {
		return nil, err
	}

	t := &p.tt[tableID]
	n := len(t.dRecs)
	t.dRecs = t.dRecs[0 : n+1]
	t.dRecs[n].k = k
	t.dRecs[n].partNum = partNum

	return rec, err
}

func (p *PTransaction) GetKeysBySecIndex(tableID int, k Key, partNum int, val Value) error {
	return p.s.GetValueBySec(tableID, k, partNum, val)
}

func (p *PTransaction) GetRecord(tableID int, k Key, partNum int, req *LockReq, isHome bool) (Record, error) {
	transExec := p
	if *SysType == ADAPTIVE {
		if transExec.st.trueCounter == 0 { // Sample Latency
			tm := time.Now()
			_, _ = transExec.s.GetRecByID(tableID, k, partNum)
			transExec.w.riMaster.latency += time.Since(tm).Nanoseconds()
			if WLTYPE == TPCCWL && tableID == WAREHOUSE {
				p.w.riMaster.readCount += WAREHOUSEWEIGHT
			} else if WLTYPE == TPCCWL && tableID == DISTRICT {
				p.w.riMaster.readCount += DISTRICTWEIGHT
			} else {
				p.w.riMaster.readCount++
			}
		}
		if isHome {
			sample := &transExec.st.homeSample
			if sample.state == 0 { // Not Enough locks acquired
				transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, true)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, true)
					sample.sampleAccess = 0
				}
			}
		}

		if !transExec.st.isPartition {
			sample := &transExec.st.allSample
			if sample.state == 0 { // Not Enough locks acquired
				transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, false)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, false)
					sample.sampleAccess = 0
				}
			}
		}
	}

	rec, err := p.s.GetRecByID(tableID, k, partNum)
	if err != nil {
		return nil, err
	}
	return rec, err
}

func (p *PTransaction) Abort(req *LockReq) TID {
	s := p.s
	for i := 0; i < len(p.tt); i++ {
		t := &p.tt[i]
		for j := 0; j < len(t.wRecs); j++ {
			wr := &t.wRecs[j]
			wr.vals = wr.vals[:0]
			wr.cols = wr.cols[:0]
			wr.isDelta = wr.isDelta[:0]
		}
		t.wRecs = t.wRecs[0:0]

		for j := 0; j < len(t.iRecs); j++ {
			s.ReleaseInsert(i, t.iRecs[j].k, t.iRecs[j].partNum)
		}
		t.iRecs = t.iRecs[0:0]

		for j := 0; j < len(t.dRecs); j++ {
			s.ReleaseDelete(i, t.dRecs[j].k, t.dRecs[j].partNum)
		}
		t.dRecs = t.dRecs[0:0]
	}
	return 0
}

func (p *PTransaction) Commit(req *LockReq) TID {
	s := p.Store()
	w := p.w
	for i := 0; i < len(p.tt); i++ {
		t := &p.tt[i]
		for j := 0; j < len(t.wRecs); j++ {
			wr := &t.wRecs[j]
			//rec := s.GetRecByID(i, wr.k, wr.partNum)
			for p := 0; p < len(wr.cols); p++ {
				if wr.isDelta[p] {
					wr.rec.DeltaValue(wr.vals[p], wr.cols[p])
				} else {
					wr.rec.SetValue(wr.vals[p], wr.cols[p])
				}
			}
			wr.vals = wr.vals[:0]
			wr.cols = wr.cols[:0]
			wr.isDelta = wr.isDelta[:0]
		}
		t.wRecs = t.wRecs[0:0]

		if len(t.iRecs) != 0 {
			s.InsertRecord(i, t.iRecs, w.iaAR[i])
		}
		t.iRecs = t.iRecs[0:0]

		for j := 0; j < len(t.dRecs); j++ {
			s.DeleteRecord(i, t.dRecs[j].k, t.dRecs[j].partNum)
		}
		t.dRecs = t.dRecs[0:0]
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
	isDelta  []bool
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
	iRecs    []InsertRec
	dRecs    []DeleteRec
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
			wk.vals = make([]Value, MAXCOLUMN+2*PADDINGINT64)
			wk.vals = wk.vals[PADDINGINT64:PADDINGINT64]
			wk.cols = make([]int, MAXCOLUMN+2*PADDINGINT)
			wk.cols = wk.cols[PADDINGINT:PADDINGINT]
			wk.isDelta = make([]bool, MAXCOLUMN+2*PADDINGBOOL)
			wk.isDelta = wk.isDelta[PADDINGBOOL:PADDINGBOOL]
		}
		t.wKeys = t.wKeys[0:0]

		t.iRecs = make([]InsertRec, MAXTRACKINGKEY)
		t.iRecs = t.iRecs[0:0]

		t.dRecs = make([]DeleteRec, MAXTRACKINGKEY)
		t.dRecs = t.dRecs[0:0]
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
			wk.isDelta = wk.isDelta[:0]
		}
		t.wKeys = t.wKeys[:0]
		t.iRecs = t.iRecs[:0]
		t.dRecs = t.dRecs[:0]

	}

	//o.tt = o.tt[:0]

}

func (o *OTransaction) ReadValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq, isHome bool) (Record, Value, bool, error) {
	transExec := o
	if *SysType == ADAPTIVE {
		if transExec.st.trueCounter == 0 { // Sample Latency
			tm := time.Now()
			_, _ = transExec.s.GetRecByID(tableID, k, partNum)
			transExec.w.riMaster.latency += time.Since(tm).Nanoseconds()
			if WLTYPE == TPCCWL && tableID == WAREHOUSE {
				transExec.w.riMaster.readCount += WAREHOUSEWEIGHT
			} else if WLTYPE == TPCCWL && tableID == DISTRICT {
				transExec.w.riMaster.readCount += DISTRICTWEIGHT
			} else {
				transExec.w.riMaster.readCount++
			}
		}
		if isHome {
			sample := &transExec.st.homeSample
			if sample.state == 0 { // Not Enough locks acquired
				transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, true)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, true)
					sample.sampleAccess = 0
				}
			}
		}

		if !transExec.st.isPartition {
			sample := &transExec.st.allSample
			if sample.state == 0 { // Not Enough locks acquired
				transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, false)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, false)
					sample.sampleAccess = 0
				}
			}
		}
	}

	var ok bool
	var tid TID
	var t *TrackTable
	var r Record
	var err error

	t = &o.tt[tableID]

	for j := 0; j < len(t.wKeys); j++ {
		wk := &t.wKeys[j]
		if wk.k == k {
			r = wk.rec
			for p := len(wk.cols) - 1; p >= 0; p-- {
				if colNum == wk.cols[p] {
					return wk.rec, wk.vals[p], false, nil
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
				o.Abort(req)
				o.w.NStats[NREADABORTS]++
				return nil, nil, true, EABORT
			}
			rk.rec.GetValue(val, colNum)
			return rk.rec, val, true, nil
		}
	}

	if r == nil {
		r, err = o.s.GetRecByID(tableID, k, partNum)
		if err != nil {
			return nil, nil, true, ENOKEY
		}
	}

	ok, tid = r.IsUnlocked()

	if tid > o.maxSeen {
		o.maxSeen = tid
	}

	if !ok {
		o.Abort(req)
		o.w.NStats[NREADABORTS]++
		return nil, nil, true, EABORT
	}

	n := len(t.rKeys)
	t.rKeys = t.rKeys[0 : n+1]
	t.rKeys[n].k = k
	t.rKeys[n].last = tid
	t.rKeys[n].rec = r

	r.GetValue(val, colNum)
	return r, val, true, nil
}

func (o *OTransaction) WriteValue(tableID int, k Key, partNum int, value Value, colNum int, req *LockReq, isDelta bool, isHome bool, inputRec Record) error {
	transExec := o
	if *SysType == ADAPTIVE {
		if transExec.st.trueCounter == 0 { // Sample Latency
			tm := time.Now()
			_, _ = transExec.s.GetRecByID(tableID, k, partNum)
			transExec.w.riMaster.latency += time.Since(tm).Nanoseconds()
			if WLTYPE == TPCCWL && tableID == WAREHOUSE {
				transExec.w.riMaster.writeCount += WAREHOUSEWEIGHT
			} else if WLTYPE == TPCCWL && tableID == DISTRICT {
				transExec.w.riMaster.writeCount += DISTRICTWEIGHT
			} else {
				transExec.w.riMaster.writeCount++
			}
		}
		if isHome {
			sample := &transExec.st.homeSample
			if sample.state == 0 { // Not Enough locks acquired
				transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, true)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, true)
					sample.sampleAccess = 0
				}
			}
		}

		if !transExec.st.isPartition {
			sample := &transExec.st.allSample
			if sample.state == 0 { // Not Enough locks acquired
				transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, false)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, false)
					sample.sampleAccess = 0
				}
			}
		}
	}

	var ok bool
	//var tid TID
	var t *TrackTable
	var r Record
	var err error

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
			wk.isDelta = wk.isDelta[0 : n+1]
			wk.isDelta[n] = isDelta
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
			if inputRec == nil {
				r, err = o.s.GetRecByID(tableID, k, partNum)
				if err != nil {
					return err
				}
			} else {
				r = inputRec
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
		t.wKeys[n].isDelta = t.wKeys[n].isDelta[0:1]
		t.wKeys[n].isDelta[0] = isDelta
	}

	return nil
}

func (o *OTransaction) MayWrite(tableID int, k Key, partNum int, req *LockReq) error {
	return nil
}

func (o *OTransaction) InsertRecord(tableID int, k Key, partNum int, rec Record) error {

	s := o.s
	err := s.PrepareInsert(tableID, k, partNum)
	if err != nil {
		return err
	}

	t := &o.tt[tableID]
	n := len(t.iRecs)
	t.iRecs = t.iRecs[0 : n+1]
	t.iRecs[n].k = k
	t.iRecs[n].partNum = partNum
	t.iRecs[n].rec = rec

	return nil
}

func (o *OTransaction) DeleteRecord(tableID int, k Key, partNum int) (Record, error) {

	s := o.s
	rec, err := s.PrepareDelete(tableID, k, partNum)
	if err != nil {
		return nil, err
	}

	t := &o.tt[tableID]
	n := len(t.dRecs)
	t.dRecs = t.dRecs[0 : n+1]
	t.dRecs[n].k = k
	t.dRecs[n].partNum = partNum

	return rec, err
}

func (o *OTransaction) GetKeysBySecIndex(tableID int, k Key, partNum int, val Value) error {
	return o.s.GetValueBySec(tableID, k, partNum, val)
}

func (o *OTransaction) GetRecord(tableID int, k Key, partNum int, req *LockReq, isHome bool) (Record, error) {
	transExec := o
	if *SysType == ADAPTIVE {
		if transExec.st.trueCounter == 0 { // Sample Latency
			tm := time.Now()
			_, _ = transExec.s.GetRecByID(tableID, k, partNum)
			transExec.w.riMaster.latency += time.Since(tm).Nanoseconds()
			if WLTYPE == TPCCWL && tableID == WAREHOUSE {
				transExec.w.riMaster.readCount += WAREHOUSEWEIGHT
			} else if WLTYPE == TPCCWL && tableID == DISTRICT {
				transExec.w.riMaster.readCount += DISTRICTWEIGHT
			} else {
				transExec.w.riMaster.readCount++
			}
		}
		if isHome {
			sample := &transExec.st.homeSample
			if sample.state == 0 { // Not Enough locks acquired
				transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, true)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, true)
					sample.sampleAccess = 0
				}
			}
		}

		if !transExec.st.isPartition {
			sample := &transExec.st.allSample
			if sample.state == 0 { // Not Enough locks acquired
				transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, false)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, false)
					sample.sampleAccess = 0
				}
			}
		}
	}

	var ok bool
	var tid TID
	var t *TrackTable
	var r Record
	var err error

	r, err = o.s.GetRecByID(tableID, k, partNum)
	if err != nil {
		return nil, err
	}

	ok, tid = r.IsUnlocked()

	if tid > o.maxSeen {
		o.maxSeen = tid
	}

	if !ok {
		o.w.NStats[NREADABORTS]++
		return nil, EABORT
	}

	t = &o.tt[tableID]
	n := len(t.rKeys)
	t.rKeys = t.rKeys[0 : n+1]
	t.rKeys[n].k = k
	t.rKeys[n].last = tid
	t.rKeys[n].rec = r

	return r, nil
}

func (o *OTransaction) Abort(req *LockReq) TID {
	s := o.s
	for i := 0; i < len(o.tt); i++ {
		t := &o.tt[i]
		for j := 0; j < len(t.wKeys); j++ {
			wk := &t.wKeys[j]
			if wk.locked {
				wk.rec.Unlock(o.maxSeen)
			}
		}
		for j := 0; j < len(t.iRecs); j++ {
			s.ReleaseInsert(i, t.iRecs[j].k, t.iRecs[j].partNum)
		}

		for j := 0; j < len(t.dRecs); j++ {
			s.ReleaseDelete(i, t.dRecs[j].k, t.dRecs[j].partNum)
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
	s := o.s
	w := o.w
	for p := 0; p < len(o.tt); p++ {
		t := &o.tt[p]

		for j := 0; j < len(t.dRecs); j++ {
			s.DeleteRecord(p, t.dRecs[j].k, t.dRecs[j].partNum)
		}

		if len(t.iRecs) != 0 {
			s.InsertRecord(p, t.iRecs, w.iaAR[p])
		}
	}

	for p := 0; p < len(o.tt); p++ {
		t := &o.tt[p]

		for i, _ := range t.wKeys {
			wk := &t.wKeys[i]
			for j := 0; j < len(wk.vals); j++ {
				if wk.isDelta[j] {
					wk.rec.DeltaValue(wk.vals[j], wk.cols[j])
				} else {
					wk.rec.SetValue(wk.vals[j], wk.cols[j])
				}
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
	isDelta  []bool
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
	iRecs    []InsertRec
	dRecs    []DeleteRec
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
			wr.vals = make([]Value, MAXCOLUMN+2*PADDINGINT64)
			wr.vals = wr.vals[PADDINGINT64:PADDINGINT64]
			wr.cols = make([]int, MAXCOLUMN+2*PADDINGINT)
			wr.cols = wr.cols[PADDINGINT:PADDINGINT]
			wr.isDelta = make([]bool, MAXCOLUMN+2*PADDINGBOOL)
			wr.isDelta = wr.isDelta[PADDINGBOOL:PADDINGBOOL]
		}
		t.wRecs = t.wRecs[0:0]

		t.iRecs = make([]InsertRec, MAXTRACKINGKEY)
		t.iRecs = t.iRecs[0:0]

		t.dRecs = make([]DeleteRec, MAXTRACKINGKEY)
		t.dRecs = t.dRecs[0:0]
	}

	return tx
}

func (l *LTransaction) getWriteRec() *WriteRec {
	return nil
}

func (l *LTransaction) Reset(t Trans) {
}

func (l *LTransaction) ReadValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq, isHome bool) (Record, Value, bool, error) {
	transExec := l
	if *SysType == ADAPTIVE {
		if transExec.st.trueCounter == 0 { // Sample Latency
			tm := time.Now()
			_, _ = transExec.s.GetRecByID(tableID, k, partNum)
			transExec.w.riMaster.latency += time.Since(tm).Nanoseconds()
			if WLTYPE == TPCCWL && tableID == WAREHOUSE {
				transExec.w.riMaster.readCount += WAREHOUSEWEIGHT
			} else if WLTYPE == TPCCWL && tableID == DISTRICT {
				transExec.w.riMaster.readCount += DISTRICTWEIGHT
			} else {
				transExec.w.riMaster.readCount++
			}
		}
		if isHome {
			sample := &transExec.st.homeSample
			if sample.state == 0 { // Not Enough locks acquired
				transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, true)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, true)
					sample.sampleAccess = 0
				}
			}
		}

		if !transExec.st.isPartition {
			sample := &transExec.st.allSample
			if sample.state == 0 { // Not Enough locks acquired
				transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, false)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, false)
					sample.sampleAccess = 0
				}
			}
		}
	}

	var ok bool = false
	var wr *WriteRec
	var rr *ReadRec
	var err error
	w := l.w

	rt := &l.rt[tableID]
	for i, _ := range rt.wRecs {
		if rt.wRecs[i].k == k {
			wr = &rt.wRecs[i]
			ok = true
			for p := len(wr.cols) - 1; p >= 0; p-- {
				if colNum == wr.cols[p] {
					return wr.rec, wr.vals[p], false, nil
				}
			}
			break
		}
	}
	// Has been Locked
	if ok {
		wr.rec.GetValue(val, colNum)
		return wr.rec, val, true, nil
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
		return rr.rec, val, true, nil
	}

	// Try RLock
	rec, err = l.s.GetRecByID(tableID, k, partNum)
	if err != nil {
		return nil, nil, true, err
	}

	if !rec.RLock(req) {
		w.NStats[NRLOCKABORTS]++
		l.Abort(req)
		return nil, nil, true, EABORT
	}

	//clog.Info("Worker %v: Trans %v RLock Table %v; Key %v Success\n", w.ID, w.NStats[NTXN], tableID, ParseKey(k, 0))

	// Success, Record it
	n := len(rt.rRecs)
	rt.rRecs = rt.rRecs[:n+1]
	rt.rRecs[n].k = k
	rt.rRecs[n].rec = rec
	rt.rRecs[n].exist = true

	rec.GetValue(val, colNum)
	return rec, val, true, nil
}

func (l *LTransaction) WriteValue(tableID int, k Key, partNum int, value Value, colNum int, req *LockReq, isDelta bool, isHome bool, inputRec Record) error {
	transExec := l
	if *SysType == ADAPTIVE {
		if transExec.st.trueCounter == 0 { // Sample Latency
			tm := time.Now()
			_, _ = transExec.s.GetRecByID(tableID, k, partNum)
			transExec.w.riMaster.latency += time.Since(tm).Nanoseconds()
			if WLTYPE == TPCCWL && tableID == WAREHOUSE {
				transExec.w.riMaster.writeCount += WAREHOUSEWEIGHT
			} else if WLTYPE == TPCCWL && tableID == DISTRICT {
				transExec.w.riMaster.writeCount += DISTRICTWEIGHT
			} else {
				transExec.w.riMaster.writeCount++
			}
		}
		if isHome {
			sample := &transExec.st.homeSample
			if sample.state == 0 { // Not Enough locks acquired
				transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, true)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, true)
					sample.sampleAccess = 0
				}
			}
		}

		if !transExec.st.isPartition {
			sample := &transExec.st.allSample
			if sample.state == 0 { // Not Enough locks acquired
				transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, false)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, false)
					sample.sampleAccess = 0
				}
			}
		}
	}

	var ok bool = false
	var wr *WriteRec
	var rr *ReadRec
	var err error
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
		wr.isDelta = wr.isDelta[0 : n+1]
		wr.isDelta[n] = isDelta
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
			wr.isDelta = wr.isDelta[0:1]
			wr.isDelta[0] = isDelta
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

	if inputRec == nil {
		rec, err = l.s.GetRecByID(tableID, k, partNum)
		if err != nil {
			return err
		}
	} else {
		rec = inputRec
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
		wr.isDelta = wr.isDelta[0:1]
		wr.isDelta[0] = isDelta
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
	var err error
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
		if rr.rec.Upgrade(req) {
			rr.exist = false
			//clog.Info("Worker %v: Trans %v Upgrade table %v; Key %v Success\n", w.ID, w.NStats[NTXN], tableID, ParseKey(k, 0))
			n := len(rt.wRecs)
			rt.wRecs = rt.wRecs[0 : n+1]
			wr := &rt.wRecs[n]
			wr.k = k
			wr.partNum = partNum
			wr.rec = rr.rec
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

	rec, err = l.s.GetRecByID(tableID, k, partNum)
	if err != nil {
		return err
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

func (l *LTransaction) InsertRecord(tableID int, k Key, partNum int, rec Record) error {

	s := l.s
	err := s.PrepareInsert(tableID, k, partNum)
	if err != nil {
		return err
	}

	t := &l.rt[tableID]
	n := len(t.iRecs)
	t.iRecs = t.iRecs[0 : n+1]
	t.iRecs[n].k = k
	t.iRecs[n].partNum = partNum
	t.iRecs[n].rec = rec

	return nil
}

func (l *LTransaction) DeleteRecord(tableID int, k Key, partNum int) (Record, error) {

	s := l.s
	rec, err := s.PrepareDelete(tableID, k, partNum)
	if err != nil {
		return nil, err
	}

	t := &l.rt[tableID]
	n := len(t.dRecs)
	t.dRecs = t.dRecs[0 : n+1]
	t.dRecs[n].k = k
	t.dRecs[n].partNum = partNum

	return rec, err
}

func (l *LTransaction) GetKeysBySecIndex(tableID int, k Key, partNum int, val Value) error {
	return l.s.GetValueBySec(tableID, k, partNum, val)
}

func (l *LTransaction) GetRecord(tableID int, k Key, partNum int, req *LockReq, isHome bool) (Record, error) {
	transExec := l
	if *SysType == ADAPTIVE {
		if transExec.st.trueCounter == 0 { // Sample Latency
			tm := time.Now()
			_, _ = transExec.s.GetRecByID(tableID, k, partNum)
			transExec.w.riMaster.latency += time.Since(tm).Nanoseconds()
			if WLTYPE == TPCCWL && tableID == WAREHOUSE {
				transExec.w.riMaster.readCount += WAREHOUSEWEIGHT
			} else if WLTYPE == TPCCWL && tableID == DISTRICT {
				transExec.w.riMaster.readCount += DISTRICTWEIGHT
			} else {
				transExec.w.riMaster.readCount++
			}
		}
		if isHome {
			sample := &transExec.st.homeSample
			if sample.state == 0 { // Not Enough locks acquired
				transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, true)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, true)
					sample.sampleAccess = 0
				}
			}
		}

		if !transExec.st.isPartition {
			sample := &transExec.st.allSample
			if sample.state == 0 { // Not Enough locks acquired
				transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, false)
			} else {
				sample.sampleAccess++
				if sample.sampleAccess >= sample.recRate {
					transExec.st.oneSampleConf(tableID, k, partNum, transExec.s, transExec.w.riMaster, false)
					sample.sampleAccess = 0
				}
			}
		}
	}

	var rec Record
	var err error
	w := l.w

	// Try RLock
	rec, err = l.s.GetRecByID(tableID, k, partNum)
	if err != nil {
		return nil, err
	}

	if !rec.RLock(req) {
		l.Abort(req)
		w.NStats[NRLOCKABORTS]++
		return nil, EABORT
	}

	//clog.Info("Worker %v: Trans %v RLock Table %v; Key %v Success\n", w.ID, w.NStats[NTXN], tableID, ParseKey(k, 0))

	rt := &l.rt[tableID]

	// Success, Record it
	n := len(rt.rRecs)
	rt.rRecs = rt.rRecs[:n+1]
	rt.rRecs[n].k = k
	rt.rRecs[n].rec = rec
	rt.rRecs[n].exist = true

	return rec, nil
}

func (l *LTransaction) Abort(req *LockReq) TID {
	//w := l.w
	s := l.s
	for i := 0; i < len(l.rt); i++ {
		t := &l.rt[i]

		for j := 0; j < len(t.dRecs); j++ {
			s.ReleaseDelete(i, t.dRecs[j].k, t.dRecs[j].partNum)
		}
		t.dRecs = t.dRecs[:0]

		for j := 0; j < len(t.iRecs); j++ {
			s.ReleaseInsert(i, t.iRecs[j].k, t.iRecs[j].partNum)
		}

		t.iRecs = t.iRecs[:0]

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
			wr.isDelta = wr.isDelta[:0]
			//clog.Info("Worker %v: Trans %v WUnlock Table %v; Key %v\n", w.ID, w.NStats[NTXN], i, ParseKey(wr.k, 0))
			wr.rec.WUnlock(req)
		}
		t.wRecs = t.wRecs[:0]

	}
	return 0
}

func (l *LTransaction) Commit(req *LockReq) TID {
	w := l.w
	s := l.s

	for i := 0; i < len(l.rt); i++ {
		t := &l.rt[i]

		for j := 0; j < len(t.dRecs); j++ {
			s.DeleteRecord(i, t.dRecs[j].k, t.dRecs[j].partNum)
		}
		t.dRecs = t.dRecs[:0]

		//for j := 0; j < len(t.iRecs); j++ {
		//	s.InsertRecord(i, t.iRecs[j].k, t.iRecs[j].partNum, t.iRecs[j].rec)
		//}
		if len(t.iRecs) != 0 {
			s.InsertRecord(i, t.iRecs, w.iaAR[i])
		}

		t.iRecs = t.iRecs[:0]
	}

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
				if wr.isDelta[p] {
					wr.rec.DeltaValue(wr.vals[p], wr.cols[p])
				} else {
					wr.rec.SetValue(wr.vals[p], wr.cols[p])
				}
			}
			wr.vals = wr.vals[:0]
			wr.cols = wr.cols[:0]
			wr.isDelta = wr.isDelta[:0]
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
