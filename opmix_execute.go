package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
)

type OPTransaction struct {
	padding0   [PADDING]byte
	w          *Worker
	s          *Store
	opTrack    []OPTrackTable
	occMaxSeen TID
	padding    [PADDING]byte
}

type OPReadRec struct {
	padding1 [PADDING]byte
	k        Key
	rec      Record
	last     TID
	exist    bool
	padding2 [PADDING]byte
}

type OPWriteRec struct {
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

type OPTrackTable struct {
	padding1 [PADDING]byte
	rRecs    []OPReadRec
	wRecs    []OPWriteRec
	iRecs    []InsertRec
	dRecs    []DeleteRec
	padding2 [PADDING]byte
}

func StartOPTransaction(w *Worker, nTables int, wc []WorkerConfig) *OPTransaction {
	tx := &OPTransaction{
		w:       w,
		s:       w.store,
		opTrack: make([]OPTrackTable, nTables, MAXTABLENUM),
	}

	for i := 0; i < len(tx.opTrack); i++ {
		t := &tx.opTrack[i]
		t.rRecs = make([]OPReadRec, 0, MAXTRACKINGKEY)
		t.wRecs = make([]OPWriteRec, MAXTRACKINGKEY)
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

func (op *OPTransaction) Reset(t Trans, hasPCC bool, hasOCC bool, hasLocking bool) {
	op.occMaxSeen = TID(0)
}

func (op *OPTransaction) ReadValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq, isHome bool) (Record, Value, bool, error) {
	var ok bool = false
	var wr *OPWriteRec
	var rr *OPReadRec
	var err error
	w := op.w

	rt := &op.opTrack[tableID]
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
		rr.rec.GetValue(val, colNum)
		return rr.rec, val, true, nil
	}

	// Try RLock
	rec, _, _, err = op.s.GetRecByID(tableID, k, partNum)
	if err != nil {
		return nil, nil, true, err
	}

	// 2PL Logic
	var tid TID
	ok, tid = rec.RLock(req)

	if !ok {
		w.NStats[NRLOCKABORTS]++
		op.Abort(req)
		return nil, nil, true, EABORT
	}

	// OCC Logic
	ok, tid = rec.IsUnlocked()

	if tid > op.occMaxSeen {
		op.occMaxSeen = tid
	}

	if !ok {
		rec.RUnlock(req)
		op.Abort(req)
		op.w.NStats[NREADABORTS]++
		return nil, nil, true, EABORT
	}

	// Success, Record it
	n := len(rt.rRecs)
	rt.rRecs = rt.rRecs[:n+1]
	rt.rRecs[n].k = k
	rt.rRecs[n].rec = rec
	rt.rRecs[n].exist = true
	rt.rRecs[n].last = tid

	rec.GetValue(val, colNum)
	return rec, val, true, nil
}

func (op *OPTransaction) WriteValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq, isDelta bool, isHome bool, inputRec Record) error {

	var ok bool = false
	var wr *OPWriteRec
	var rr *OPReadRec
	var err error
	w := op.w

	rt := &op.opTrack[tableID]
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
		wr.vals[n] = val
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
			wr.locked = false
			wr.vals = wr.vals[0:1]
			wr.vals[0] = val
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
			op.Abort(req)
			return EABORT
		}
	}

	if inputRec == nil {
		rec, _, _, err = op.s.GetRecByID(tableID, k, partNum)
		if err != nil {
			return err
		}
	} else {
		rec = inputRec
	}

	ok, _ = rec.WLock(req)

	if ok {
		n := len(rt.wRecs)
		rt.wRecs = rt.wRecs[0 : n+1]
		wr := &rt.wRecs[n]
		wr.k = k
		wr.partNum = partNum
		wr.rec = rec
		wr.locked = false
		wr.vals = wr.vals[0:1]
		wr.vals[0] = val
		wr.cols = wr.cols[0:1]
		wr.cols[0] = colNum
		wr.isDelta = wr.isDelta[0:1]
		wr.isDelta[0] = isDelta
		return nil
	} else {
		w.NStats[NWLOCKABORTS]++
		op.Abort(req)
		return EABORT
	}
}

func (op *OPTransaction) MayWrite(tableID int, k Key, partNum int, req *LockReq) error {
	return nil
}

func (op *OPTransaction) InsertRecord(tableID int, k Key, partNum int, rec Record) error {
	s := op.s
	err := s.PrepareInsert(tableID, k, partNum)
	if err != nil {
		return err
	}
	t := &op.opTrack[tableID]
	n := len(t.iRecs)
	t.iRecs = t.iRecs[0 : n+1]
	t.iRecs[n].k = k
	t.iRecs[n].partNum = partNum
	t.iRecs[n].rec = rec
	rec.SetKey(k)

	return nil
}

func (op *OPTransaction) DeleteRecord(tableID int, k Key, partNum int) (Record, error) {
	s := op.s
	rec, err := s.PrepareDelete(tableID, k, partNum)
	if err != nil {
		return nil, err
	}
	t := &op.opTrack[tableID]
	n := len(t.dRecs)
	t.dRecs = t.dRecs[0 : n+1]
	t.dRecs[n].k = k
	t.dRecs[n].partNum = partNum

	return rec, err
}

func (op *OPTransaction) GetKeysBySecIndex(tableID int, k Key, partNum int, val Value) error {
	return op.s.GetValueBySec(tableID, k, partNum, val)
}

func (op *OPTransaction) GetRecord(tableID int, k Key, partNum int, req *LockReq, isHome bool) (Record, error) {
	var rec Record
	var err error
	w := op.w

	// Try RLock
	rec, _, _, err = op.s.GetRecByID(tableID, k, partNum)
	if err != nil {
		return nil, err
	}

	// 2PL Logic
	ok, tid := rec.RLock(req)

	if !ok {
		op.Abort(req)
		w.NStats[NRLOCKABORTS]++
		return nil, EABORT
	}

	// OCC Logic
	ok, tid = rec.IsUnlocked()

	if tid > op.occMaxSeen {
		op.occMaxSeen = tid
	}

	if !ok {
		rec.RUnlock(req)
		op.w.NStats[NREADABORTS]++
		return nil, EABORT
	}

	rt := &op.opTrack[tableID]

	// Success, Record it
	n := len(rt.rRecs)
	rt.rRecs = rt.rRecs[:n+1]
	rt.rRecs[n].k = k
	rt.rRecs[n].rec = rec
	rt.rRecs[n].exist = true
	rt.rRecs[n].last = tid

	return rec, nil
}

func (op *OPTransaction) Abort(req *LockReq) TID {
	s := op.s
	for i := 0; i < len(op.opTrack); i++ {
		opT := &op.opTrack[i]

		// Release Writes
		for j, _ := range opT.rRecs {
			rr := &opT.rRecs[j]
			if rr.exist {
				rr.rec.RUnlock(req)
			}
		}
		opT.rRecs = opT.rRecs[:0]

		for j, _ := range opT.wRecs {
			wr := &opT.wRecs[j]
			wr.vals = wr.vals[:0]
			wr.cols = wr.cols[:0]
			wr.isDelta = wr.isDelta[:0]
			wr.rec.WUnlock(req, 0) // 2PL Unlock
			if wr.locked {
				wr.rec.Unlock(op.occMaxSeen) // OCC Unlock
			}
		}
		opT.wRecs = opT.wRecs[:0]

		// For Insert and Delete
		for j := 0; j < len(opT.iRecs); j++ {
			s.ReleaseInsert(i, opT.iRecs[j].k, opT.iRecs[j].partNum)
		}
		opT.iRecs = opT.iRecs[0:0]
		for j := 0; j < len(opT.dRecs); j++ {
			s.ReleaseDelete(i, opT.dRecs[j].k, opT.dRecs[j].partNum)
		}
		opT.dRecs = opT.dRecs[0:0]
	}

	return 0
}

func (op *OPTransaction) Commit(req *LockReq, isHome bool) TID {
	// OCC Validate
	// Phase 1: Lock all write keys
	var tid TID

	for j := 0; j < len(op.opTrack); j++ {
		t := &op.opTrack[j]
		if occ_wait { // If wait on writes, sort by keys
			for i := 1; i < len(t.wRecs); i++ {
				wk := t.wRecs[i]
				p := i - 1
				for p >= 0 && Compare(wk.k, t.wRecs[p].k) < 0 {
					t.wRecs[p+1] = t.wRecs[p]
					p--
				}
				t.wRecs[p+1] = wk
			}
		}

		for i := 0; i < len(t.wRecs); i++ {
			wk := &t.wRecs[i]
			var former TID
			var ok bool

			if occ_wait {
				for !ok {
					ok, former = wk.rec.Lock()
				}
			} else {
				ok, former = wk.rec.Lock()
				if !ok {
					op.w.NStats[NLOCKABORTS]++
					return op.Abort(req)
				}
			}
			wk.locked = true
			if former > op.occMaxSeen {
				op.occMaxSeen = former
			}
		}
	}

	tid = op.w.commitTID()
	if tid <= op.occMaxSeen {
		op.w.ResetTID(op.occMaxSeen)
		tid = op.w.commitTID()
		if tid < op.occMaxSeen {
			clog.Error("%v MaxSeen %v, reset TID but %v<%v", op.w.ID, op.occMaxSeen, tid, op.occMaxSeen)
		}
	}

	// Phase 2: Check conflicts
	for j := 0; j < len(op.opTrack); j++ {
		t := &op.opTrack[j]
		for i := 0; i < len(t.rRecs); i++ {
			k := t.rRecs[i].k
			rk := &t.rRecs[i]
			//verify whether TID has changed
			var ok1, ok2 bool
			var tmpTID TID
			ok1, tmpTID = rk.rec.IsUnlocked()
			if tmpTID != rk.last {
				op.w.NStats[NRCHANGEABORTS]++
				return op.Abort(req)
			}

			// Check whether read key is not in wKeys
			ok2 = false
			for p := 0; p < len(t.wRecs); p++ {
				wk := &t.wRecs[p]
				if wk.k == k {
					ok2 = true
					break
				}
			}

			if !ok1 && !ok2 {
				op.w.NStats[NRWABORTS]++
				return op.Abort(req)
			}
		}
	}

	s := op.Store()
	w := op.w
	for i := len(op.opTrack) - 1; i >= 0; i-- {
		t := &op.opTrack[i]

		if len(t.iRecs) != 0 {
			s.InsertRecord(i, t.iRecs, w.iaAR[i])
		}
		t.iRecs = t.iRecs[0:0]

		for j := 0; j < len(t.dRecs); j++ {
			s.DeleteRecord(i, t.dRecs[j].k, t.dRecs[j].partNum)
		}
		t.dRecs = t.dRecs[0:0]

	}

	for i := len(op.opTrack) - 1; i >= 0; i-- {

		t := &op.opTrack[i]
		for j, _ := range t.rRecs {
			rr := &t.rRecs[j]
			if rr.exist {
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

			wr.rec.WUnlock(req, 0)
			wr.rec.Unlock(tid)
			wr.locked = false
		}
		t.wRecs = t.wRecs[:0]
	}

	return 1
}

func (op *OPTransaction) Store() *Store {
	return op.s
}

func (op *OPTransaction) Worker() *Worker {
	return op.w
}

func (op *OPTransaction) GetType() int {
	return ADAPTIVE
}
