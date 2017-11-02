package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
)

// Tebaldi Transaction Implementation
type TTransaction struct {
	padding0 [PADDING]byte
	w        *Worker
	s        *Store
	st       *SampleTool
	rt       []RecTable
	maxSeen  TID
	padding  [PADDING]byte
}

func StartTTransaction(w *Worker, tableCount int) *TTransaction {
	tx := &TTransaction{
		w:  w,
		s:  w.store,
		st: w.st,
		rt: make([]RecTable, tableCount, MAXTABLENUM),
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

func (tx *TTransaction) Reset(t Trans, hasPCC bool, hasOCC bool, hasLocking bool) {
	tx.maxSeen = TID(0)
}

func (tx *TTransaction) ReadValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq, isHome bool) (Record, Value, bool, error) {
	return nil, nil, true, nil
}

func (tx *TTransaction) WriteValue(tableID int, k Key, partNum int, value Value, colNum int, req *LockReq, isDelta bool, isHome bool, inputRec Record) error {
	return nil
}

func (tx *TTransaction) MayWrite(tableID int, k Key, partNum int, req *LockReq) error {
	return nil
}

func (tx *TTransaction) InsertRecord(tableID int, k Key, partNum int, rec Record) error {

	s := tx.s
	err := s.PrepareInsert(tableID, k, partNum)
	if err != nil {
		return err
	}

	t := &tx.rt[tableID]
	n := len(t.iRecs)
	t.iRecs = t.iRecs[0 : n+1]
	t.iRecs[n].k = k
	t.iRecs[n].partNum = partNum
	t.iRecs[n].rec = rec
	rec.SetKey(k)

	return nil
}

func (tx *TTransaction) DeleteRecord(tableID int, k Key, partNum int) (Record, error) {

	s := tx.s
	rec, err := s.PrepareDelete(tableID, k, partNum)
	if err != nil {
		return nil, err
	}

	t := &tx.rt[tableID]
	n := len(t.dRecs)
	t.dRecs = t.dRecs[0 : n+1]
	t.dRecs[n].k = k
	t.dRecs[n].partNum = partNum

	return rec, err
}

func (tx *TTransaction) GetKeysBySecIndex(tableID int, k Key, partNum int, val Value) error {
	return tx.s.GetValueBySec(tableID, k, partNum, val)
}

func (tx *TTransaction) GetRecord(tableID int, k Key, partNum int, req *LockReq, isHome bool) (Record, error) {
	return nil, nil
}

func (tx *TTransaction) Abort(req *LockReq) TID {
	return 0
}

func (tx *TTransaction) Commit(req *LockReq, isHome bool) TID {
	return 0
}

func (tx *TTransaction) Store() *Store {
	return tx.s
}

func (tx *TTransaction) Worker() *Worker {
	return tx.w
}

func (tx *TTransaction) GetType() int {
	return TEBALDI
}

func (tx *TTransaction) GetRecord_Ex(tableID int, k Key, partNum int, req *LockReq, isLock bool, group int) (Record, error) {

	var rec Record
	var err error
	w := tx.w

	// Try RLock
	rec, _, _, err = tx.s.GetRecByID(tableID, k, partNum)
	for err == ENOKEY {
		rec, _, _, err = tx.s.GetRecByID(tableID, k, partNum)
	}
	if err != nil {
		return nil, err
	}

	rec.(*TRecord).GroupLock(group)

	if isLock {
		ok, tid := rec.WLock(req)

		if tid > tx.maxSeen {
			tx.maxSeen = tid
		}

		if !ok {
			clog.Error("Cannot abort")
			tx.Abort(req)
			w.NStats[NRLOCKABORTS]++
			return nil, EABORT
		}
	}

	//clog.Info("Worker %v: Trans %v RLock Table %v; Key %v Success\n", w.ID, w.NStats[NTXN], tableID, ParseKey(k, 0))

	rt := &tx.rt[tableID]

	// Success, Record it
	n := len(rt.rRecs)
	rt.rRecs = rt.rRecs[:n+1]
	rt.rRecs[n].k = k
	rt.rRecs[n].rec = rec
	rt.rRecs[n].exist = true
	if isLock {
		rt.rRecs[n].locked = true
	} else {
		rt.rRecs[n].locked = false
	}

	return rec, nil
}

func (tx *TTransaction) ReadValue_Ex(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq, isLock bool, group int) (Record, Value, bool, error) {

	var ok bool = false
	var wr *WriteRec
	var rr *ReadRec
	var err error
	w := tx.w

	rt := &tx.rt[tableID]
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
	rec, _, _, err = tx.s.GetRecByID(tableID, k, partNum)
	if err != nil {
		return nil, nil, true, err
	}

	rec.(*TRecord).GroupLock(group)

	if isLock {
		var tid TID
		ok, tid = rec.WLock(req)

		if tid > tx.maxSeen {
			tx.maxSeen = tid
		}

		if !ok {
			clog.Error("Cannot abort")
			tx.Abort(req)
			w.NStats[NRLOCKABORTS]++
			return nil, nil, true, EABORT
		}
	}

	//clog.Info("Worker %v: Trans %v RLock Table %v; Key %v Success\n", w.ID, w.NStats[NTXN], tableID, ParseKey(k, 0))

	// Success, Record it
	n := len(rt.rRecs)
	rt.rRecs = rt.rRecs[:n+1]
	rt.rRecs[n].k = k
	rt.rRecs[n].rec = rec
	rt.rRecs[n].exist = true
	if isLock {
		rt.rRecs[n].locked = true
	} else {
		rt.rRecs[n].locked = false
	}

	rec.GetValue(val, colNum)
	return rec, val, true, nil
}

func (tx *TTransaction) WriteValue_Ex(tableID int, k Key, partNum int, value Value, colNum int, req *LockReq, isDelta bool, isLock bool, group int, inputRec Record) error {

	var ok bool = false
	var wr *WriteRec
	var rr *ReadRec
	var err error
	w := tx.w

	rt := &tx.rt[tableID]
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
		if isDelta {
			wr.rec.DeltaValue(wr.vals[n], wr.cols[n])
		} else {
			wr.rec.SetValue(wr.vals[n], wr.cols[n])
		}
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
		if rr.locked {
			rr.exist = false
			n := len(rt.wRecs)
			rt.wRecs = rt.wRecs[0 : n+1]
			wr = &rt.wRecs[n]
			wr.k = k
			wr.partNum = partNum
			wr.rec = rr.rec
			wr.vals = wr.vals[0:1]
			wr.vals[0] = value
			wr.cols = wr.cols[0:1]
			wr.cols[0] = colNum
			wr.isDelta = wr.isDelta[0:1]
			wr.isDelta[0] = isDelta
		}
		if isDelta {
			rr.rec.DeltaValue(value, colNum)
		} else {
			rr.rec.SetValue(value, colNum)
		}
		return nil
	}

	if inputRec == nil {
		rec, _, _, err = tx.s.GetRecByID(tableID, k, partNum)
		if err != nil {
			return err
		}
	} else {
		rec = inputRec
	}

	rec.(*TRecord).GroupLock(group)

	var tid TID
	ok, tid = rec.WLock(req)

	if tid > tx.maxSeen {
		tx.maxSeen = tid
	}

	if ok {
		n := len(rt.wRecs)
		rt.wRecs = rt.wRecs[0 : n+1]
		wr = &rt.wRecs[n]
		wr.k = k
		wr.partNum = partNum
		wr.rec = rec
		wr.vals = wr.vals[0:1]
		wr.vals[0] = value
		wr.cols = wr.cols[0:1]
		wr.cols[0] = colNum
		wr.isDelta = wr.isDelta[0:1]
		wr.isDelta[0] = isDelta
		if isDelta {
			wr.rec.DeltaValue(wr.vals[0], wr.cols[0])
		} else {
			wr.rec.SetValue(wr.vals[0], wr.cols[0])
		}
		return nil
	} else {
		clog.Info("Cannot Abort")
		w.NStats[NWLOCKABORTS]++
		tx.Abort(req)
		return EABORT
	}
}

func (tx *TTransaction) Commit_Ex(req *LockReq, isHome bool, group int) TID {

	w := tx.w
	s := tx.s

	//for i := 0; i < len(l.rt); i++ {
	for i := len(tx.rt) - 1; i >= 0; i-- {
		t := &tx.rt[i]

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

	for i := 0; i < len(tx.rt); i++ {
		t := &tx.rt[i]

		for j, _ := range t.wRecs {
			wr := &t.wRecs[j]

			// For SSI, write multiple versions
			if i == STOCK {
				Insert_NewVersion(wr.k, wr.vals, wr.cols)
			}

			wr.vals = wr.vals[:0]
			wr.cols = wr.cols[:0]
			wr.isDelta = wr.isDelta[:0]
			//clog.Info("Worker %v: Trans %v WUnlock Table %v; Key %v\n", w.ID, w.NStats[NTXN], i, ParseKey(wr.k, 0))
			wr.rec.WUnlock(req, tx.maxSeen)
			wr.rec.(*TRecord).GroupUnlock(group)

		}
		t.wRecs = t.wRecs[:0]

		for j, _ := range t.rRecs {
			rr := &t.rRecs[j]
			if rr.exist {
				rr.rec.(*TRecord).GroupUnlock(group)
				if rr.locked {
					rr.rec.WUnlock(req, tx.maxSeen)
				}
			}
			rr.exist = false
		}
		t.rRecs = t.rRecs[:0]
	}

	return 1
}
