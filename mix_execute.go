package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
)

type MTransaction struct {
	padding0    [PADDING]byte
	w           *Worker
	s           *Store
	st          *SampleTool
	lockTrack   []RecTable    // 2PL
	pccTrack    []PTrackTable // PCC
	occTrack    []TrackTable  // OCC
	occMaxSeen  TID
	lockMaxSeen TID
	partToExec  []int
	padding     [PADDING]byte
}

func StartMTransaction(w *Worker, nTables int, wc []WorkerConfig) *MTransaction {
	tx := &MTransaction{
		w:          w,
		s:          w.store,
		st:         w.st,
		pccTrack:   make([]PTrackTable, nTables, MAXTABLENUM),
		lockTrack:  make([]RecTable, nTables, MAXTABLENUM),
		occTrack:   make([]TrackTable, nTables, MAXTABLENUM),
		partToExec: make([]int, len(wc)),
	}

	for i := 0; i < len(wc); i++ {
		tx.partToExec[i] = int(wc[i].protocol)
	}

	// pcc
	for i := 0; i < len(tx.pccTrack); i++ {
		t := &tx.pccTrack[i]
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

	for i := 0; i < len(tx.lockTrack); i++ {
		t := &tx.lockTrack[i]
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

	for j := 0; j < len(tx.occTrack); j++ {
		t := &tx.occTrack[j]
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

		t.rBucket = make([]ReadBucket, MAXTRACKINGKEY)
		t.rBucket = t.rBucket[0:0]
	}

	return tx
}

func (m *MTransaction) Reset(t Trans) {
	m.occMaxSeen = TID(0)
	m.lockMaxSeen = TID(0)
}

func (m *MTransaction) ReadValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq, isHome bool) (Record, Value, bool, error) {
	if m.partToExec[partNum] == PARTITION {
		t := &m.pccTrack[tableID]
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

		s := m.s
		//err := s.GetValueByID(tableID, k, partNum, val, colNum)
		rec, _, _, err := s.GetRecByID(tableID, k, partNum)
		if err != nil {
			return nil, nil, true, err
		}
		rec.GetValue(val, colNum)
		return rec, val, true, nil
	} else if m.partToExec[partNum] == LOCKING { // 2PL
		var ok bool = false
		var wr *WriteRec
		var rr *ReadRec
		var err error
		w := m.w

		rt := &m.lockTrack[tableID]
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
		rec, _, _, err = m.s.GetRecByID(tableID, k, partNum)
		if err != nil {
			return nil, nil, true, err
		}

		var tid TID
		ok, tid = rec.RLock(req)

		if tid > m.lockMaxSeen {
			m.lockMaxSeen = tid
		}

		if !ok {
			w.NStats[NRLOCKABORTS]++
			m.Abort(req)
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
	} else { // OCC
		var ok bool
		var tid TID
		var t *TrackTable
		var r Record
		var err error

		t = &m.occTrack[tableID]

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
					m.Abort(req)
					m.w.NStats[NREADABORTS]++
					return nil, nil, true, EABORT
				}
				rk.rec.GetValue(val, colNum)
				return rk.rec, val, true, nil
			}
		}

		if r == nil {
			//if WLTYPE != TPCCWL {
			r, _, _, err = m.s.GetRecByID(tableID, k, partNum)
			if err != nil {
				return nil, nil, true, ENOKEY
			}
		}

		ok, tid = r.IsUnlocked()

		if tid > m.occMaxSeen {
			m.occMaxSeen = tid
		}

		if !ok {
			m.Abort(req)
			m.w.NStats[NREADABORTS]++
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
}

func (m *MTransaction) WriteValue(tableID int, k Key, partNum int, val Value, colNum int, req *LockReq, isDelta bool, isHome bool, inputRec Record) error {
	if m.partToExec[partNum] == PARTITION {
		t := &m.pccTrack[tableID]
		for i := 0; i < len(t.wRecs); i++ {
			wr := &t.wRecs[i]
			if wr.k == k {
				n := len(wr.cols)
				wr.cols = wr.cols[0 : n+1]
				wr.cols[n] = colNum
				wr.vals = wr.vals[0 : n+1]
				wr.vals[n] = val
				wr.isDelta = wr.isDelta[0 : n+1]
				wr.isDelta[n] = isDelta
				return nil
			}
		}

		s := m.s
		var err error
		var r Record
		if inputRec == nil {
			r, _, _, err = s.GetRecByID(tableID, k, partNum)
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
		t.wRecs[n].vals[0] = val
		t.wRecs[n].isDelta = t.wRecs[n].isDelta[0:1]
		t.wRecs[n].isDelta[0] = isDelta
		return nil
	} else if m.partToExec[partNum] == LOCKING {
		var ok bool = false
		var wr *WriteRec
		var rr *ReadRec
		var err error
		w := m.w

		rt := &m.lockTrack[tableID]
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
				m.Abort(req)
				return EABORT
			}
		}

		if inputRec == nil {
			rec, _, _, err = m.s.GetRecByID(tableID, k, partNum)
			if err != nil {
				return err
			}
		} else {
			rec = inputRec
		}

		var tid TID
		ok, tid = rec.WLock(req)

		if tid > m.lockMaxSeen {
			m.lockMaxSeen = tid
		}

		if ok {
			n := len(rt.wRecs)
			rt.wRecs = rt.wRecs[0 : n+1]
			wr := &rt.wRecs[n]
			wr.k = k
			wr.partNum = partNum
			wr.rec = rec
			wr.vals = wr.vals[0:1]
			wr.vals[0] = val
			wr.cols = wr.cols[0:1]
			wr.cols[0] = colNum
			wr.isDelta = wr.isDelta[0:1]
			wr.isDelta[0] = isDelta
			return nil
		} else {
			w.NStats[NWLOCKABORTS]++
			m.Abort(req)
			return EABORT
		}
	} else { // OCC
		var ok bool
		//var tid TID
		var t *TrackTable
		var r Record
		var err error

		ok = false
		t = &m.occTrack[tableID]
		for j := 0; j < len(t.wKeys); j++ {
			wk := &t.wKeys[j]
			if wk.k == k {
				ok = true
				r = wk.rec
				n := len(wk.vals)
				wk.vals = wk.vals[0 : n+1]
				wk.vals[n] = val
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
					//var bucket Bucket
					//var version uint64
					r, _, _, err = m.s.GetRecByID(tableID, k, partNum)
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
			t.wKeys[n].vals[0] = val
			t.wKeys[n].cols = t.wKeys[n].cols[0:1]
			t.wKeys[n].cols[0] = colNum
			t.wKeys[n].isDelta = t.wKeys[n].isDelta[0:1]
			t.wKeys[n].isDelta[0] = isDelta
		}

		return nil
	}
}

func (m *MTransaction) MayWrite(tableID int, k Key, partNum int, req *LockReq) error {
	return nil
}

func (m *MTransaction) InsertRecord(tableID int, k Key, partNum int, rec Record) error {
	s := m.s
	err := s.PrepareInsert(tableID, k, partNum)
	if err != nil {
		return err
	}
	if m.partToExec[partNum] == PARTITION {
		t := &m.pccTrack[tableID]
		n := len(t.iRecs)
		t.iRecs = t.iRecs[0 : n+1]
		t.iRecs[n].k = k
		t.iRecs[n].partNum = partNum
		t.iRecs[n].rec = rec

	} else if m.partToExec[partNum] == LOCKING {
		t := &m.lockTrack[tableID]
		n := len(t.iRecs)
		t.iRecs = t.iRecs[0 : n+1]
		t.iRecs[n].k = k
		t.iRecs[n].partNum = partNum
		t.iRecs[n].rec = rec

	} else {
		t := &m.occTrack[tableID]
		n := len(t.iRecs)
		t.iRecs = t.iRecs[0 : n+1]
		t.iRecs[n].k = k
		t.iRecs[n].partNum = partNum
		t.iRecs[n].rec = rec
	}
	rec.SetKey(k)

	return nil
}

func (m *MTransaction) DeleteRecord(tableID int, k Key, partNum int) (Record, error) {
	return nil, nil
}

func (m *MTransaction) GetKeysBySecIndex(tableID int, k Key, partNum int, val Value) error {
	return m.s.GetValueBySec(tableID, k, partNum, val)
}

func (m *MTransaction) GetRecord(tableID int, k Key, partNum int, req *LockReq, isHome bool) (Record, error) {
	if m.partToExec[partNum] == PARTITION {
		rec, _, _, err := m.s.GetRecByID(tableID, k, partNum)
		if err != nil {
			return nil, err
		}
		return rec, err
	} else if m.partToExec[partNum] == LOCKING {
		var rec Record
		var err error
		w := m.w

		// Try RLock
		rec, _, _, err = m.s.GetRecByID(tableID, k, partNum)
		if err != nil {
			return nil, err
		}

		ok, tid := rec.RLock(req)

		if tid > m.lockMaxSeen {
			m.lockMaxSeen = tid
		}

		if !ok {
			m.Abort(req)
			w.NStats[NRLOCKABORTS]++
			return nil, EABORT
		}

		//clog.Info("Worker %v: Trans %v RLock Table %v; Key %v Success\n", w.ID, w.NStats[NTXN], tableID, ParseKey(k, 0))

		rt := &m.lockTrack[tableID]

		// Success, Record it
		n := len(rt.rRecs)
		rt.rRecs = rt.rRecs[:n+1]
		rt.rRecs[n].k = k
		rt.rRecs[n].rec = rec
		rt.rRecs[n].exist = true

		return rec, nil
	} else {
		var ok bool
		var tid TID
		var t *TrackTable = &m.occTrack[tableID]
		var r Record
		var err error

		r, _, _, err = m.s.GetRecByID(tableID, k, partNum)
		if err != nil {
			return nil, err
		}

		ok, tid = r.IsUnlocked()

		if tid > m.occMaxSeen {
			m.occMaxSeen = tid
		}

		if !ok {
			m.w.NStats[NREADABORTS]++
			return nil, EABORT
		}

		n := len(t.rKeys)
		t.rKeys = t.rKeys[0 : n+1]
		t.rKeys[n].k = k
		t.rKeys[n].last = tid
		t.rKeys[n].rec = r

		return r, nil
	}
}

func (m *MTransaction) Abort(req *LockReq) TID {
	s := m.s
	for i := 0; i < len(m.pccTrack); i++ {
		pccT := &m.pccTrack[i]
		lockT := &m.lockTrack[i]
		occT := &m.occTrack[i]

		// Release Writes
		// PCC
		for j := 0; j < len(pccT.wRecs); j++ {
			wr := &pccT.wRecs[j]
			wr.vals = wr.vals[:0]
			wr.cols = wr.cols[:0]
			wr.isDelta = wr.isDelta[:0]
		}
		pccT.wRecs = pccT.wRecs[0:0]

		// LOCKING
		for j, _ := range lockT.rRecs {
			rr := &lockT.rRecs[j]
			if rr.exist {
				//clog.Info("Worker %v: Trans %v RUnlock Table %v; Key %v\n", w.ID, w.NStats[NTXN], i, ParseKey(rr.k, 0))
				rr.rec.RUnlock(req)
			}
		}
		lockT.rRecs = lockT.rRecs[:0]
		for j, _ := range lockT.wRecs {
			wr := &lockT.wRecs[j]
			wr.vals = wr.vals[:0]
			wr.cols = wr.cols[:0]
			wr.isDelta = wr.isDelta[:0]
			//clog.Info("Worker %v: Trans %v WUnlock Table %v; Key %v\n", w.ID, w.NStats[NTXN], i, ParseKey(wr.k, 0))
			wr.rec.WUnlock(req, m.lockMaxSeen)
		}
		lockT.wRecs = lockT.wRecs[:0]

		// OCC
		occT.rKeys = occT.rKeys[:0]
		for j := 0; j < len(occT.wKeys); j++ {
			wk := &occT.wKeys[j]
			if wk.locked {
				wk.rec.Unlock(m.occMaxSeen)
			}
		}
		occT.wKeys = occT.wKeys[:0]

		// For Insert
		// PCC
		for j := 0; j < len(pccT.iRecs); j++ {
			s.ReleaseInsert(i, pccT.iRecs[j].k, pccT.iRecs[j].partNum)
		}
		pccT.iRecs = pccT.iRecs[0:0]

		// LOCKING
		for j := 0; j < len(lockT.iRecs); j++ {
			s.ReleaseInsert(i, lockT.iRecs[j].k, lockT.iRecs[j].partNum)
		}
		lockT.iRecs = lockT.iRecs[:0]

		// OCC
		for j := 0; j < len(occT.iRecs); j++ {
			s.ReleaseInsert(i, occT.iRecs[j].k, occT.iRecs[j].partNum)
		}
		occT.iRecs = occT.iRecs[:0]

		// For Delete
		// PCC
		for j := 0; j < len(pccT.dRecs); j++ {
			s.ReleaseDelete(i, pccT.dRecs[j].k, pccT.dRecs[j].partNum)
		}
		pccT.dRecs = pccT.dRecs[0:0]

		// LOCKING
		for j := 0; j < len(lockT.dRecs); j++ {
			s.ReleaseDelete(i, lockT.dRecs[j].k, lockT.dRecs[j].partNum)
		}
		lockT.dRecs = lockT.dRecs[:0]

		// OCC
		for j := 0; j < len(occT.dRecs); j++ {
			s.ReleaseDelete(i, occT.dRecs[j].k, occT.dRecs[j].partNum)
		}
		occT.dRecs = occT.dRecs[:0]

	}

	return 0
}

func (m *MTransaction) Commit(req *LockReq, isHome bool) TID {
	// OCC Validate
	// Phase 1: Lock all write keys
	//for _, wk := range o.wKeys {

	for j := 0; j < len(m.occTrack); j++ {
		t := &m.occTrack[j]
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
					m.w.NStats[NLOCKABORTS]++
					return m.Abort(req)
				}
			}
			wk.locked = true
			if former > m.occMaxSeen {
				m.occMaxSeen = former
			}
		}
	}

	tid := m.w.commitTID()
	if tid <= m.occMaxSeen {
		m.w.ResetTID(m.occMaxSeen)
		tid = m.w.commitTID()
		if tid < m.occMaxSeen {
			clog.Error("%v MaxSeen %v, reset TID but %v<%v", m.w.ID, m.occMaxSeen, tid, m.occMaxSeen)
		}
	}

	// Phase 2: Check conflicts
	//for k, rk := range o.rKeys {
	for j := 0; j < len(m.occTrack); j++ {
		t := &m.occTrack[j]
		for i := 0; i < len(t.rKeys); i++ {
			k := t.rKeys[i].k
			rk := &t.rKeys[i]
			//verify whether TID has changed
			var ok1, ok2 bool
			var tmpTID TID
			ok1, tmpTID = rk.rec.IsUnlocked()
			if tmpTID != rk.last {
				m.w.NStats[NRCHANGEABORTS]++
				return m.Abort(req)
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
				m.w.NStats[NRWABORTS]++
				return m.Abort(req)
			}
		}
	}

	// PCC Commit
	s := m.Store()
	w := m.w
	//for i := 0; i < len(p.tt); i++ {
	for i := len(m.pccTrack) - 1; i >= 0; i-- {
		pccT := &m.pccTrack[i]
		occT := &m.occTrack[i]
		lockT := &m.lockTrack[i]

		// PCC
		if len(pccT.iRecs) != 0 {
			s.InsertRecord(i, pccT.iRecs, w.iaAR[i])
		}
		pccT.iRecs = pccT.iRecs[0:0]

		for j := 0; j < len(pccT.dRecs); j++ {
			s.DeleteRecord(i, pccT.dRecs[j].k, pccT.dRecs[j].partNum)
		}
		pccT.dRecs = pccT.dRecs[0:0]

		// LOCK
		if len(lockT.iRecs) != 0 {
			s.InsertRecord(i, lockT.iRecs, w.iaAR[i])
		}
		lockT.iRecs = lockT.iRecs[:0]

		for j := 0; j < len(lockT.dRecs); j++ {
			s.DeleteRecord(i, lockT.dRecs[j].k, lockT.dRecs[j].partNum)
		}
		lockT.dRecs = lockT.dRecs[:0]

		// OCC
		if len(occT.iRecs) != 0 {
			s.InsertRecord(i, occT.iRecs, w.iaAR[i])
		}
		occT.iRecs = occT.iRecs[:0]

		for j := 0; j < len(occT.dRecs); j++ {
			s.DeleteRecord(i, occT.dRecs[j].k, occT.dRecs[j].partNum)
		}
		occT.dRecs = occT.dRecs[:0]
	}

	for i := len(m.pccTrack) - 1; i >= 0; i-- {
		pccT := &m.pccTrack[i]
		occT := &m.occTrack[i]
		lockT := &m.lockTrack[i]

		// PCC
		for j := 0; j < len(pccT.wRecs); j++ {
			wr := &pccT.wRecs[j]
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
		pccT.wRecs = pccT.wRecs[0:0]

		// LOCKING
		lockT.rRecs = lockT.rRecs[:0]
		for j, _ := range lockT.wRecs {
			wr := &lockT.wRecs[j]
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
			wr.rec.WUnlock(req, m.lockMaxSeen)
		}
		lockT.wRecs = lockT.wRecs[:0]

		// OCC
		for i, _ := range occT.wKeys {
			wk := &occT.wKeys[i]
			for j := 0; j < len(wk.vals); j++ {
				if wk.isDelta[j] {
					wk.rec.DeltaValue(wk.vals[j], wk.cols[j])
				} else {
					wk.rec.SetValue(wk.vals[j], wk.cols[j])
				}
			}

			wk.vals = wk.vals[:0]
			wk.cols = wk.cols[:0]
			wk.isDelta = wk.isDelta[:0]

			wk.rec.Unlock(tid)
			wk.locked = false
		}
		occT.rKeys = occT.rKeys[:0]
		occT.wKeys = occT.wKeys[:0]

	}

	return 1
}

func (m *MTransaction) Store() *Store {
	return m.s
}

func (m *MTransaction) Worker() *Worker {
	return m.w
}

func (m *MTransaction) GetType() int {
	return ADAPTIVE
}
