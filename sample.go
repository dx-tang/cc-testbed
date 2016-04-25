package testbed

import (
	"container/list"
	"flag"
	"time"

	//"github.com/totemtang/cc-testbed/wfmutex"
	"github.com/totemtang/cc-testbed/clog"
)

const (
	HISTOGRAMLEN = 100
	CACHESIZE    = 1000
	BUFSIZE      = 5
	TRIAL        = 20
	RECSR        = 1000
)

var Report = flag.Bool("report", false, "whether periodically report runtime information to coordinator")

type ReportInfo struct {
	padding0    [PADDING]byte
	execTime    time.Duration
	prevExec    time.Duration
	txn         int64
	aborts      int64
	prevTxn     int64
	prevAborts  int64
	txnSample   int64
	partTotal   int64
	partStat    []int64
	partLenStat int64
	recStat     []int64
	readCount   int64
	writeCount  int64
	hits        int64
	accessCount int64
	conflicts   int64
	partAccess  int64
	partSuccess int64
	latency     int64
	padding1    [PADDING]byte
}

func (ri *ReportInfo) Reset() {
	ri.execTime = 0
	ri.prevExec = 0
	ri.txn = 0
	ri.aborts = 0
	ri.prevTxn = 0
	ri.prevAborts = 0

	for i, _ := range ri.partStat {
		ri.partStat[i] = 0
	}
	ri.partLenStat = 0

	ri.partTotal = 0

	for i, _ := range ri.recStat {
		ri.recStat[i] = 0
	}

	ri.readCount = 0
	ri.writeCount = 0
	ri.txnSample = 0

	ri.accessCount = 0
	ri.conflicts = 0
	ri.hits = 0

	ri.partAccess = 0
	ri.partSuccess = 0

	ri.latency = 0

}

func NewReportInfo(nParts int, tableCount int) *ReportInfo {
	ri := &ReportInfo{}

	ri.partStat = make([]int64, 2*PADDINGINT64+nParts)
	ri.partStat = ri.partStat[PADDINGINT64 : PADDINGINT64+nParts]

	ri.recStat = make([]int64, 2*PADDINGINT64+tableCount)
	ri.recStat = ri.recStat[PADDINGINT64 : PADDINGINT64+tableCount]

	return ri
}

type SampleTool struct {
	padding0     [PADDING]byte
	nParts       int
	tableCount   int
	sampleCount  int
	sampleAccess int
	recBuf       []*ARecord
	trials       int
	state        int
	sampleRate   int
	lruAr        []*LRU
	ap           []int
	cur          int
	partTrial    int
	s            *Store
	padding1     [PADDING]byte
}

func NewSampleTool(nParts int, sampleRate int, s *Store) *SampleTool {
	st := &SampleTool{
		nParts:     nParts,
		tableCount: len(s.tables),
		sampleRate: sampleRate,
		//lruAr:        make([]*LRU, len(IDToKeyRange)),
	}

	//for i := 0; i < len(IDToKeyRange); i++ {
	//	st.lruAr[i] = NewLRU(CACHESIZE)
	//}

	st.ap = make([]int, nParts+2*PADDINGINT)
	st.ap = st.ap[PADDINGINT:PADDINGINT]
	//st.ap = st.ap[PADDINGINT : PADDINGINT+nParts]
	st.s = s
	st.cur = -1
	//st.cur = 0
	//st.partTrial = 0

	st.recBuf = make([]*ARecord, BUFSIZE+2*PADDINGINT64)
	st.recBuf = st.recBuf[PADDINGINT64:PADDINGINT64]

	return st
}

func (st *SampleTool) oneSampleConf(tableID int, key Key, partNum int, s *Store, ri *ReportInfo) {
	// We need acquire more locks
	if st.state == 0 {
		for _, rec := range st.recBuf {
			if rec.key == key {
				return
			}
		}

		rec, err := s.GetRecByID(tableID, key, partNum)
		if err != nil {
			clog.Error("Error No Key in Sample")
		}
		tmpRec := rec.(*ARecord)
		ok := tmpRec.conflict.RLock()
		if ok {
			n := len(st.recBuf)
			st.recBuf = st.recBuf[0 : n+1]
			st.recBuf[n] = tmpRec
			if n+1 == BUFSIZE {
				st.state = 1
			}
		}

		return
	}

	ri.accessCount++
	var ok bool = false
	conf := int32(0)
	for _, rec := range st.recBuf {
		if rec.key == key {
			ok = true
			conf++
		}
	}

	tm := time.Now()
	rec, err := s.GetRecByID(tableID, key, partNum)
	if err != nil {
		clog.Error("Error No Key in Sample")
	}
	ri.latency += time.Since(tm).Nanoseconds()
	tmpRec := rec.(*ARecord)
	x := tmpRec.conflict.CheckLock()
	if x != 0 {
		if ok {
			ri.conflicts += int64(x - conf)
		} else {
			ri.conflicts += int64(x)
		}
	}

	st.trials++
	if st.trials == TRIAL {
		st.trials = 0
		st.state = 0
		for _, rec := range st.recBuf {
			rec.conflict.RUnlock()
		}
		st.recBuf = st.recBuf[0:0]
	}
}

func (st *SampleTool) oneSample(tableID int, ri *ReportInfo, isRead bool) {
	ri.recStat[tableID]++
	//if st.lruAr[tableID].Insert(key) {
	//	ri.hits++
	//}

	if isRead {
		ri.readCount++
	} else {
		ri.writeCount++
	}

}

func (st *SampleTool) onePartSample(ap []int, ri *ReportInfo) {
	ri.txnSample++

	plus := int64((len(ap) - 1))
	ri.partTotal += plus*plus + 1
	for _, p := range ap {
		ri.partStat[p]++
	}

	//ri.partLenStat += int64(len(ap) * len(ap))

	// Part Conflicts
	//if st.cur > 0 {
	//	ri.partAccess += int64(st.cur)
	//}

	if st.cur >= len(st.ap) {
		for _, p := range st.ap {
			st.s.wfLock[p].lock.Unlock(0)
		}
		st.cur = -1
		st.ap = st.ap[0:0]
		//ri.partSuccess++
		return
	}

	if st.cur == -1 {
		st.cur = 0
		st.ap = st.ap[0:len(ap)]
		for i, p := range ap {
			st.ap[i] = p
		}
		ri.partSuccess += int64(len(ap))
	}

	cur := st.cur
	// Try Locking
	for cur < len(st.ap) {
		ok, _ := st.s.wfLock[st.ap[cur]].lock.Lock()
		if !ok {
			ri.partAccess++
			if cur > 1 {
				ri.partAccess += int64(cur + 1)
			}
			break
		}
		cur++
	}

	//ri.partAccess += int64((len(st.ap) - cur))

	st.cur = cur

	/*if st.cur < 5 {
		for _, p := range ap {
			st.s.confLock[p].lock.RLock(0)
			st.ap[p]++
		}
		st.cur++
	} else {
		ri.partSuccess++
		for _, p := range ap {
			x := st.s.confLock[p].lock.CheckLock()
			ri.partAccess += int64(int(x) - st.ap[p])
		}
		st.partTrial++
		if st.partTrial >= 10 {
			for i, p := range st.ap {
				if p != 0 {
					st.s.confLock[i].lock.RUnlockN(int32(p))
				}
				st.ap[i] = 0
			}
			st.partTrial = 0
			st.cur = 0
		}
	}*/
}

func (st *SampleTool) oneAccessSample(conflict bool, ri *ReportInfo) {

	if conflict {
		ri.conflicts++
	} else {
		ri.accessCount++
	}

}

func (st *SampleTool) Reset() {
	st.sampleCount = 0
	st.sampleAccess = 0
	st.trials = 0
	st.state = 0
	for _, rec := range st.recBuf {
		rec.conflict.RUnlock()
	}
	st.recBuf = st.recBuf[0:0]

	//for i := 0; i < len(st.lruAr); i++ {
	//	st.lruAr[i] = NewLRU(st.lruAr[i].size)
	//}

	for i := 0; i < st.cur; i++ {
		st.s.wfLock[st.ap[i]].lock.Unlock(0)
	}
	st.ap = st.ap[0:0]
	/*ap := st.ap
	for i := 0; i < len(ap); i++ {
		//for j := 0; j < ap[i]; j++ {
		if ap[i] != 0 {
			st.s.confLock[i].lock.RUnlockN(int32(ap[i]))
		}
		ap[i] = 0
		//}
	}*/
	st.cur = -1
	//st.cur = 0
	st.partTrial = 0
}

type LRU struct {
	padding1 [PADDING]byte
	l        *list.List
	m        map[Key]*list.Element
	size     int
	padding2 [PADDING]byte
}

func NewLRU(size int) *LRU {
	lru := &LRU{
		l:    list.New(),
		m:    make(map[Key]*list.Element),
		size: size,
	}

	for i := 0; i < size; i++ {
		var k Key
		lru.l.PushBack(&k)
	}

	return lru

}

func (lru *LRU) Insert(k Key) bool {
	e, ok := lru.m[k]
	if ok {
		lru.l.MoveToFront(e)
		return true
	} else {
		back := lru.l.Back()
		kP := back.Value.(*Key)
		delete(lru.m, *kP)
		*kP = k
		lru.l.MoveToFront(back)
		lru.m[k] = back
		return false
	}
}

func (lru *LRU) Reset() {
	for e := lru.l.Front(); e != nil; e = e.Next() {
		var k Key
		kp := e.Value.(*Key)
		delete(lru.m, *kp)
		*kp = k
	}
}
