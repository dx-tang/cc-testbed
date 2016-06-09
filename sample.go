package testbed

import (
	"container/list"
	"flag"
	"runtime/debug"
	"time"

	//"github.com/totemtang/cc-testbed/wfmutex"
	"github.com/totemtang/cc-testbed/clog"
)

var Report = flag.Bool("report", false, "whether periodically report runtime information to coordinator")

type ReportInfo struct {
	padding0        [PADDING]byte
	execTime        time.Duration
	prevExec        time.Duration
	genTime         time.Duration
	prevGen         time.Duration
	txn             int64
	aborts          int64
	prevTxn         int64
	prevAborts      int64
	txnSample       int64
	partTotal       int64
	partStat        []int64
	partLenStat     int64
	recStat         []int64
	readCount       int64
	writeCount      int64
	totalCount      int64
	hits            int64
	accessCount     []int64
	conflicts       []int64
	accessHomeCount []int64
	homeConflicts   []int64
	partAccess      int64
	partSuccess     int64
	latency         int64
	padding1        [PADDING]byte
}

func (ri *ReportInfo) Reset() {
	ri.execTime = 0
	ri.prevExec = 0
	ri.genTime = 0
	ri.prevGen = 0
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
		ri.accessCount[i] = 0
		ri.accessHomeCount[i] = 0
		ri.conflicts[i] = 0
		ri.homeConflicts[i] = 0

	}

	ri.readCount = 0
	ri.writeCount = 0
	ri.txnSample = 0
	ri.totalCount = 0

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

	ri.accessCount = make([]int64, 2*PADDINGINT64+tableCount)
	ri.accessCount = ri.accessCount[PADDINGINT64 : PADDINGINT64+tableCount]

	ri.conflicts = make([]int64, 2*PADDINGINT64+tableCount)
	ri.conflicts = ri.conflicts[PADDINGINT64 : PADDINGINT64+tableCount]

	ri.accessHomeCount = make([]int64, 2*PADDINGINT64+tableCount)
	ri.accessHomeCount = ri.accessHomeCount[PADDINGINT64 : PADDINGINT64+tableCount]

	ri.homeConflicts = make([]int64, 2*PADDINGINT64+tableCount)
	ri.homeConflicts = ri.homeConflicts[PADDINGINT64 : PADDINGINT64+tableCount]

	return ri
}

type SampleTool struct {
	padding0    [PADDING]byte
	nParts      int
	isPartition bool
	tableCount  int
	sampleRate  int
	sampleCount int
	trueRate    int
	trueCounter int
	lruAr       []*LRU
	ap          []int
	cur         int
	partTrial   int
	s           *Store
	homeSample  ConfSample
	allSample   ConfSample
	padding1    [PADDING]byte
}

type ConfSample struct {
	sampleAccess int
	recBuf       []*ARecord
	tableBuf     []int
	trials       int
	state        int
	recRate      int
}

func NewSampleTool(nParts int, sampleRate int, s *Store) *SampleTool {
	st := &SampleTool{
		nParts:      nParts,
		isPartition: s.isPartition,
		tableCount:  len(s.priTables),
		trueRate:    sampleRate,
		trueCounter: 0,
		//lruAr:        make([]*LRU, len(IDToKeyRange)),
	}

	if !st.isPartition {
		st.sampleRate = sampleRate / (*NumPart)
		st.homeSample.recRate = RECSR / (*NumPart)
	} else {
		st.sampleRate = sampleRate - sampleRate%(*NumPart)
		st.homeSample.recRate = RECSR - RECSR%(*NumPart)
	}

	st.allSample.recRate = RECSR - RECSR%(*NumPart)

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

	st.allSample.recBuf = make([]*ARecord, BUFSIZE+2*PADDINGINT64)
	st.allSample.recBuf = st.allSample.recBuf[PADDINGINT64:PADDINGINT64]
	st.allSample.tableBuf = make([]int, BUFSIZE+2*PADDINGINT)
	st.allSample.tableBuf = st.allSample.tableBuf[PADDINGINT:PADDINGINT]

	st.homeSample.recBuf = make([]*ARecord, BUFSIZE+2*PADDINGINT64)
	st.homeSample.recBuf = st.homeSample.recBuf[PADDINGINT64:PADDINGINT64]
	st.homeSample.tableBuf = make([]int, BUFSIZE+2*PADDINGINT)
	st.homeSample.tableBuf = st.homeSample.tableBuf[PADDINGINT:PADDINGINT]

	return st
}

func (st *SampleTool) oneSampleConf(tableID int, key Key, partNum int, s *Store, ri *ReportInfo, isHome bool) {
	var sample *ConfSample
	if isHome {
		sample = &st.homeSample
	} else {
		sample = &st.allSample
	}

	// We need acquire more locks
	if sample.state == 0 {
		for i, rec := range sample.recBuf {
			if rec.key == key && sample.tableBuf[i] == tableID {
				return
			}
		}

		rec, err := s.GetRecByID(tableID, key, partNum)
		if err != nil {
			debug.PrintStack()
			clog.Error("Error No Key in Sample")
		}
		tmpRec := rec.(*ARecord)
		if isHome {
			tmpRec.cd.IncrHomeCounter()
		} else {
			tmpRec.cd.IncrCounter()
		}

		n := len(sample.recBuf)
		sample.recBuf = sample.recBuf[0 : n+1]
		sample.recBuf[n] = tmpRec
		sample.tableBuf = sample.tableBuf[0 : n+1]
		sample.tableBuf[n] = tableID
		if n+1 == BUFSIZE {
			sample.state = 1
		}
		return
	}

	var ok bool = false
	conf := int32(0)
	for i, rec := range sample.recBuf {
		if rec.key == key && sample.tableBuf[i] == tableID {
			ok = true
			conf++
		}
	}

	//tm := time.Now()
	rec, err := s.GetRecByID(tableID, key, partNum)
	if err != nil {
		clog.Error("Error No Key in Sample")
	}
	//ri.latency += time.Since(tm).Nanoseconds()
	tmpRec := rec.(*ARecord)
	var x int32
	if isHome {
		x = tmpRec.cd.CheckCounterHome()
	} else {
		x = tmpRec.cd.CheckCounter()
	}
	if x != 0 {
		if ok {
			conf = x - conf
		} else {
			conf = x
		}
	}

	sample.trials++
	if sample.trials == TRIAL {
		sample.trials = 0
		sample.state = 0
		for _, rec := range sample.recBuf {
			if isHome {
				rec.cd.DecHomeCounter()
			} else {
				rec.cd.DecCounter()
			}
		}
		sample.recBuf = sample.recBuf[0:0]
		sample.tableBuf = sample.tableBuf[0:0]
	}

	if isHome {
		ri.accessHomeCount[tableID]++
		ri.homeConflicts[tableID] += int64(conf)
	} else {
		ri.accessCount[tableID]++
		ri.conflicts[tableID] += int64(conf)
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

func (st *SampleTool) onePartSample(ap []int, ri *ReportInfo, pi int) {
	//plus := int64((len(ap) - 1))
	//ri.partTotal += plus*plus + 1

	//ri.partStat[pi]++

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
			//if cur > 1 {
			//	ri.partAccess += int64(cur + 1)
			//}
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

	/*if conflict {
		ri.conflicts++
	} else {
		ri.accessCount++
	}*/

}

func (st *SampleTool) Reset() {
	st.sampleCount = 0
	st.trueCounter = 0

	sample := &st.allSample
	sample.sampleAccess = 0
	sample.trials = 0
	sample.state = 0
	for _, rec := range sample.recBuf {
		rec.cd.CleanCounter()
	}
	sample.recBuf = sample.recBuf[0:0]

	sample = &st.homeSample
	sample.sampleAccess = 0
	sample.trials = 0
	sample.state = 0
	for _, rec := range sample.recBuf {
		rec.cd.CleanHomeCounter()
	}
	sample.recBuf = sample.recBuf[0:0]

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

func (st *SampleTool) reconf(isPartition bool) {
	st.isPartition = isPartition
	if isPartition {
		st.sampleRate = st.sampleRate * (*NumPart)
		st.homeSample.recRate = st.homeSample.recRate * (*NumPart)
	} else {
		st.sampleRate = st.sampleRate / (*NumPart)
		st.homeSample.recRate = st.homeSample.recRate / (*NumPart)
	}
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
