package testbed

import (
	"container/list"
	"flag"
	"time"

	//"github.com/totemtang/cc-testbed/clog"
)

const (
	HISTOGRAMLEN = 100
	CACHESIZE    = 100
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
	partStat    []int64
	partLenStat int64
	recStat     []int64
	readCount   int64
	writeCount  int64
	hits        int64
	accessCount int64
	conflicts   int64
	padding1    [PADDING]byte
	//recStat     [][]int64

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

	/*
		for i, _ := range ri.recStat {
			for j, _ := range ri.recStat[i] {
				ri.recStat[i][j] = 0
			}
		}
	*/

	for i, _ := range ri.recStat {
		ri.recStat[i] = 0
	}

	ri.readCount = 0
	ri.writeCount = 0
	ri.txnSample = 0

	ri.accessCount = 0
	ri.conflicts = 0
	ri.hits = 0

}

func NewReportInfo(nParts int, tableCount int) *ReportInfo {
	ri := &ReportInfo{}

	ri.partStat = make([]int64, 2*PADDINGINT64+nParts)
	ri.partStat = ri.partStat[PADDINGINT64 : PADDINGINT64+nParts]

	ri.recStat = make([]int64, 2*PADDINGINT64+tableCount)
	ri.recStat = ri.recStat[PADDINGINT64 : PADDINGINT64+tableCount]

	/*
		for i := 0; i < tableCount; i++ {
			ri.recStat[i] = make([]int64, 2*PADDINGINT64+HISTOGRAMLEN)
			ri.recStat[i] = ri.recStat[i][PADDINGINT64 : PADDINGINT64+HISTOGRAMLEN]
		}
	*/

	return ri
}

type SampleTool struct {
	padding0     [PADDING]byte
	nParts       int
	tableCount   int
	IDToKeyRange [][]int64
	sampleCount  int
	sampleRate   int
	lru          *LRU
	padding1     [PADDING]byte
	//IDToKeys     []int64
	//IDToKeyLen   []int
	//period   []int64
	//offset   []int64
	//offIndex []int
}

func NewSampleTool(nParts int, IDToKeyRange [][]int64, sampleRate int) *SampleTool {
	st := &SampleTool{
		nParts:       nParts,
		tableCount:   len(IDToKeyRange),
		IDToKeyRange: IDToKeyRange,
		sampleRate:   sampleRate,
		lru:          NewLRU(CACHESIZE),
	}

	/*
		st.period = make([]int64, 2*PADDINGINT64+st.tableCount)
		st.offset = make([]int64, 2*PADDINGINT64+st.tableCount)
		st.offIndex = make([]int, 2*PADDINGINT+st.tableCount)
		st.IDToKeys = make([]int64, 2*PADDINGINT64+st.tableCount)
		st.IDToKeyLen = make([]int, 2*PADDINGINT+st.tableCount)

		st.period = st.period[PADDINGINT64 : PADDINGINT64+st.tableCount]
		st.offset = st.offset[PADDINGINT64 : PADDINGINT64+st.tableCount]
		st.offIndex = st.offIndex[PADDINGINT : PADDINGINT+st.tableCount]
		st.IDToKeys = st.IDToKeys[PADDINGINT64 : PADDINGINT64+st.tableCount]
		st.IDToKeyLen = st.IDToKeyLen[PADDINGINT : PADDINGINT+st.tableCount]

		for i := 0; i < st.tableCount; i++ {
			keyLen := len(st.IDToKeyRange[i])
			var nKeys int64 = 1
			for j := 0; j < keyLen; j++ {
				nKeys *= st.IDToKeyRange[i][j]
			}
			st.period[i] = nKeys / HISTOGRAMLEN
			r := nKeys % HISTOGRAMLEN

			st.offset[i] = (st.period[i] + 1) * r
			st.offIndex[i] = int(r)
			st.IDToKeys[i] = nKeys
			st.IDToKeyLen[i] = keyLen
		}
	*/

	return st
}

func (st *SampleTool) oneSample(tableID int, key Key, ri *ReportInfo, isRead bool) {
	if st.sampleCount != 0 {
		return
	}

	/*
		var intKey int64 = int64(ParseKey(key, st.IDToKeyLen[tableID]-1))

		for i := st.IDToKeyLen[tableID] - 2; i >= 0; i-- {
			intKey *= st.IDToKeyRange[tableID][i]
			intKey += int64(ParseKey(key, i))
		}

		var index int64
		if intKey < st.offset[tableID] {
			index = intKey / (st.period[tableID] + 1)
		} else {
			index = int64(st.offIndex[tableID]) + (intKey-st.offset[tableID])/st.period[tableID]
		}

		ri.recStat[tableID][index]++
	*/

	ri.recStat[tableID]++

	if st.lru.Insert(key) {
		ri.hits++
	}

	if isRead {
		ri.readCount++
	} else {
		ri.writeCount++
	}
}

func (st *SampleTool) onePartSample(ap []int, ri *ReportInfo) {
	st.sampleCount++
	if st.sampleCount < st.sampleRate {
		return
	}
	st.sampleCount = 0
	ri.txnSample++

	for _, p := range ap {
		ri.partStat[p]++
	}

	ri.partLenStat += int64(len(ap) * len(ap))
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
	//st.lru.Reset()
	st.lru = NewLRU(st.lru.size)
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
