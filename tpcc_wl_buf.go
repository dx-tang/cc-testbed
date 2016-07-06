package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
)

type OrderAllocator struct {
	padding1 [PADDING]byte
	lRecs    []LRecord
	oRecs    []ORecord
	pRecs    []PRecord
	aRecs    []ARecord
	tuples   []OrderTuple
	cur      int
	padding2 [PADDING]byte
}

func NewOrderAllocator() *OrderAllocator {
	oa := &OrderAllocator{}
	oa.OneAllocate()
	return oa
}

func (oa *OrderAllocator) Reset() {
	oa.cur = 0
}

func (oa *OrderAllocator) OneAllocate() {
	oa.tuples = make([]OrderTuple, ORDER_PER_ALLOC)
	oa.lRecs = nil
	oa.pRecs = nil
	oa.pRecs = nil
	oa.aRecs = nil
	if *SysType == LOCKING {
		oa.lRecs = make([]LRecord, ORDER_PER_ALLOC)
		for i := 0; i < ORDER_PER_ALLOC; i++ {
			oa.lRecs[i].SetTuple(&oa.tuples[i])
		}
	} else if *SysType == OCC {
		oa.oRecs = make([]ORecord, ORDER_PER_ALLOC)
		for i := 0; i < ORDER_PER_ALLOC; i++ {
			oa.oRecs[i].SetTuple(&oa.tuples[i])
		}
	} else if *SysType == PARTITION {
		oa.pRecs = make([]PRecord, ORDER_PER_ALLOC)
		for i := 0; i < ORDER_PER_ALLOC; i++ {
			oa.pRecs[i].SetTuple(&oa.tuples[i])
		}
	} else if *SysType == ADAPTIVE {
		oa.aRecs = make([]ARecord, ORDER_PER_ALLOC)
		for i := 0; i < ORDER_PER_ALLOC; i++ {
			oa.aRecs[i].SetTuple(&oa.tuples[i])
		}
	} else {
		clog.Info("System Type %v Not Support", *SysType)
	}
	oa.cur = 0
}

func (oa *OrderAllocator) genOrderRec() Record {
	if oa.cur == ORDER_PER_ALLOC {
		clog.Info("Order One Allocate")
		oa.OneAllocate()
	}
	var rec Record
	if *SysType == LOCKING {
		rec = &oa.lRecs[oa.cur]
	} else if *SysType == OCC {
		rec = &oa.oRecs[oa.cur]
	} else if *SysType == PARTITION {
		rec = &oa.pRecs[oa.cur]
	} else if *SysType == ADAPTIVE {
		rec = &oa.aRecs[oa.cur]
	} else {
		clog.Info("System Type %v Not Support", *SysType)
	}

	oa.cur++

	return rec
}

type OrderLineAllocator struct {
	padding1 [PADDING]byte
	lRecs    []LRecord
	oRecs    []ORecord
	pRecs    []PRecord
	aRecs    []ARecord
	tuples   []OrderLineTuple
	cur      int
	padding2 [PADDING]byte
}

func NewOrderLineAllocator() *OrderLineAllocator {
	ola := &OrderLineAllocator{}
	ola.OneAllocate()
	return ola
}

func (ola *OrderLineAllocator) Reset() {
	ola.cur = 0
}

func (ola *OrderLineAllocator) OneAllocate() {
	ola.tuples = make([]OrderLineTuple, ORDERLINE_PER_ALLOC)
	if *SysType == LOCKING {
		ola.lRecs = make([]LRecord, ORDERLINE_PER_ALLOC)
		for i := 0; i < ORDERLINE_PER_ALLOC; i++ {
			ola.lRecs[i].SetTuple(&ola.tuples[i])
		}
	} else if *SysType == OCC {
		ola.oRecs = make([]ORecord, ORDERLINE_PER_ALLOC)
		for i := 0; i < ORDERLINE_PER_ALLOC; i++ {
			ola.oRecs[i].SetTuple(&ola.tuples[i])
		}
	} else if *SysType == PARTITION {
		ola.pRecs = make([]PRecord, ORDERLINE_PER_ALLOC)
		for i := 0; i < ORDERLINE_PER_ALLOC; i++ {
			ola.pRecs[i].SetTuple(&ola.tuples[i])
		}
	} else if *SysType == ADAPTIVE {
		ola.aRecs = make([]ARecord, ORDERLINE_PER_ALLOC)
		for i := 0; i < ORDERLINE_PER_ALLOC; i++ {
			ola.aRecs[i].SetTuple(&ola.tuples[i])
		}
	} else {
		clog.Info("System Type %v Not Support", *SysType)
	}
	ola.cur = 0
}

func (ola *OrderLineAllocator) genOrderLineRec() Record {
	if ola.cur == ORDERLINE_PER_ALLOC {
		clog.Info("OrderLine One Allocate")
		ola.OneAllocate()
	}
	var rec Record
	if *SysType == LOCKING {
		rec = &ola.lRecs[ola.cur]
	} else if *SysType == OCC {
		rec = &ola.oRecs[ola.cur]
	} else if *SysType == PARTITION {
		rec = &ola.pRecs[ola.cur]
	} else if *SysType == ADAPTIVE {
		rec = &ola.aRecs[ola.cur]
	} else {
		clog.Info("System Type %v Not Support", *SysType)
	}

	ola.cur++

	return rec
}

type HistoryAllocator struct {
	padding1 [PADDING]byte
	lRecs    []LRecord
	oRecs    []ORecord
	pRecs    []PRecord
	aRecs    []ARecord
	tuples   []HistoryTuple
	cur      int
	padding2 [PADDING]byte
}

func NewHistoryAllocator() *HistoryAllocator {
	ha := &HistoryAllocator{}
	ha.OneAllocate()
	return ha
}

func (ha *HistoryAllocator) OneAllocate() {
	ha.tuples = make([]HistoryTuple, HISTORY_PER_ALLOC)
	if *SysType == LOCKING {
		ha.lRecs = make([]LRecord, HISTORY_PER_ALLOC)
		for i := 0; i < HISTORY_PER_ALLOC; i++ {
			ha.lRecs[i].SetTuple(&ha.tuples[i])
		}
	} else if *SysType == OCC {
		ha.oRecs = make([]ORecord, HISTORY_PER_ALLOC)
		for i := 0; i < HISTORY_PER_ALLOC; i++ {
			ha.oRecs[i].SetTuple(&ha.tuples[i])
		}
	} else if *SysType == PARTITION {
		ha.pRecs = make([]PRecord, HISTORY_PER_ALLOC)
		for i := 0; i < HISTORY_PER_ALLOC; i++ {
			ha.pRecs[i].SetTuple(&ha.tuples[i])
		}
	} else if *SysType == ADAPTIVE {
		ha.aRecs = make([]ARecord, HISTORY_PER_ALLOC)
		for i := 0; i < HISTORY_PER_ALLOC; i++ {
			ha.aRecs[i].SetTuple(&ha.tuples[i])
		}
	} else {
		clog.Info("System Type %v Not Support", *SysType)
	}
	ha.cur = 0
}

func (ha *HistoryAllocator) genHistoryRec() Record {
	if ha.cur == HISTORY_PER_ALLOC {
		clog.Info("History One Allocate")
		ha.OneAllocate()
	}
	var rec Record
	if *SysType == LOCKING {
		rec = &ha.lRecs[ha.cur]
	} else if *SysType == OCC {
		rec = &ha.oRecs[ha.cur]
	} else if *SysType == PARTITION {
		rec = &ha.pRecs[ha.cur]
	} else if *SysType == ADAPTIVE {
		rec = &ha.aRecs[ha.cur]
	} else {
		clog.Info("System Type %v Not Support", *SysType)
	}

	ha.cur++

	return rec
}

func (ha *HistoryAllocator) Reset() {
	ha.cur = 0
}

type IndexAlloc interface {
	OneAllocate()
	OneAllocateSize(size int)
	GetEntry() Value
	GetSecEntry() Value
	Reset()
}

type OrderIndexAlloc struct {
	padding1    [PADDING]byte
	bucketEntry []OrderBucketEntry
	secEntry    []OrderSecEntry
	bucketCur   int
	secCur      int
	padding2    [PADDING]byte
}

func (oia *OrderIndexAlloc) OneAllocate() {
	oia.bucketEntry = make([]OrderBucketEntry, ORDER_INDEX_PER_ALLOC)
	oia.secEntry = make([]OrderSecEntry, ORDER_SECINDEX_PER_ALLOC)
	oia.secCur = 0
	oia.bucketCur = 0
}

func (oia *OrderIndexAlloc) OneAllocateSize(size int) {
	oia.bucketEntry = make([]OrderBucketEntry, size)
	oia.secEntry = make([]OrderSecEntry, size)
	oia.secCur = 0
	oia.bucketCur = 0
}

func (oia *OrderIndexAlloc) GetEntry() Value {
	if oia.bucketCur == len(oia.bucketEntry) {
		clog.Info("Order Index One Allocate")
		oia.bucketEntry = make([]OrderBucketEntry, oia.bucketCur)
		oia.bucketCur = 0
	}
	entry := &oia.bucketEntry[oia.bucketCur]
	entry.next = nil
	entry.before = nil
	entry.t = 0
	oia.bucketCur++
	return entry
}

func (oia *OrderIndexAlloc) GetSecEntry() Value {
	if oia.secCur == len(oia.secEntry) {
		clog.Info("Order SecIndex One Allocate")
		oia.secEntry = make([]OrderSecEntry, oia.secCur)
		oia.secCur = 0
	}
	entry := &oia.secEntry[oia.secCur]
	entry.next = nil
	entry.before = nil
	entry.t = 0
	oia.secCur++
	return entry
}

func (oia *OrderIndexAlloc) Reset() {
	oia.bucketCur = 0
	oia.secCur = 0
}

type OrderLineIndexAlloc struct {
	padding1    [PADDING]byte
	bucketEntry []OrderLineBucketEntry
	bucketCur   int
	padding2    [PADDING]byte
}

func (ol *OrderLineIndexAlloc) OneAllocate() {
	ol.bucketEntry = make([]OrderLineBucketEntry, ORDERLINE_INDEX_PER_ALLOC)
	ol.bucketCur = 0
}

func (ol *OrderLineIndexAlloc) OneAllocateSize(size int) {
	ol.bucketEntry = make([]OrderLineBucketEntry, size)
	ol.bucketCur = 0
}

func (ol *OrderLineIndexAlloc) GetEntry() Value {
	if ol.bucketCur == len(ol.bucketEntry) {
		clog.Info("OrderLine Index One Allocate")
		ol.OneAllocateSize(ol.bucketCur)
	}
	entry := &ol.bucketEntry[ol.bucketCur]
	entry.t = 0
	entry.before = nil
	entry.next = nil
	ol.bucketCur++
	return entry
}

func (ol *OrderLineIndexAlloc) GetSecEntry() Value {
	return nil
}

func (ol *OrderLineIndexAlloc) Reset() {
	ol.bucketCur = 0
}

type HistoryIndexAlloc struct {
	padding1 [PADDING]byte
	entry    []HistoryEntry
	cur      int
	padding2 [PADDING]byte
}

func (h *HistoryIndexAlloc) OneAllocate() {
	h.entry = make([]HistoryEntry, HISTORY_INDEX_PER_ALLOC)
	h.cur = 0
}

func (h *HistoryIndexAlloc) OneAllocateSize(size int) {
	h.entry = make([]HistoryEntry, size)
	h.cur = 0
}

func (h *HistoryIndexAlloc) GetEntry() Value {
	if h.cur == len(h.entry) {
		clog.Info("History Index One Allocate")
		h.OneAllocateSize(h.cur)
	}
	entry := &h.entry[h.cur]
	entry.index = 0
	entry.next = nil
	h.cur++
	return entry
}

func (h *HistoryIndexAlloc) GetSecEntry() Value {
	return nil
}

func (h *HistoryIndexAlloc) Reset() {
	h.cur = 0
}

type NewOrderIndexAlloc struct {
	padding1 [PADDING]byte
	entry    []NoEntry
	cur      int
	padding2 [PADDING]byte
}

func (no *NewOrderIndexAlloc) OneAllocate() {
	no.entry = make([]NoEntry, NEWORDER_INDEX_PER_ALLOC)
	for i := 0; i < NEWORDER_INDEX_PER_ALLOC; i++ {
		dRec := &DRecord{}
		dRec.tuple = &NewOrderTuple{}
		no.entry[i].rec = dRec
	}
	no.cur = 0
}

func (no *NewOrderIndexAlloc) OneAllocateSize(size int) {
	no.entry = make([]NoEntry, size)
	for i := 0; i < size; i++ {
		dRec := &DRecord{}
		dRec.tuple = &NewOrderTuple{}
		no.entry[i].rec = dRec
	}
	no.cur = 0
}

func (no *NewOrderIndexAlloc) GetEntry() Value {
	if no.cur == len(no.entry) {
		clog.Info("NewOrder Index One Allocate")
		no.OneAllocateSize(no.cur)
	}
	entry := &no.entry[no.cur]
	entry.t = 0
	entry.h = 0
	entry.next = nil
	no.cur++
	return entry
}

func (no *NewOrderIndexAlloc) GetSecEntry() Value {
	return nil
}

func (no *NewOrderIndexAlloc) Reset() {
	no.cur = 0
}
