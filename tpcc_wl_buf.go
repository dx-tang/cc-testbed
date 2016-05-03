package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
)

const (
	ORDER_PER_ALLOC     = 30000
	ORDERLINE_PER_ALLOC = 300000
	HISTORY_PER_ALLOC   = 10000
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

func (oa *OrderAllocator) OneAllocate() {
	oa.tuples = make([]OrderTuple, ORDER_PER_ALLOC)
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
