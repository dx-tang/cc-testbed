package testbed

import (
	"math"
)

type WValue interface{}

const (
	RANDOM_UPDATE_INT = iota
	ADD_ONE
	RANDOM_UPDATE_STRING
)

type SingleIntValue struct {
	intVals []int32
}

type StringListValue struct {
	strVals []*StrAttr
}

type Query struct {
	TXN         int // The transaction to be executed
	T           TID
	txnLen      int
	isPartition bool
	accessParts []int
	rKeys       []Key
	wKeys       []Key
	wValue      WValue
}

func (q *Query) GenValue(seed uint32) {
	if q.TXN == RANDOM_UPDATE_INT {
		v := &SingleIntValue{
			intVals: make([]int32, len(q.wKeys)),
		}

		for i := range v.intVals {
			v.intVals[i] = int32(RandN(&seed, uint32(math.MaxInt32)))
		}

		q.wValue = v
	} else if q.TXN == RANDOM_UPDATE_STRING {
		v := &StringListValue{
			strVals: make([]*StrAttr, len(q.wKeys)),
		}

		var sz uint32
		for i := range v.strVals {
			sz = RandN(&seed, uint32(PERFIELD)) + 1
			v.strVals[i] = &StrAttr{
				index: int(RandN(&seed, uint32(FIELDS-1))),
				value: Randstr(int(sz)),
			}
		}

		q.wValue = v
	}
}
