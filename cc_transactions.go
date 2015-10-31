package testbed

import (
	"math/rand"
)

type WValue interface{}

const (
	ADD_ONE = iota
	RANDOM_UPDATE_INT
	RANDOM_UPDATE_STRING

	LAST_TXN
)

type RetIntValue struct {
	intVals []int64
}

type RetStringValue struct {
	strVals [][]string
}

type SingleIntValue struct {
	intVals []int64
}

type StringListValue struct {
	strVals []*StrAttr
}

type Result struct {
	V Value
}

type Query struct {
	TXN         int // The transaction to be executed
	T           TID
	txnLen      int
	isPartition bool
	partitioner Partitioner
	accessParts []int
	rKeys       []Key
	wKeys       []Key
	wValue      WValue
}

func (q *Query) GenValue(rnd *rand.Rand) {

	if q.TXN == RANDOM_UPDATE_INT {
		v := &SingleIntValue{
			intVals: make([]int64, len(q.wKeys)),
		}

		for i := range v.intVals {
			v.intVals[i] = rnd.Int63()
		}

		q.wValue = v
	} else if q.TXN == RANDOM_UPDATE_STRING {
		v := &StringListValue{
			strVals: make([]*StrAttr, len(q.wKeys)),
		}

		for i := range v.strVals {
			v.strVals[i] = &StrAttr{
				index: rnd.Intn(FIELDS),
				value: Randstr(int(PERFIELD)),
			}
		}

		q.wValue = v
	}
}

func AddOneTXN(q *Query, tx ETransaction) (*Result, error) {
	// Apply Writes
	for _, wk := range q.wKeys {
		partNum := q.partitioner.GetPartition(wk)
		v, err := tx.Read(wk, partNum)
		if err != nil {
			return nil, err
		}

		newVal := (v.Value().(int64)) + 1

		err = tx.WriteInt64(wk, newVal, partNum)
		if err != nil {
			return nil, err
		}
	}

	// Read Results
	var r Result
	rValue := &RetIntValue{
		intVals: make([]int64, len(q.rKeys)),
	}
	for i, rk := range q.rKeys {
		partNum := q.partitioner.GetPartition(rk)
		v, err := tx.Read(rk, partNum)
		if err != nil {
			return nil, err
		}
		rValue.intVals[i] = v.Value().(int64)
	}

	r.V = rValue
	return &r, nil
}

func UpdateIntTXN(q *Query, tx ETransaction) (*Result, error) {
	// Apply Writes
	updateVals := q.wValue.(*SingleIntValue)
	for i, wk := range q.wKeys {
		partNum := q.partitioner.GetPartition(wk)
		err := tx.WriteInt64(wk, updateVals.intVals[i], partNum)
		if err != nil {
			return nil, err
		}
	}

	// Read Results
	var r Result
	rValue := &RetIntValue{
		intVals: make([]int64, len(q.rKeys)),
	}
	for i, rk := range q.rKeys {
		partNum := q.partitioner.GetPartition(rk)
		v, err := tx.Read(rk, partNum)
		if err != nil {
			return nil, err
		}
		rValue.intVals[i] = v.Value().(int64)
	}

	r.V = rValue
	return &r, nil

}

func UpdateStringTXN(q *Query, tx ETransaction) (*Result, error) {
	// Apply Writes
	updateVals := q.wValue.(*StringListValue)
	for i, wk := range q.wKeys {
		partNum := q.partitioner.GetPartition(wk)
		err := tx.WriteString(wk, updateVals.strVals[i], partNum)
		if err != nil {
			return nil, err
		}
	}

	// Read Results
	var r Result
	rValue := &RetStringValue{
		strVals: make([][]string, len(q.rKeys)),
	}
	for i, rk := range q.rKeys {
		partNum := q.partitioner.GetPartition(rk)
		v, err := tx.Read(rk, partNum)
		if err != nil {
			return nil, err
		}
		rValue.strVals[i] = v.Value().([]string)
	}

	r.V = rValue
	return &r, nil
}
