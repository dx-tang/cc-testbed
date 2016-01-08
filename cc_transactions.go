package testbed

import (
	"math/rand"

	//"github.com/totemtang/cc-testbed/clog"
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
	padding1    [64]byte
	TXN         int // The transaction to be executed
	T           TID
	txnLen      int
	isPartition bool
	partitioner *HashPartitioner
	accessParts []int
	rKeys       []Key
	wKeys       []Key
	wValue      WValue
	padding2    [64]byte
}

func (q *Query) DoNothing() {

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
				//value: Randstr(int(PERFIELD)),
			}
		}

		q.wValue = v
	}
}

func AddOneTXN(q *Query, tx ETransaction) (*Result, error) {
	var partNum int
	// Apply Writes
	for _, wk := range q.wKeys {
		if q.partitioner != nil {
			partNum = q.partitioner.GetPartition(wk)
		}
		v, err := tx.Read(wk, partNum, false)
		if err != nil {
			return nil, err
		}

		newVal := *(v.Value().(*int64)) + 1
		//*(v.Value().(*int64))++

		err = tx.WriteInt64(wk, newVal, partNum)
		if err != nil {
			return nil, err
		}
	}

	// Read Results
	//var r Result
	//rValue := &RetIntValue{
	//	intVals: make([]int64, len(q.rKeys)),
	//}
	//for i, rk := range q.rKeys {
	//for _, rk := range q.rKeys {
	var val Value
	for i := 0; i < len(q.rKeys); i++ {
		if q.partitioner != nil {
			partNum = q.partitioner.GetPartition(q.rKeys[i])
		}

		v, err := tx.Read(q.rKeys[i], partNum, false)
		if err != nil {
			return nil, err
		}

		//rValue.intVals[i] = v.Value().(int64)
		val = v.Value()
		intVal := *(val.(*int64))
		intVal++
	}

	if tx.Commit() == 0 {
		return nil, EABORT
	}

	if val == nil {
		return nil, nil
	}

	//return &r, nil
	return nil, nil
}

func UpdateIntTXN(q *Query, tx ETransaction) (*Result, error) {
	var partNum int
	// Apply Writes
	updateVals := q.wValue.(*SingleIntValue)
	for i, wk := range q.wKeys {
		if q.partitioner != nil {
			partNum = q.partitioner.GetPartition(wk)
		}
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
		if q.partitioner != nil {
			partNum = q.partitioner.GetPartition(rk)
		}
		v, err := tx.Read(rk, partNum, false)
		if err != nil {
			return nil, err
		}
		rValue.intVals[i] = v.Value().(int64)
	}

	r.V = rValue

	if tx.Commit() == 0 {
		return nil, EABORT
	}

	return &r, nil

}

func UpdateStringTXN(q *Query, tx ETransaction) (*Result, error) {
	var partNum int
	// Apply Writes
	updateVals := q.wValue.(*StringListValue)
	for i, wk := range q.wKeys {
		if q.partitioner != nil {
			partNum = q.partitioner.GetPartition(wk)
		}
		updateVals.strVals[i].value = Randstr(int(PERFIELD))
		err := tx.WriteString(wk, updateVals.strVals[i], partNum)
		if err != nil {
			return nil, err
		}
	}

	// Read Results
	var r Result
	var ok bool
	rValue := &RetStringValue{
		strVals: make([][]string, len(q.rKeys)),
	}
	for i, rk := range q.rKeys {
		if q.partitioner != nil {
			partNum = q.partitioner.GetPartition(rk)
		}
		v, err := tx.Read(rk, partNum, false)
		if err != nil {
			return nil, err
		}

		if rValue.strVals[i], ok = v.Value().([]string); !ok {
			tmpVal := v.Value().(*StrAttr)
			v, err = tx.Read(rk, partNum, true)
			if err != nil {
				return nil, err
			}
			rValue.strVals[i] = v.Value().([]string)
			rValue.strVals[i][tmpVal.index] = tmpVal.value
		}
	}

	r.V = rValue

	if tx.Commit() == 0 {
		return nil, EABORT
	}

	return &r, nil
}
