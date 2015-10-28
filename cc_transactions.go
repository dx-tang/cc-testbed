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
	rwSets      *RWSets
}

func (q *Query) GenValue(rnd *rand.Rand) {

	if q.TXN == RANDOM_UPDATE_INT {
		v := &SingleIntValue{
			intVals: make([]int64, len(q.wKeys)),
		}

		for i := range v.intVals {
			//v.intVals[i] = int32(RandN(&seed, uint32(math.MaxInt32)))
			v.intVals[i] = rnd.Int63()
		}

		q.wValue = v
	} else if q.TXN == RANDOM_UPDATE_STRING {
		v := &StringListValue{
			strVals: make([]*StrAttr, len(q.wKeys)),
		}

		for i := range v.strVals {
			//sz = RandN(&seed, uint32(PERFIELD)) + 1
			v.strVals[i] = &StrAttr{
				//index: int(RandN(&seed, uint32(FIELDS-1))),
				index: rnd.Intn(FIELDS),
				value: Randstr(int(PERFIELD)),
			}
		}

		q.wValue = v
	}
}

func AddOneTXN(q *Query, tx ETransaction) (*Result, error) {
	//rwSets := q.rwSets
	//wValue := &RetIntValue{
	//	intVals: make([]int64, len(q.wKeys)),
	//}

	//var rIndex int

	// Apply Writes
	for _, wk := range q.wKeys {
		partNum := q.partitioner.GetPartition(wk)
		v, err := tx.Read(wk, partNum)
		if err != nil {
			return nil, err
		}

		newVal := v.intVal + 1

		//rwSets.wKeys[i] = wk
		//wValue.intVals[i] = newVal

		//rwSets.rKeys[rIndex] = wk
		//rwSets.rTIDs[rIndex] = v.GetTID()
		//rIndex++

		err = tx.WriteInt64(wk, newVal, partNum)
		if err != nil {
			return nil, err
		}
	}

	//rwSets.wValue = wValue

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
		rValue.intVals[i] = v.intVal

		//rwSets.rKeys[rIndex] = rk
		//rwSets.rTIDs[rIndex] = v.GetTID()
		//rIndex++
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
		rValue.intVals[i] = v.intVal
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
		rValue.strVals[i] = v.stringVal
	}

	r.V = rValue
	return &r, nil
}
