package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/wfmutex"
)

const (
	STRMAXLEN = 100
)

type Value interface{}

type IntValue struct {
	padding1 [PADDING]byte
	intVal   int64
	padding2 [PADDING]byte
}

type FloatValue struct {
	padding1 [PADDING]byte
	floatVal float64
	padding2 [PADDING]byte
}

type StringValue struct {
	stringVal []byte
}

type Record interface {
	Lock() (bool, TID)
	Unlock(tid TID)
	IsUnlocked() (bool, TID)
	GetValue(colNum int) Value
	GetKey() Key
	SetValue(val Value, colNum int)
	GetTID() TID
	SetTID(tid TID)
}

func allocStrVal() *StringValue {
	sv := &StringValue{
		stringVal: make([]byte, 0, STRMAXLEN+2*PADDINGBYTE),
	}
	sv.stringVal = sv.stringVal[PADDINGBYTE:PADDINGBYTE]
	return sv
}

func allocAttr(valType BTYPE, val Value) Value {
	switch valType {
	case INTEGER:
		ret := &IntValue{
			intVal: val.(*IntValue).intVal,
		}
		return ret
	case FLOAT:
		ret := &FloatValue{
			floatVal: val.(*FloatValue).floatVal,
		}
		return ret
	case STRING:
		input := val.(*StringValue).stringVal
		ret := allocStrVal()
		ret.stringVal = ret.stringVal[0:len(input)]
		for i := 0; i < len(input); i++ {
			ret.stringVal[i] = input[i]
		}
		return ret
	default:
		clog.Error("Value Type Not Supported")
		return nil
	}
}

func MakeRecord(table *Table, k Key, v []Value) Record {

	if *SysType == PARTITION {
		pr := &PRecord{
			table: table,
			key:   k,
			value: make([]Value, len(v)),
		}

		for i := 0; i < len(v); i++ {
			pr.value[i] = allocAttr(table.valueSchema[i], v[i])
		}

		return pr
	} else if *SysType == OCC {
		or := &ORecord{
			table: table,
			key:   k,
			value: make([]Value, len(v)),
			last:  wfmutex.WFMutex{},
		}

		for i := 0; i < len(v); i++ {
			or.value[i] = allocAttr(table.valueSchema[i], v[i])
		}

		return or
	} else {
		clog.Error("System Type %v Not Supported Yet", *SysType)
		return nil
	}
}

type PRecord struct {
	padding1 [PADDING]byte
	key      Key
	value    []Value
	table    *Table
	padding2 [PADDING]byte
}

func (pr *PRecord) GetKey() Key {
	return pr.key
}

func (pr *PRecord) Lock() (bool, TID) {
	clog.Error("Partition mode does not support Lock Operation")
	return false, 0
}

func (pr *PRecord) Unlock(tid TID) {
	clog.Error("Partition mode does not support Unlock Operation")
}

func (pr *PRecord) IsUnlocked() (bool, TID) {
	clog.Error("Partition mode does not support IsUnlocked Operation")
	return false, 0
}

func (pr *PRecord) GetValue(colNum int) Value {
	return pr.value[colNum]
}

func (pr *PRecord) SetValue(val Value, colNum int) {
	//pr.value[colNum] = val
	bt := pr.table.valueSchema[colNum]
	setVal(bt, pr.value[colNum], val)
}

func (pr *PRecord) GetTID() TID {
	clog.Error("Partition mode does not support GetTID Operation")
	return 0
}

func (pr *PRecord) SetTID(tid TID) {
	clog.Error("Partition mode does not support SetTID Operation")
}

type ORecord struct {
	padding1 [PADDING]byte
	key      Key
	value    []Value
	last     wfmutex.WFMutex
	table    *Table
	padding2 [PADDING]byte
}

func (or *ORecord) Lock() (bool, TID) {
	b, x := or.last.Lock()
	return b, TID(x)
}

func (or *ORecord) Unlock(tid TID) {
	or.last.Unlock(uint64(tid))
}

func (or *ORecord) IsUnlocked() (bool, TID) {
	x := or.last.Read()
	if x&wfmutex.LOCKED != 0 {
		return false, TID(x & wfmutex.TIDMASK)
	}
	return true, TID(x)
}

func (or *ORecord) GetValue(colNum int) Value {
	return or.value[colNum]
}

func (or *ORecord) GetKey() Key {
	return or.key
}

func (or *ORecord) SetValue(val Value, colNum int) {
	bt := or.table.valueSchema[colNum]
	setVal(bt, or.value[colNum], val)
}

func (or *ORecord) GetTID() TID {
	return TID(or.last.Read())
}

func (or *ORecord) SetTID(tid TID) {
	clog.Error("OCC mode does not support SetTID Operation")
}

// Dummy Record
type DRecord struct {
	padding1 [PADDING]byte
	key      Key
	value    Value
	colNum   int
	padding2 [PADDING]byte
}

func (dr *DRecord) GetKey() Key {
	clog.Error("Dummy Record does not support GetKey Operation")
	return dr.key
}

func (dr *DRecord) Lock() (bool, TID) {
	clog.Error("Dummy Record does not support Lock Operation")
	return false, 0
}

func (dr *DRecord) Unlock(tid TID) {
	clog.Error("Dummy Record does not support Unlock Operation")
}

func (dr *DRecord) IsUnlocked() (bool, TID) {
	clog.Error("Dummy Record does not support IsUnlocked Operation")
	return false, 0
}

func (dr *DRecord) GetValue(colNum int) Value {
	return dr.value
}

func (dr *DRecord) SetValue(val Value, colNum int) {
	dr.value = val
	dr.colNum = colNum
}

func (dr *DRecord) GetTID() TID {
	clog.Error("Dummy Record does not support GetTID Operation")
	return 0
}

func (dr *DRecord) SetTID(tid TID) {
	clog.Error("Dummy Record does not support SetTID Operation")
}

func setVal(bt BTYPE, oldVal Value, newVal Value) {
	switch bt {
	case INTEGER:
		old := oldVal.(*IntValue)
		old.intVal = newVal.(*IntValue).intVal
	case FLOAT:
		old := oldVal.(*FloatValue)
		old.floatVal = newVal.(*FloatValue).floatVal
	case STRING:
		old := oldVal.(*StringValue)
		newOne := newVal.(*StringValue)
		old.stringVal = old.stringVal[:len(newOne.stringVal)]
		for i, b := range newOne.stringVal {
			old.stringVal[i] = b
		}
	default:
		clog.Error("Set Value Error; Not Support Value Type\n")
	}
}
