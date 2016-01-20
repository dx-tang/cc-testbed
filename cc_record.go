package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/wdspinlock"
	"github.com/totemtang/cc-testbed/wfmutex"
)

const (
	WDTRIAL = 10
)

type Tuple interface {
	GetValue(col int) Value
	SetValue(val Value, col int)
}

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
	WLock() bool
	WUnlock()
	RLock() bool
	RUnlock()
	Upgrade() bool
}

/*
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
}*/

func MakeRecord(table *Table, k Key, tuple Tuple) Record {

	if *SysType == PARTITION {
		pr := &PRecord{
			table: table,
			key:   k,
			tuple: tuple,
		}

		return pr
	} else if *SysType == OCC {
		or := &ORecord{
			table: table,
			key:   k,
			tuple: tuple,
			last:  wfmutex.WFMutex{},
		}

		return or
	} else if *SysType == LOCKING {
		lr := &LRecord{
			table:  table,
			key:    k,
			tuple:  tuple,
			rwLock: wdspinlock.WDRWSpinlock{},
		}
		lr.rwLock.SetTrial(WDTRIAL)

		return lr
	} else {
		clog.Error("System Type %v Not Supported Yet", *SysType)
		return nil
	}
}

type PRecord struct {
	padding1 [PADDING]byte
	key      Key
	tuple    Tuple
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
	return pr.tuple.GetValue(colNum)
}

func (pr *PRecord) SetValue(val Value, colNum int) {
	//pr.value[colNum] = val
	//bt := pr.table.valueSchema[colNum]
	//setVal(bt, pr.value[colNum], val)
	pr.tuple.SetValue(val, colNum)
}

func (pr *PRecord) GetTID() TID {
	clog.Error("Partition mode does not support GetTID Operation")
	return 0
}

func (pr *PRecord) SetTID(tid TID) {
	clog.Error("Partition mode does not support SetTID Operation")
}

func (pr *PRecord) WLock() bool {
	clog.Error("Partition mode does not support WLock Operation")
	return false
}

func (pr *PRecord) WUnlock() {
	clog.Error("Partition mode does not support WUnlock Operation")
}

func (pr *PRecord) RLock() bool {
	clog.Error("Partition mode does not support RLock Operation")
	return false
}

func (pr *PRecord) RUnlock() {
	clog.Error("Partition mode does not support RUnlock Operation")
}

func (pr *PRecord) Upgrade() bool {
	clog.Error("Partition mode does not support Upgrade Operation")
	return false
}

type ORecord struct {
	padding1 [PADDING]byte
	key      Key
	tuple    Tuple
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
	return or.tuple.GetValue(colNum)
}

func (or *ORecord) GetKey() Key {
	return or.key
}

func (or *ORecord) SetValue(val Value, colNum int) {
	//bt := or.table.valueSchema[colNum]
	//setVal(bt, or.value[colNum], val)
	or.tuple.SetValue(val, colNum)
}

func (or *ORecord) GetTID() TID {
	return TID(or.last.Read())
}

func (or *ORecord) SetTID(tid TID) {
	clog.Error("OCC mode does not support SetTID Operation")
}

func (or *ORecord) WLock() bool {
	clog.Error("OCC mode does not support WLock Operation")
	return false
}

func (or *ORecord) WUnlock() {
	clog.Error("OCC mode does not support WUnlock Operation")
}

func (or *ORecord) RLock() bool {
	clog.Error("OCC mode does not support RLock Operation")
	return false
}

func (or *ORecord) RUnlock() {
	clog.Error("OCC mode does not support RUnlock Operation")
}

func (or *ORecord) Upgrade() bool {
	clog.Error("OCC mode does not support Upgrade Operation")
	return false
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

func (dr *DRecord) WLock() bool {
	clog.Error("Dummy mode does not support WLock Operation")
	return false
}

func (dr *DRecord) WUnlock() {
	clog.Error("Dummy mode does not support WUnlock Operation")
}

func (dr *DRecord) RLock() bool {
	clog.Error("Dummy mode does not support RLock Operation")
	return false
}

func (dr *DRecord) RUnlock() {
	clog.Error("Dummy mode does not support RUnlock Operation")
}

func (dr *DRecord) Upgrade() bool {
	clog.Error("Dummy mode does not support Upgrade Operation")
	return false
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

type LRecord struct {
	padding1 [PADDING]byte
	key      Key
	tuple    Tuple
	rwLock   wdspinlock.WDRWSpinlock
	table    *Table
	padding2 [PADDING]byte
}

func (lr *LRecord) Lock() (bool, TID) {
	clog.Error("Lock mode does not support Lock Operation")
	return false, TID(0)
}

func (lr *LRecord) Unlock(tid TID) {
	clog.Error("Lock mode does not support Unlock Operation")
}

func (lr *LRecord) IsUnlocked() (bool, TID) {
	clog.Error("Lock mode does not support IsUnlocked Operation")
	return false, TID(0)
}

func (lr *LRecord) GetValue(colNum int) Value {
	return lr.tuple.GetValue(colNum)
}

func (lr *LRecord) GetKey() Key {
	return lr.key
}

func (lr *LRecord) SetValue(val Value, colNum int) {
	lr.tuple.SetValue(val, colNum)
}

func (lr *LRecord) GetTID() TID {
	clog.Error("Lock mode does not support GetTID Operation")
	return TID(0)
}

func (lr *LRecord) SetTID(tid TID) {
	clog.Error("Lock mode does not support SetTID Operation")
}

func (lr *LRecord) WLock() bool {
	return lr.rwLock.Lock()
	//return true
}

func (lr *LRecord) WUnlock() {
	lr.rwLock.Unlock()
}

func (lr *LRecord) RLock() bool {
	return lr.rwLock.RLock()
	//return true
}

func (lr *LRecord) RUnlock() {
	lr.rwLock.RUnlock()
	//return
}

func (lr *LRecord) Upgrade() bool {
	return lr.rwLock.Upgrade()
	//return true
}
