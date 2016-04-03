package testbed

import (
	"runtime/debug"
	"time"

	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/spinlockopt"
	"github.com/totemtang/cc-testbed/wdlock"
	"github.com/totemtang/cc-testbed/wfmutex"
)

const (
	WDTRIAL = 0
)

type Tuple interface {
	GetValue(val Value, col int)
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

type DateValue struct {
	padding1 [PADDING]byte
	dateVal  time.Duration
	padding2 [PADDING]byte
}

type Record interface {
	Lock() (bool, TID)
	Unlock(tid TID)
	IsUnlocked() (bool, TID)
	GetValue(val Value, colNum int)
	GetKey() Key
	SetValue(val Value, colNum int)
	GetTID() TID
	SetTID(tid TID)
	WLock(tid TID) bool
	WUnlock()
	RLock(tid TID) bool
	RUnlock()
	Upgrade(tid TID) bool
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
			table: table,
			key:   k,
			tuple: tuple,
		}
		if *NoWait {
			lr.rwLock.SetTrial(WDTRIAL)
		} else {
			lr.wd.Initialize()
		}

		return lr
	} else if *SysType == ADAPTIVE {
		ar := &ARecord{
			table: table,
			key:   k,
			tuple: tuple,
			last:  wfmutex.WFMutex{},
		}
		if *NoWait {
			ar.rwLock.SetTrial(WDTRIAL)
		} else {
			ar.wd.Initialize()
		}
		return ar
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

func (pr *PRecord) GetValue(val Value, colNum int) {
	pr.tuple.GetValue(val, colNum)
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

func (pr *PRecord) WLock(tid TID) bool {
	clog.Error("Partition mode does not support WLock Operation")
	return false
}

func (pr *PRecord) WUnlock() {
	clog.Error("Partition mode does not support WUnlock Operation")
}

func (pr *PRecord) RLock(tid TID) bool {
	clog.Error("Partition mode does not support RLock Operation")
	return false
}

func (pr *PRecord) RUnlock() {
	clog.Error("Partition mode does not support RUnlock Operation")
}

func (pr *PRecord) Upgrade(tid TID) bool {
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

func (or *ORecord) GetValue(val Value, colNum int) {
	or.tuple.GetValue(val, colNum)
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

func (or *ORecord) WLock(tid TID) bool {
	clog.Error("OCC mode does not support WLock Operation")
	return false
}

func (or *ORecord) WUnlock() {
	clog.Error("OCC mode does not support WUnlock Operation")
}

func (or *ORecord) RLock(tid TID) bool {
	debug.PrintStack()
	clog.Error("OCC mode does not support RLock Operation")
	return false
}

func (or *ORecord) RUnlock() {
	clog.Error("OCC mode does not support RUnlock Operation")
}

func (or *ORecord) Upgrade(tid TID) bool {
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

func (dr *DRecord) GetValue(val Value, colNum int) {
	//return dr.value
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

func (dr *DRecord) WLock(tid TID) bool {
	clog.Error("Dummy mode does not support WLock Operation")
	return false
}

func (dr *DRecord) WUnlock() {
	clog.Error("Dummy mode does not support WUnlock Operation")
}

func (dr *DRecord) RLock(tid TID) bool {
	clog.Error("Dummy mode does not support RLock Operation")
	return false
}

func (dr *DRecord) RUnlock() {
	clog.Error("Dummy mode does not support RUnlock Operation")
}

func (dr *DRecord) Upgrade(tid TID) bool {
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
	wd       wdlock.WDLock
	rwLock   spinlockopt.WDRWSpinlock
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

func (lr *LRecord) GetValue(val Value, colNum int) {
	lr.tuple.GetValue(val, colNum)
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

func (lr *LRecord) WLock(tid TID) bool {
	if *NoWait {
		return lr.rwLock.Lock(0)
	} else {
		return lr.wd.Lock(uint64(tid))
	}
	//return true
}

func (lr *LRecord) WUnlock() {
	if *NoWait {
		lr.rwLock.Unlock()
	} else {
		lr.wd.Unlock()
	}
}

func (lr *LRecord) RLock(tid TID) bool {
	if *NoWait {
		return lr.rwLock.RLock(0)
	} else {
		return lr.wd.RLock(uint64(tid))
	}
}

func (lr *LRecord) RUnlock() {
	if *NoWait {
		lr.rwLock.RUnlock()
	} else {
		lr.wd.RUnlock()
	}
	//return
}

func (lr *LRecord) Upgrade(tid TID) bool {
	if *NoWait {
		return lr.rwLock.Upgrade(0)
	} else {
		return lr.wd.Upgrade(uint64(tid))
	}
}

type ARecord struct {
	padding1 [PADDING]byte
	key      Key
	tuple    Tuple
	wd       wdlock.WDLock
	rwLock   spinlockopt.WDRWSpinlock
	last     wfmutex.WFMutex
	table    *Table
	conflict spinlockopt.WDRWSpinlock
	padding2 [PADDING]byte
}

func (ar *ARecord) Lock() (bool, TID) {
	b, x := ar.last.Lock()
	return b, TID(x)
}

func (ar *ARecord) Unlock(tid TID) {
	ar.last.Unlock(uint64(tid))
}

func (ar *ARecord) IsUnlocked() (bool, TID) {
	x := ar.last.Read()
	if x&wfmutex.LOCKED != 0 {
		return false, TID(x & wfmutex.TIDMASK)
	}
	return true, TID(x)
}

func (ar *ARecord) GetValue(val Value, colNum int) {
	ar.tuple.GetValue(val, colNum)
}

func (ar *ARecord) GetKey() Key {
	return ar.key
}

func (ar *ARecord) SetValue(val Value, colNum int) {
	ar.tuple.SetValue(val, colNum)
}

func (ar *ARecord) GetTID() TID {
	return TID(ar.last.Read())
}
func (ar *ARecord) SetTID(tid TID) {
	clog.Error("Adaptive mode does not support SetTID Operation")
}

func (ar *ARecord) WLock(tid TID) bool {
	if *NoWait {
		return ar.rwLock.Lock(0)
	} else {
		return ar.wd.Lock(uint64(tid))
	}
	//return true
}

func (ar *ARecord) WUnlock() {
	if *NoWait {
		ar.rwLock.Unlock()
	} else {
		ar.wd.Unlock()
	}
}

func (ar *ARecord) RLock(tid TID) bool {
	if *NoWait {
		return ar.rwLock.RLock(0)
	} else {
		return ar.wd.RLock(uint64(tid))
	}
}

func (ar *ARecord) RUnlock() {
	if *NoWait {
		ar.rwLock.RUnlock()
	} else {
		ar.wd.RUnlock()
	}
}

func (ar *ARecord) Upgrade(tid TID) bool {
	if *NoWait {
		return ar.rwLock.Upgrade(0)
	} else {
		return ar.wd.Upgrade(uint64(tid))
	}
}
