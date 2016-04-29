package testbed

import (
	"runtime/debug"
	"time"

	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/nowaitlock"
	"github.com/totemtang/cc-testbed/spinlock"
	"github.com/totemtang/cc-testbed/wfmutex"
)

type Tuple interface {
	GetValue(val Value, col int)
	SetValue(val Value, col int)
	DeltaValue(val Value, col int)
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
	dateVal  time.Time
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
	WLock(req *LockReq) bool
	WUnlock(req *LockReq)
	RLock(req *LockReq) bool
	RUnlock(req *LockReq)
	Upgrade(req *LockReq) bool
	GetTuple() Tuple
	SetTuple(t Tuple)
	DeltaValue(val Value, column int)
}

func MakeRecord(table Table, k Key, tuple Tuple) Record {

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
		}

		return or
	} else if *SysType == LOCKING {
		lr := &LRecord{
			table: table,
			key:   k,
			tuple: tuple,
		}

		lr.wdLock.Initialize()

		return lr
	} else if *SysType == ADAPTIVE {
		ar := &ARecord{
			table: table,
			key:   k,
			tuple: tuple,
		}

		ar.wdLock.Initialize()

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
	table    Table
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
	pr.tuple.SetValue(val, colNum)
}

func (pr *PRecord) GetTID() TID {
	clog.Error("Partition mode does not support GetTID Operation")
	return 0
}

func (pr *PRecord) SetTID(tid TID) {
	clog.Error("Partition mode does not support SetTID Operation")
}

func (pr *PRecord) WLock(req *LockReq) bool {
	clog.Error("Partition mode does not support WLock Operation")
	return false
}

func (pr *PRecord) WUnlock(req *LockReq) {
	clog.Error("Partition mode does not support WUnlock Operation")
}

func (pr *PRecord) RLock(req *LockReq) bool {
	clog.Error("Partition mode does not support RLock Operation")
	return false
}

func (pr *PRecord) RUnlock(req *LockReq) {
	clog.Error("Partition mode does not support RUnlock Operation")
}

func (pr *PRecord) Upgrade(req *LockReq) bool {
	clog.Error("Partition mode does not support Upgrade Operation")
	return false
}

func (pr *PRecord) GetTuple() Tuple {
	return pr.tuple
}

func (pr *PRecord) SetTuple(t Tuple) {
	pr.tuple = t
}

func (pr *PRecord) DeltaValue(val Value, col int) {
	pr.tuple.DeltaValue(val, col)
}

type ORecord struct {
	padding1 [PADDING]byte
	key      Key
	tuple    Tuple
	last     wfmutex.WFMutex
	table    Table
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

func (or *ORecord) WLock(req *LockReq) bool {
	clog.Error("OCC mode does not support WLock Operation")
	return false
}

func (or *ORecord) WUnlock(req *LockReq) {
	clog.Error("OCC mode does not support WUnlock Operation")
}

func (or *ORecord) RLock(req *LockReq) bool {
	debug.PrintStack()
	clog.Error("OCC mode does not support RLock Operation")
	return false
}

func (or *ORecord) RUnlock(req *LockReq) {
	clog.Error("OCC mode does not support RUnlock Operation")
}

func (or *ORecord) Upgrade(req *LockReq) bool {
	clog.Error("OCC mode does not support Upgrade Operation")
	return false
}

func (or *ORecord) GetTuple() Tuple {
	return or.tuple
}

func (or *ORecord) SetTuple(t Tuple) {
	or.tuple = t
}

func (or *ORecord) DeltaValue(val Value, col int) {
	or.tuple.DeltaValue(val, col)
}

// Dummy Record
type DRecord struct {
	padding1 [PADDING]byte
	key      Key
	value    Value
	colNum   int
	tuple    Tuple
	lock     spinlock.Spinlock
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

func (dr *DRecord) WLock(req *LockReq) bool {
	dr.lock.Lock()
	return true
}

func (dr *DRecord) WUnlock(req *LockReq) {
	dr.lock.Unlock()
}

func (dr *DRecord) RLock(req *LockReq) bool {
	clog.Error("Dummy mode does not support RLock Operation")
	return false
}

func (dr *DRecord) RUnlock(req *LockReq) {
	clog.Error("Dummy mode does not support RUnlock Operation")
}

func (dr *DRecord) Upgrade(req *LockReq) bool {
	clog.Error("Dummy mode does not support Upgrade Operation")
	return false
}

func (dr *DRecord) GetTuple() Tuple {
	clog.Error("Dummy mode does not support GetTuple Operation")
	return nil
}

func (dr *DRecord) SetTuple(t Tuple) {
	clog.Error("Dummy mode does not support SetTuple Operation")
}

func (dr *DRecord) DeltaValue(val Value, col int) {
	clog.Error("Dummy mode does not support DeltaValue Operation")
}

type LRecord struct {
	padding1 [PADDING]byte
	key      Key
	tuple    Tuple
	wdLock   WDLock
	nwLock   nowaitlock.NoWaitLock
	table    Table
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

func (lr *LRecord) WLock(req *LockReq) bool {
	if *NoWait {
		return lr.nwLock.Lock()
	} else {
		req.reqType = LOCK_EX
		retState := lr.wdLock.Lock(req)
		if retState == LOCK_OK {
			return true
		} else if retState == LOCK_ABORT {
			return false
		} else { // Wait
			retState = <-req.state
			if retState == LOCK_OK {
				return true
			} else {
				clog.Error("Wait-Die: Unsuccessful Wait\n")
				return false
			}
		}
	}
}

func (lr *LRecord) WUnlock(req *LockReq) {
	if *NoWait {
		lr.nwLock.Unlock()
	} else {
		req.reqType = LOCK_EX
		lr.wdLock.Unlock(req)
	}
}

func (lr *LRecord) RLock(req *LockReq) bool {
	if *NoWait {
		return lr.nwLock.RLock()
	} else {
		req.reqType = LOCK_SH
		retState := lr.wdLock.Lock(req)
		if retState == LOCK_OK {
			return true
		} else if retState == LOCK_ABORT {
			return false
		} else { // Wait
			retState = <-req.state
			if retState == LOCK_OK {
				return true
			} else {
				clog.Error("Wait-Die: Unsuccessful Wait\n")
				return false
			}
		}
	}
}

func (lr *LRecord) RUnlock(req *LockReq) {
	if *NoWait {
		lr.nwLock.RUnlock()
	} else {
		req.reqType = LOCK_SH
		lr.wdLock.Unlock(req)
	}
	//return
}

func (lr *LRecord) Upgrade(req *LockReq) bool {
	if *NoWait {
		return lr.nwLock.Upgrade()
	} else {
		//return lr.wd.Lock(uint64(tid))
		req.reqType = LOCK_EX
		retState := lr.wdLock.Lock(req)
		if retState == LOCK_OK {
			return true
		} else if retState == LOCK_ABORT {
			return false
		} else { // Wait
			retState = <-req.state
			if retState == LOCK_OK {
				return true
			} else {
				clog.Error("Wait-Die: Unsuccessful Wait\n")
				return false
			}
		}
	}
}

func (lr *LRecord) GetTuple() Tuple {
	return lr.tuple
}

func (lr *LRecord) SetTuple(t Tuple) {
	lr.tuple = t
}

func (lr *LRecord) DeltaValue(val Value, col int) {
	lr.tuple.DeltaValue(val, col)
}

type ARecord struct {
	padding1 [PADDING]byte
	key      Key
	tuple    Tuple
	wdLock   WDLock
	nwLock   nowaitlock.NoWaitLock
	last     wfmutex.WFMutex
	conflict nowaitlock.NoWaitLock
	table    Table
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

func (ar *ARecord) WLock(req *LockReq) bool {
	if *NoWait {
		return ar.nwLock.Lock()
	} else {
		req.reqType = LOCK_EX
		retState := ar.wdLock.Lock(req)
		if retState == LOCK_OK {
			return true
		} else if retState == LOCK_ABORT {
			return false
		} else { // Wait
			retState = <-req.state
			if retState == LOCK_OK {
				return true
			} else {
				clog.Error("Wait-Die: Unsuccessful Wait\n")
				return false
			}
		}
	}
}

func (ar *ARecord) WUnlock(req *LockReq) {
	if *NoWait {
		ar.nwLock.Unlock()
	} else {
		req.reqType = LOCK_EX
		ar.wdLock.Unlock(req)
	}
}

func (ar *ARecord) RLock(req *LockReq) bool {
	if *NoWait {
		return ar.nwLock.RLock()
	} else {
		//return ar.wd.RLock(uint64(tid))
		req.reqType = LOCK_SH
		retState := ar.wdLock.Lock(req)
		if retState == LOCK_OK {
			return true
		} else if retState == LOCK_ABORT {
			return false
		} else { // Wait
			retState = <-req.state
			if retState == LOCK_OK {
				return true
			} else {
				clog.Error("Wait-Die: Unsuccessful Wait\n")
				return false
			}
		}
	}
}

func (ar *ARecord) RUnlock(req *LockReq) {
	if *NoWait {
		ar.nwLock.RUnlock()
	} else {
		req.reqType = LOCK_SH
		ar.wdLock.Unlock(req)
	}
}

func (ar *ARecord) Upgrade(req *LockReq) bool {
	if *NoWait {
		return ar.nwLock.Upgrade()
	} else {
		//return ar.wd.Upgrade(uint64(tid))
		req.reqType = LOCK_EX
		retState := ar.wdLock.Lock(req)
		if retState == LOCK_OK {
			return true
		} else if retState == LOCK_ABORT {
			return false
		} else { // Wait
			retState = <-req.state
			if retState == LOCK_OK {
				return true
			} else {
				clog.Error("Wait-Die: Unsuccessful Wait\n")
				return false
			}
		}
	}
}

func (ar *ARecord) GetTuple() Tuple {
	return ar.tuple
}

func (ar *ARecord) SetTuple(t Tuple) {
	ar.tuple = t
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

func (ar *ARecord) DeltaValue(val Value, col int) {
	ar.tuple.DeltaValue(val, col)
}
