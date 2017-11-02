package testbed

import (
	//"runtime/debug"
	//"time"

	"github.com/totemtang/cc-testbed/clog"
	//"github.com/totemtang/cc-testbed/mixlock"
	//"github.com/totemtang/cc-testbed/nowaitlock"
	"github.com/totemtang/cc-testbed/spinlock"
	//"github.com/totemtang/cc-testbed/wfmutex"
)

type TRecord struct {
	padding1  [PADDING]byte
	key       Key
	lock      spinlock.Spinlock
	groupLock spinlock.RWSpinlock
	table     Table
	tuple     Tuple
	stash     SSI_Entry
	padding2  [PADDING]byte
}

func (tr *TRecord) GetKey() Key {
	return tr.key
}

func (tr *TRecord) SetKey(k Key) {
	tr.key = k
}

func (tr *TRecord) Lock() (bool, TID) {
	clog.Error("Partition mode does not support Lock Operation")
	return false, 0
}

func (tr *TRecord) Unlock(tid TID) {
	clog.Error("Partition mode does not support Unlock Operation")
}

func (tr *TRecord) IsUnlocked() (bool, TID) {
	clog.Error("Partition mode does not support IsUnlocked Operation")
	return false, 0
}

func (tr *TRecord) GetValue(val Value, colNum int) {
	tr.tuple.GetValue(val, colNum)
}

func (tr *TRecord) SetValue(val Value, colNum int) {
	tr.tuple.SetValue(val, colNum)
}

func (tr *TRecord) GetTID() TID {
	clog.Error("Partition mode does not support GetTID Operation")
	return 0
}

func (tr *TRecord) SetTID(tid TID) {
	clog.Error("Partition mode does not support SetTID Operation")
}

func (tr *TRecord) WLock(req *LockReq) (bool, TID) {
	tr.lock.Lock()
	return true, 0
}

func (tr *TRecord) WUnlock(req *LockReq, tid TID) {
	tr.lock.Unlock()
}

func (tr *TRecord) RLock(req *LockReq) (bool, TID) {
	clog.Error("Partition mode does not support RLock Operation")
	return false, 0
}

func (tr *TRecord) RUnlock(req *LockReq) {
	clog.Error("Partition mode does not support RUnlock Operation")
}

func (tr *TRecord) Upgrade(req *LockReq) bool {
	clog.Error("Partition mode does not support Upgrade Operation")
	return false
}

func (tr *TRecord) GetTuple() Tuple {
	return tr.tuple
}

func (tr *TRecord) SetTuple(t Tuple) {
	tr.tuple = t
}

func (tr *TRecord) DeltaValue(val Value, col int) {
	tr.tuple.DeltaValue(val, col)
}

func (tr *TRecord) GroupLock(group int) {
	if group == GROUPA {
		tr.groupLock.RLock()
	} else if group == GROUPB {
		tr.groupLock.Lock()
	}
}

func (tr *TRecord) GroupUnlock(group int) {
	if group == GROUPA {
		tr.groupLock.RUnlock()
	} else if group == GROUPB {
		tr.groupLock.Unlock()
	}
}

func (tr *TRecord) Insert(vals []Value, cols []int) {
	tr.stash.vals[0] = vals[0].(*IntValue).intVal
	tr.stash.vals[1] = vals[1].(*IntValue).intVal
	tr.stash.vals[2] = vals[2].(*IntValue).intVal
	tr.stash.cols[0] = cols[0]
	tr.stash.cols[1] = cols[1]
	tr.stash.cols[2] = cols[2]
}
