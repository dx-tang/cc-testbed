package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/spinlock"
)

var nowait_flag bool = false

const (
	maxreaders   = 1 << 7
	readermask   = 1<<8 - 1
	readeroffset = 8
)

type WDLock struct {
	latch     spinlock.Spinlock
	owners    LockReqList
	waiters   LockReqList
	lock_type int
}

func (w *WDLock) Initialize() {
	w.latch.SetTrial(spinlock.PREEMPT)
	//w.owners = NewLockReqList()
	//w.waiters = NewLockReqList()
	w.lock_type = LOCK_NONE
}

func (w *WDLock) Lock(req *LockReq) int {
	w.latch.Lock()
	defer w.latch.Unlock()

	//clog.Info("lock %v, %v", req.id, req.tid)

	var retState int

	waiters := &w.waiters
	owners := &w.owners

	noWaiters := false
	if waiters.Header() == nil {
		noWaiters = true
	}

	if owners.Header() != nil && owners.Header().Next == nil && owners.Header().id == req.id { // Upgrade
		en := owners.Header()
		en.reqType = req.reqType
		w.lock_type = req.reqType
		return LOCK_OK
	}

	conflict := Conflict_Req(req.reqType, w.lock_type)

	if nowait_flag && !conflict && !noWaiters { // SHARE vs. SHARE
		l := waiters.Header()
		if req.tid < l.tid { // Older than waiters
			conflict = true
		}
	}

	if conflict {
		if !nowait_flag {
			return LOCK_ABORT
		}
		canWait := true
		entry := owners.Header()
		for entry != nil {
			if req.tid > entry.tid {
				canWait = false
				break
			}
			entry = entry.Next
		}
		if canWait {
			newE := allocEntry(req.id)
			newE.id = req.id
			newE.tid = req.tid
			newE.reqType = req.reqType
			req.state = newE.state
			entry := waiters.Header()
			for entry != nil && req.tid < entry.tid {
				entry = entry.Next
			}
			if entry != nil {
				waiters.InsertBefore(newE, entry)
			} else {
				waiters.InsertTailer(newE)
			}
			//clog.Info("%v Waiting %v", req.id, req.tid)
			retState = LOCK_WAIT
		} else {
			retState = LOCK_ABORT
		}
	} else {
		newE := allocEntry(req.id)
		newE.id = req.id
		newE.tid = req.tid
		newE.reqType = req.reqType
		owners.InsertHeader(newE)
		w.lock_type = req.reqType
		retState = LOCK_OK
	}

	return retState

}

func (w *WDLock) Unlock(req *LockReq) {
	w.latch.Lock()
	defer w.latch.Unlock()

	//clog.Info("Unlock %v, %v", req.id, req.tid)

	owners := &w.owners
	waiters := &w.waiters
	en := owners.Header()
	for en != nil && en.id != req.id {
		en = en.Next
	}
	// Find in owners list; delete it
	if en != nil {
		owners.Remove(en)
		releaseElem(en.id, en)
		if owners.Header() == nil {
			w.lock_type = LOCK_NONE
		}
	} else {
		clog.Error("Not Possible\n")
		en = waiters.Header()
		for en != nil && en.id != req.id {
			en = en.Next
		}
		waiters.Remove(en)
		releaseElem(en.id, en)
	}

	/*if owners.Header() != nil {
		clog.Info("Error Here %v; %v", owners.Header().id, owners.Header().tid)
		if waiters.Header() != nil {
			clog.Error("Waiters %v; %v", waiters.Header().id, waiters.Header().tid)
		}
	}*/

	if owners.Header() != nil && owners.Header().Next == nil && waiters.Header() != nil && owners.Header().id == waiters.Header().id {
		en = owners.RemoveHeader()
		releaseElem(en.id, en)
		en = waiters.RemoveHeader()
		owners.InsertHeader(en)
		w.lock_type = en.reqType
		en.state <- LOCK_OK
		return
	}

	for waiters.Header() != nil && !Conflict_Req(w.lock_type, waiters.Header().reqType) {
		en = waiters.RemoveHeader()
		owners.InsertHeader(en)
		en.state <- LOCK_OK
		w.lock_type = en.reqType
		//clog.Info("Notify %v %v", en.id, en.tid)
	}

}

func (w *WDLock) LockState() int {
	w.latch.Lock()
	defer w.latch.Unlock()
	return w.lock_type
}

func Conflict_Req(req1 int, req2 int) bool {
	if req1 == LOCK_NONE || req2 == LOCK_NONE {
		return false
	} else if req1 == LOCK_SH && req2 == LOCK_SH {
		return false
	}
	return true
}
