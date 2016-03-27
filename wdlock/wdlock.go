package wdlock

import (
	"log"
	"sync/atomic"
)

const (
	maxreaders   = 1 << 7
	readermask   = 1<<8 - 1
	readeroffset = 8
)

type WDLock struct {
	l uint64
}

func (w *WDLock) Initialize() {
	w.l = maxreaders
}

func (w *WDLock) RLock(tid uint64) bool {
	var oldlock, newlock, readers uint64
	for {
		//log.Printf("id %v; tid %v\n", id, tid)
		oldlock = atomic.LoadUint64(&w.l)
		readers = oldlock & readermask
		if readers >= maxreaders {
			// No Writers
			newtid := tid
			if readers != maxreaders {
				oldtid := oldlock >> readeroffset
				if tid > oldtid {
					newtid = oldtid
				}
			}
			newlock = newtid<<readeroffset + readers + 1
			done := atomic.CompareAndSwapUint64(&w.l, oldlock, newlock)
			if done {
				return true
			}
		} else {
			oldtid := oldlock >> readeroffset
			if tid > oldtid {
				//log.Fatalf("Here %v, %v, %v, %v", tid, oldtid, maxreaders, readers)
				return false
			}
			//else if tid == oldtid {
			//	log.Fatalf("RLock itself\n")
			//}
		}
	}
}

func (w *WDLock) RUnlock() {
	atomic.AddUint64(&w.l, ^uint64(0))
}

func (w *WDLock) IsRlocked() bool {
	oldlock := atomic.LoadUint64(&w.l)
	readers := oldlock & readermask
	if readers != maxreaders && readers != 0 {
		return true
	} else {
		return false
	}
}

func (w *WDLock) Lock(tid uint64) bool {
	var oldlock, newlock, readers uint64
	for {
		oldlock = atomic.LoadUint64(&w.l)
		readers = oldlock & readermask
		if readers == maxreaders {
			// No Readers and Writers
			newlock = tid << readeroffset
			done := atomic.CompareAndSwapUint64(&w.l, oldlock, newlock)
			if done {
				return true
			}
		} else {
			oldtid := oldlock >> readeroffset
			if tid > oldtid {
				return false
			}
			//else if tid == oldtid {
			//	log.Fatalf("WLock itself\n")
			//}
		}
	}
}

func (w *WDLock) Unlock() {
	atomic.AddUint64(&w.l, maxreaders)
}

func (w *WDLock) IsLocked() bool {
	oldlock := atomic.LoadUint64(&w.l)
	readers := oldlock & readermask
	if readers == 0 {
		return true
	} else {
		return false
	}
}

func (w *WDLock) Upgrade(tid uint64) bool {
	var oldlock, newlock, readers uint64
	for {
		oldlock = atomic.LoadUint64(&w.l)
		readers = oldlock & readermask
		if readers == maxreaders {
			log.Fatalf("Upgrade Error")
		} else if readers == maxreaders+1 || tid == oldlock>>readeroffset {
			newlock = oldlock - 1 - maxreaders
			done := atomic.CompareAndSwapUint64(&w.l, oldlock, newlock)
			if done {
				for atomic.LoadUint64(&w.l)&readermask != 0 {
				}
				return true
			}
		} else {
			w.RUnlock()
			return false
		}
	}
}

func (w *WDLock) GetTid() uint64 {
	return (atomic.LoadUint64(&w.l) >> readeroffset)
}
