package mixlock

import (
	//"fmt"
	"sync/atomic"
)

// LayOut: LOCK[0:6] TIMESTAMP[7:63]
type MixLock struct {
	w uint64
}

const (
	LOCKMASK   = 1<<63 - 1<<7 + 1<<63
	TIDMASK    = (1<<7 - 1)
	READERMASK = 1<<63 - 1<<6 + 1<<63
	MAXREADERS = 1<<6 - 1
	LOCKBITS   = 7
	LOCKED     = 1 << 6
)

func (l *MixLock) RLock() (bool, uint64) {

	w := atomic.LoadUint64(&l.w)
	if w&LOCKED != 0 {
		return false, 0
	}

	w = atomic.AddUint64(&l.w, 1)

	if w&LOCKED != 0 {
		atomic.AddUint64(&l.w, ^uint64(0))
		return false, 0
	}

	return true, w >> LOCKBITS
}

func (l *MixLock) Read() uint64 {
	return atomic.LoadUint64(&l.w)
}

func (l *MixLock) RUnlock() {
	atomic.AddUint64(&l.w, ^uint64(0))
}

func (l *MixLock) RUnlockN(n int32) {
	atomic.AddUint64(&l.w, ^uint64(n-1))
}

func (l *MixLock) CheckLock() uint64 {
	return atomic.LoadUint64(&l.w)
	//readCount := atomic.LoadUint64(&l.w) & LOCKMASK
	/*if readCount <= MAXREADERS {
		return readCount
	} else {
		return 0
	}*/

}

func (l *MixLock) Lock() (bool, uint64) {
	w := atomic.LoadUint64(&l.w)
	if w&TIDMASK != 0 {
		return false, 0
	}

	newWord := w | LOCKED

	success := atomic.CompareAndSwapUint64(&l.w, w, newWord)

	if success {
		return true, w >> LOCKBITS
	} else {
		return false, 0
	}
}

func (l *MixLock) Unlock(t uint64) {
	newWord := t << LOCKBITS
	oldWord := atomic.LoadUint64(&l.w) & READERMASK
	atomic.AddUint64(&l.w, newWord-oldWord)
}

func (l *MixLock) Upgrade() bool {
	w := atomic.LoadUint64(&l.w)
	if w&TIDMASK != 1 {
		l.RUnlock()
		return false
	}

	newWord := (w & LOCKMASK) | LOCKED

	success := atomic.CompareAndSwapUint64(&l.w, w, newWord)

	if success {
		return true
	} else {
		l.RUnlock()
		return false
	}
}
