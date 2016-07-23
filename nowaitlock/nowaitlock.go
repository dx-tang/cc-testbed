package nowaitlock

import (
	"sync/atomic"
)

type NoWaitLock struct {
	readerCount int32
}

const nwLockMaxReaders = 1 << 30

func (l *NoWaitLock) RLock() bool {

	r := atomic.LoadInt32(&l.readerCount)
	if r < 0 {
		return false
	}

	if atomic.CompareAndSwapInt32(&l.readerCount, r, r+1) {
		return true
	} else {
		return false
	}
}

func (l *NoWaitLock) RUnlock() {
	atomic.AddInt32(&l.readerCount, -1)
}

func (l *NoWaitLock) RUnlockN(n int32) {
	atomic.AddInt32(&l.readerCount, -n)
}

func (l *NoWaitLock) CheckLock() int32 {
	readCount := atomic.LoadInt32(&l.readerCount)
	if readCount > 0 {
		return readCount
	} else {
		return 0
	}
}

func (l *NoWaitLock) Lock() bool {
	if atomic.LoadInt32(&l.readerCount) != 0 {
		return false
	}

	success := atomic.CompareAndSwapInt32(&l.readerCount, 0, -nwLockMaxReaders)

	if success {
		return true
	} else {
		return false
	}

}

func (l *NoWaitLock) Unlock() {
	atomic.AddInt32(&l.readerCount, nwLockMaxReaders)
}

func (l *NoWaitLock) Upgrade() bool {

	if atomic.LoadInt32(&l.readerCount) != 1 {
		l.RUnlock()
		return false
	}

	success := atomic.CompareAndSwapInt32(&l.readerCount, 1, -nwLockMaxReaders)

	if success {
		return true
	} else {
		l.RUnlock()
		return false
	}

}
