package nowaitlock

import (
	"sync/atomic"
)

type NoWaitLock struct {
	readerCount int32
}

const nwLockMaxReaders = 1 << 30

func (l *NoWaitLock) RLock() bool {

	if atomic.LoadInt32(&l.readerCount) < 0 {
		return false
	}

	if atomic.AddInt32(&l.readerCount, 1) < 0 {
		atomic.AddInt32(&l.readerCount, -1)
		return false
	}

	return true
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
	if atomic.LoadInt32(&l.readerCount) > 0 {
		return false
	}

	r := atomic.AddInt32(&l.readerCount, -nwLockMaxReaders) + nwLockMaxReaders
	if r == 0 {
		return true
	} else {
		atomic.AddInt32(&l.readerCount, nwLockMaxReaders)
		return false
	}
}

func (l *NoWaitLock) Unlock() {
	atomic.AddInt32(&l.readerCount, nwLockMaxReaders)
}

func (l *NoWaitLock) Upgrade() bool {

	if atomic.LoadInt32(&l.readerCount) > 1 { // More than one reader
		atomic.AddInt32(&l.readerCount, -1)
		return false
	}

	r := atomic.AddInt32(&l.readerCount, -(nwLockMaxReaders+1)) + nwLockMaxReaders
	if r == 0 {
		return true
	} else {
		atomic.AddInt32(&l.readerCount, nwLockMaxReaders)
		return false
	}
}
