package spinlockopt

import (
	"sync/atomic"
)

type WDRWSpinlock struct {
	readerCount int32
	trial       int
}

const spinlockMaxReaders = 1 << 30

func (l *WDRWSpinlock) RLock(trial int) bool {
	i := trial
	for atomic.LoadInt32(&l.readerCount) < 0 {
		if i == 0 {
			return false
		}
		i--
	}

	if atomic.AddInt32(&l.readerCount, 1) < 0 {
		atomic.AddInt32(&l.readerCount, -1)
		return false
	}

	return true
}

func (l *WDRWSpinlock) RUnlock() {
	atomic.AddInt32(&l.readerCount, -1)
}

func (l *WDRWSpinlock) CheckLock() int32 {
	readCount := atomic.LoadInt32(&l.readerCount)
	if readCount > 0 {
		return readCount
	} else {
		return 0
	}

}

func (l *WDRWSpinlock) Lock(trial int) bool {
	i := trial
	for atomic.LoadInt32(&l.readerCount) > 0 {
		if i == 0 {
			return false
		}
		i--
	}

	r := atomic.AddInt32(&l.readerCount, -spinlockMaxReaders) + spinlockMaxReaders
	if r == 0 {
		return true
	} else {
		atomic.AddInt32(&l.readerCount, spinlockMaxReaders)
		return false
	}
}

func (l *WDRWSpinlock) Unlock() {
	atomic.AddInt32(&l.readerCount, spinlockMaxReaders)
}

func (l *WDRWSpinlock) Upgrade(trial int) bool {
	i := trial
	for atomic.LoadInt32(&l.readerCount) > 1 {
		if i == 0 {
			atomic.AddInt32(&l.readerCount, -1)
			return false
		}
		i--
	}

	r := atomic.AddInt32(&l.readerCount, -(spinlockMaxReaders+1)) + spinlockMaxReaders
	if r == 0 {
		return true
	} else {
		atomic.AddInt32(&l.readerCount, spinlockMaxReaders)
		return false
	}
}

func (l *WDRWSpinlock) SetTrial(trial int) {
	l.trial = trial
}
