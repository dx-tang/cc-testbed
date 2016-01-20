package wdspinlock

import (
	"sync/atomic"
)

type WDSpinlock struct {
	state int32
	trial int
}

const (
	mutexLocked = 1 << iota // mutex is locked
)

// Lock locks s.
// If the lock is already in use, the calling goroutine
// spins until the mutex is available.
func (s *WDSpinlock) Lock() bool {
	done := false
	i := s.trial
	for !done {
		if i == 0 {
			return false
		}
		done = atomic.CompareAndSwapInt32(&s.state, 0, mutexLocked)
		i--
	}
	return true
}

// Unlock unlocks s.
//
// A locked Spinlock is not associated with a particular goroutine.
// It is allowed for one goroutine to lock a Spinlock and then
// arrange for another goroutine to unlock it.
func (s *WDSpinlock) Unlock() {
	new := atomic.AddInt32(&s.state, -mutexLocked)
	if (new+mutexLocked)&mutexLocked == 0 {
		panic("sync: unlock of unlocked mutex")
	}
}

func (s *WDSpinlock) SetTrial(trial int) {
	s.trial = trial
}

type WDRWSpinlock struct {
	w           WDSpinlock
	readerCount int32
}

const spinlockMaxReaders = 1 << 30

func (l *WDRWSpinlock) RLock() bool {
	if atomic.AddInt32(&l.readerCount, 1) < 0 {
		i := l.w.trial
		for atomic.LoadInt32(&l.readerCount) < 0 {
			if i == 0 {
				atomic.AddInt32(&l.readerCount, -1)
				return false
			}
			i--
		}
	}
	return true
}

func (l *WDRWSpinlock) RUnlock() {
	atomic.AddInt32(&l.readerCount, -1)
}

func (l *WDRWSpinlock) Lock() bool {
	if !l.w.Lock() {
		return false
	}
	r := atomic.AddInt32(&l.readerCount, -spinlockMaxReaders) + spinlockMaxReaders
	i := l.w.trial
	for r != 0 {
		if i == 0 {
			l.w.Unlock()
			return false
		}
		r = atomic.LoadInt32(&l.readerCount) + spinlockMaxReaders
		i--
	}
	return true
}

func (l *WDRWSpinlock) Unlock() {
	atomic.AddInt32(&l.readerCount, spinlockMaxReaders)
	l.w.Unlock()
}

func (l *WDRWSpinlock) Upgrade() bool {
	if !l.w.Lock() {
		atomic.AddInt32(&l.readerCount, -1)
		return false
	}
	r := atomic.AddInt32(&l.readerCount, -(spinlockMaxReaders+1)) + spinlockMaxReaders
	i := l.w.trial
	for r != 0 {
		if i == 0 {
			l.Unlock()
			return false
		}
		r = atomic.LoadInt32(&l.readerCount) + spinlockMaxReaders
		i--
	}
	return true
}

func (l *WDRWSpinlock) SetTrial(trial int) {
	l.w.trial = trial
}
