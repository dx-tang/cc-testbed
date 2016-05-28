package testbed

import (
	"sync/atomic"
)

type ConfDetector struct {
	readerCount int32
	padding1    [PADDING]byte
	homeCount   int32
}

func (cd *ConfDetector) IncrCounter() {
	atomic.AddInt32(&cd.readerCount, 1)
}

func (cd *ConfDetector) IncrHomeCounter() {
	atomic.AddInt32(&cd.homeCount, 1)
}

func (cd *ConfDetector) DecCounter() {
	atomic.AddInt32(&cd.readerCount, -1)
}

func (cd *ConfDetector) DecHomeCounter() {
	atomic.AddInt32(&cd.homeCount, -1)
}

func (cd *ConfDetector) DecCounterN(n int32) {
	atomic.AddInt32(&cd.readerCount, -n)
}

func (cd *ConfDetector) DecHomeCounterN(n int32) {
	atomic.AddInt32(&cd.homeCount, -n)
}

func (cd *ConfDetector) CleanCounter() {
	atomic.StoreInt32(&cd.readerCount, 0)
}

func (cd *ConfDetector) CleanHomeCounter() {
	atomic.StoreInt32(&cd.homeCount, 0)
}

func (cd *ConfDetector) CheckCounter() int32 {
	return atomic.LoadInt32(&cd.readerCount)
}

func (cd *ConfDetector) CheckCounterHome() int32 {
	return atomic.LoadInt32(&cd.homeCount)
}
