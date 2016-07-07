package testbed

import (
	"sync/atomic"

	"github.com/totemtang/cc-testbed/clog"
)

const (
	LOCKED  = 1 << 63
	TIDMASK = 1<<63 - 1
)

type IndexLock struct {
	padding1 [PADDING]byte
	version  uint64
	padding2 [PADDING]byte
}

func (iLock *IndexLock) Lock() {
	done := false
	for !done {
		version := atomic.LoadUint64(&iLock.version)
		if version&LOCKED == 0 {
			new_version := (version + 1) | LOCKED
			done = atomic.CompareAndSwapUint64(&iLock.version, version, new_version)
		}
	}
}

func (iLock *IndexLock) Unlock() {
	version := atomic.LoadUint64(&iLock.version)
	version_unlock := version & TIDMASK
	done := atomic.CompareAndSwapUint64(&iLock.version, version, version_unlock)
	if !done {
		clog.Error("Unlock Fail %v", version)
	}
}

func (iLock *IndexLock) Read() uint64 {
	return atomic.LoadUint64(&iLock.version)
}
