package testbed

import (
	"github.com/totemtang/cc-testbed/spinlock"
)

type RecType int

const (
	SINGLEINT = iota
	STRINGLIST
)

const (
	FIELDS   = 16
	PERFIELD = 100
)

type BRecord struct {
	padding1  [128]byte
	key       Key
	intVal    int64
	stringVal []string
	recType   RecType
	lock      spinlock.RWSpinlock
	last      TID
	exist     bool
	padding2  [128]byte
}

func MakeBR(k Key, val Value, rt RecType) *BRecord {
	br := &BRecord{
		key:     k,
		recType: rt,
		last:    0,
		exist:   true,
	}

	// whether need a spinlock for every record
	if *SysType != PARTITION {
		br.lock = spinlock.RWSpinlock{}
	}

	// Initiate Value according to different types
	switch rt {
	case SINGLEINT:
		if val != nil {
			br.intVal = val.(int64)
		}
	case STRINGLIST:
		if val != nil {
			var inputStrList = val.([]string)
			br.stringVal = make([]string, len(inputStrList))
			for i, _ := range inputStrList {
				br.stringVal[i] = inputStrList[i]
			}
		}
	}
	return br
}

func (br *BRecord) Lock() {
	br.lock.Lock()
}

func (br *BRecord) Unlock() {
	br.lock.Unlock()
}

func (br *BRecord) RLock() {
	br.lock.RLock()
}

func (br *BRecord) RUnlock() {
	br.lock.RUnlock()
}

func (br *BRecord) Value() Value {
	switch br.recType {
	case SINGLEINT:
		return br.intVal
	case STRINGLIST:
		return br.stringVal
	}
	return nil
}
