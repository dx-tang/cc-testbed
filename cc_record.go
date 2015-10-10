package testbed

import (
	"log"
	"sync"
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

type StrAttr struct {
	index int
	value string
}

type BRecord struct {
	padding1  [128]byte
	key       Key
	intVal    int64
	stringVal []string
	recType   RecType
	lock      sync.RWMutex
	last      TID
	exist     bool
	padding2  [128]byte
}

func MakeBR(k Key, v Value, rt RecType) *BRecord {
	br := &BRecord{
		key:     k,
		recType: rt,
		last:    0,
		exist:   true,
	}

	// Initiate Value according to different types
	switch rt {
	case SINGLEINT:
		if v != nil {
			br.intVal = v.(int64)
		}
	case STRINGLIST:
		if v != nil {
			var inputStrList = v.([]string)
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

func (br *BRecord) Update(val Value) bool {
	if val == nil {
		return false
	}
	switch br.recType {
	case SINGLEINT:
		br.intVal = val.(int64)
	case STRINGLIST:
		strAttr := val.(*StrAttr)
		if strAttr.index >= len(br.stringVal) {
			log.Fatalf("Index %v out of range array length %v",
				strAttr.index, len(br.stringVal))
		}
		br.stringVal[strAttr.index] = strAttr.value
	}
	return true
}
