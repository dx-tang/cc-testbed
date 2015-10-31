package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
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

type Record interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()
	Value() Value
	GetKey() Key
	UpdateValue(val Value) bool
	GetTID() TID
}

type PRecord struct {
	padding1  [128]byte
	key       Key
	intVal    int64
	stringVal []string
	recType   RecType
	padding2  [128]byte
}

func MakeRecord(k Key, v Value, rt RecType) Record {
	if *SysType == PARTITION {
		pr := &PRecord{
			key:     k,
			recType: rt,
		}

		// Initiate Value according to different types
		switch rt {
		case SINGLEINT:
			if v != nil {
				pr.intVal = v.(int64)
			}
		case STRINGLIST:
			if v != nil {
				var inputStrList = v.([]string)
				pr.stringVal = make([]string, len(inputStrList))
				for i, _ := range inputStrList {
					pr.stringVal[i] = inputStrList[i]
				}
			}
		}
		return pr
	} else {
		clog.Error("System Type %v Not Supported Yet", *SysType)
		return nil
	}

}

func (pr *PRecord) GetKey() Key {
	return pr.key
}

func (pr *PRecord) Lock() {
	clog.Error("Partition mode does not support Lock Operation")
}

func (pr *PRecord) Unlock() {
	clog.Error("Partition mode does not support Unlock Operation")
}

func (pr *PRecord) RLock() {
	clog.Error("Partition mode does not support RLock Operation")
}

func (pr *PRecord) RUnlock() {
	clog.Error("Partition mode does not support RUnlock Operation")
}

func (pr *PRecord) Value() Value {
	switch pr.recType {
	case SINGLEINT:
		return pr.intVal
	case STRINGLIST:
		return pr.stringVal
	}
	return nil
}

func (pr *PRecord) UpdateValue(val Value) bool {
	if val == nil {
		return false
	}
	switch pr.recType {
	case SINGLEINT:
		pr.intVal = val.(int64)
	case STRINGLIST:
		strAttr := val.(*StrAttr)
		if strAttr.index >= len(pr.stringVal) {
			clog.Error("Index %v out of range array length %v",
				strAttr.index, len(pr.stringVal))
		}
		pr.stringVal[strAttr.index] = strAttr.value
	}
	return true
}

func (pr *PRecord) GetTID() TID {
	clog.Error("Partition mode does not support GetTID Operation")
	return 0
}
