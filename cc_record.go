package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/wfmutex"
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
	Lock() (bool, TID)
	Unlock(tid TID)
	IsUnlocked() (bool, TID)
	Value() Value
	GetKey() Key
	UpdateValue(val Value) bool
	GetTID() TID
	SetTID(tid TID)
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
	} else if *SysType == OCC {
		or := &ORecord{
			key:     k,
			recType: rt,
			last:    wfmutex.WFMutex{},
		}
		// Initiate Value according to different types
		switch rt {
		case SINGLEINT:
			if v != nil {
				or.intVal = v.(int64)
			}
		case STRINGLIST:
			if v != nil {
				var inputStrList = v.([]string)
				or.stringVal = make([]string, len(inputStrList))
				for i, _ := range inputStrList {
					or.stringVal[i] = inputStrList[i]
				}
			}
		}
		return or
	} else {
		clog.Error("System Type %v Not Supported Yet", *SysType)
		return nil
	}
}

type PRecord struct {
	padding1  [128]byte
	key       Key
	intVal    int64
	stringVal []string
	recType   RecType
	padding2  [128]byte
}

func (pr *PRecord) GetKey() Key {
	return pr.key
}

func (pr *PRecord) Lock() (bool, TID) {
	clog.Error("Partition mode does not support Lock Operation")
	return false, 0
}

func (pr *PRecord) Unlock(tid TID) {
	clog.Error("Partition mode does not support Unlock Operation")
}

func (pr *PRecord) IsUnlocked() (bool, TID) {
	clog.Error("Partition mode does not support IsUnlocked Operation")
	return false, 0
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

func (pr *PRecord) SetTID(tid TID) {
	clog.Error("Partition mode does not support SetTID Operation")
}

type ORecord struct {
	padding1  [128]byte
	key       Key
	intVal    int64
	stringVal []string
	recType   RecType
	last      wfmutex.WFMutex
	padding2  [128]byte
}

func (or *ORecord) Lock() (bool, TID) {
	b, x := or.last.Lock()
	return b, TID(x)
}

func (or *ORecord) Unlock(tid TID) {
	or.last.Unlock(uint64(tid))
}

func (or *ORecord) IsUnlocked() (bool, TID) {
	x := or.last.Read()
	if x&wfmutex.LOCKED != 0 {
		return false, TID(x)
	}
	return true, TID(x)
}

func (or *ORecord) Value() Value {
	switch or.recType {
	case SINGLEINT:
		return or.intVal
	case STRINGLIST:
		return or.stringVal
	}
	return nil
}

func (or *ORecord) GetKey() Key {
	return or.key
}

func (or *ORecord) UpdateValue(val Value) bool {
	if val == nil {
		return false
	}
	switch or.recType {
	case SINGLEINT:
		or.intVal = val.(int64)
	case STRINGLIST:
		strAttr := val.(*StrAttr)
		if strAttr.index >= len(or.stringVal) {
			clog.Error("Index %v out of range array length %v",
				strAttr.index, len(or.stringVal))
		}
		or.stringVal[strAttr.index] = strAttr.value
	}
	return true
}

func (or *ORecord) GetTID() TID {
	return TID(or.last.Read())
}

func (or *ORecord) SetTID(tid TID) {
	clog.Error("OCC mode does not support SetTID Operation")
}
