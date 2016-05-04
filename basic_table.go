package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/spinlock"
	"time"
)

const (
	SHARDCOUNT = 256
	KEY0       = 0
	KEY1       = 1
	KEY2       = 2
	KEY3       = 3
)

type Table interface {
	CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error)
	GetRecByID(k Key, partNum int) (Record, error)
	SetValueByID(k Key, partNum int, value Value, colNum int) error
	GetValueByID(k Key, partNum int, value Value, colNum int) error
	DeltaValueByID(k Key, partNum int, value Value, colNum int) error
	PrepareDelete(k Key, partNum int) (Record, error)
	DeleteRecord(k Key, partNum int) error
	ReleaseDelete(k Key, partNum int)
	PrepareInsert(k Key, partNum int) error
	InsertRecord(recs []InsertRec) error
	ReleaseInsert(k Key, partNum int)
	GetValueBySec(k Key, partNum int, val Value) error
	SetMode(mode int)
	BulkLoad(table Table)
}

type Shard struct {
	padding1 [PADDING]byte
	spinlock.RWSpinlock
	rows     map[Key]Record
	padding2 [PADDING]byte
}

type Partition struct {
	padding1   [PADDING]byte
	shardedMap []Shard
	padding2   [PADDING]byte
}

type BasicTable struct {
	padding1    [PADDING]byte
	data        []Partition
	valueSchema []BTYPE
	nKeys       int
	name        string
	isPartition bool
	nParts      int
	shardHash   func(Key) int
	mode        int
	iLock       spinlock.Spinlock
	padding2    [PADDING]byte
}

const (
	HASHINIT  = 2166136261
	HASHMULTI = 16777619
)

func NewBasicTable(schemaStrs []string, nParts int, isPartition bool, mode int, tableID int) *BasicTable {

	// We allocate more space to make the array algined to cache line
	bt := &BasicTable{
		valueSchema: make([]BTYPE, len(schemaStrs)-1),
		nKeys:       0,
		name:        schemaStrs[0],
		isPartition: isPartition,
		nParts:      nParts,
		mode:        mode,
	}

	if isPartition {
		bt.shardHash = func(k Key) int {
			return k[KEY1] % SHARDCOUNT
		}
	} else {
		bt.shardHash = func(k Key) int {
			hash := k[KEY1]*(*NumPart) + k[KEY0]
			return hash % SHARDCOUNT
		}
	}

	if tableID == ITEM {
		bt.shardHash = func(k Key) int {
			return k[KEY0] % SHARDCOUNT
		}
	} else if tableID == STOCK {
		if isPartition {
			bt.shardHash = func(k Key) int {
				return k[KEY1] % SHARDCOUNT
			}
		} else {
			bt.shardHash = func(k Key) int {
				return (k[KEY1]*(*NumPart) + k[KEY0]) % SHARDCOUNT
			}
		}
	}

	for j := 0; j < len(schemaStrs)-1; j++ {
		switch schemaStrs[j+1] {
		case "int":
			bt.valueSchema[j] = INTEGER
		case "string":
			bt.valueSchema[j] = STRING
		case "float":
			bt.valueSchema[j] = FLOAT
		case "date":
			bt.valueSchema[j] = DATE
		default:
			clog.Error("Wrong Value Type %s", schemaStrs[j+1])
		}
	}

	bt.data = make([]Partition, nParts)
	for k := 0; k < nParts; k++ {
		bt.data[k].shardedMap = make([]Shard, SHARDCOUNT)
		for i := 0; i < SHARDCOUNT; i++ {
			bt.data[k].shardedMap[i].rows = make(map[Key]Record)
		}
	}

	return bt

}

func (bt *BasicTable) CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error) {
	bt.iLock.Lock()
	defer bt.iLock.Unlock()
	bt.nKeys++

	if !bt.isPartition {
		partNum = 0
	}

	shardNum := bt.shardHash(k)

	shard := &bt.data[partNum].shardedMap[shardNum]

	if _, ok := shard.rows[k]; ok {
		return nil, EDUPKEY //One record with that key has existed;
	}

	r := MakeRecord(bt, k, tuple)
	shard.rows[k] = r
	return r, nil
}

func (bt *BasicTable) GetRecByID(k Key, partNum int) (Record, error) {

	if !bt.isPartition {
		partNum = 0
	}

	shardNum := bt.shardHash(k)
	shard := &bt.data[partNum].shardedMap[shardNum]

	if bt.mode != PARTITION {
		shard.RLock()
	}

	r, ok := shard.rows[k]
	if !ok {
		if bt.mode != PARTITION {
			shard.RUnlock()
		}
		return nil, ENOKEY
	} else {
		if bt.mode != PARTITION {
			shard.RUnlock()
		}
		return r, nil
	}
}

func (bt *BasicTable) SetValueByID(k Key, partNum int, value Value, colNum int) error {

	if !bt.isPartition {
		partNum = 0
	}

	shardNum := bt.shardHash(k)
	shard := &bt.data[partNum].shardedMap[shardNum]

	if bt.mode != PARTITION {
		shard.RLock()
	}

	r, ok := shard.rows[k]
	if !ok {
		if bt.mode != PARTITION {
			shard.RUnlock()
		}
		return ENOKEY
	}

	r.SetValue(value, colNum)
	if bt.mode != PARTITION {
		shard.RUnlock()
	}
	return nil
}

func (bt *BasicTable) GetValueByID(k Key, partNum int, value Value, colNum int) error {

	if !bt.isPartition {
		partNum = 0
	}

	shardNum := bt.shardHash(k)
	shard := &bt.data[partNum].shardedMap[shardNum]

	if bt.mode != PARTITION {
		shard.RLock()
	}

	r, ok := shard.rows[k]
	if !ok {
		if bt.mode != PARTITION {
			shard.RUnlock()
		}
		return ENOKEY
	}

	r.GetValue(value, colNum)

	if bt.mode != PARTITION {
		shard.RUnlock()
	}
	return nil
}

func (bt *BasicTable) PrepareDelete(k Key, partNum int) (Record, error) {
	clog.Error("Basic Table Not Support PrepareDelete")
	return nil, nil
}

func (bt *BasicTable) DeleteRecord(k Key, partNum int) error {
	clog.Error("Basic Table Not Support DeleteRecord")
	return nil
}

func (bt *BasicTable) ReleaseDelete(k Key, partNum int) {
	clog.Error("Basic Table Not Support BasicTable")
}

func (bt *BasicTable) PrepareInsert(k Key, partNum int) error {
	return nil
}

func (bt *BasicTable) InsertRecord(recs []InsertRec) error {
	for i, _ := range recs {
		iRec := &recs[i]
		partNum := iRec.partNum
		if !bt.isPartition {
			partNum = 0
		}

		shardNum := bt.shardHash(iRec.k)
		shard := &bt.data[partNum].shardedMap[shardNum]

		if bt.mode != PARTITION {
			shard.Lock()
		}

		_, ok := shard.rows[iRec.k]
		if ok {
			return EDUPKEY
		}

		shard.rows[iRec.k] = iRec.rec
		if bt.mode != PARTITION {
			shard.Unlock()
		}
	}

	return nil
}

func (bt *BasicTable) ReleaseInsert(k Key, partNum int) {
}

func (bt *BasicTable) GetValueBySec(k Key, partNum int, val Value) error {
	clog.Error("Basic Table Not Support GetValueBySec")
	return nil
}

func (bt *BasicTable) SetMode(mode int) {
	bt.mode = mode
}

func (bt *BasicTable) DeltaValueByID(k Key, partNum int, value Value, colNum int) error {
	if !bt.isPartition {
		partNum = 0
	}

	shardNum := bt.shardHash(k)
	shard := &bt.data[partNum].shardedMap[shardNum]

	if bt.mode != PARTITION {
		shard.RLock()
		defer shard.RUnlock()
	}

	r, ok := shard.rows[k]
	if !ok {
		return ENOKEY
	}

	r.DeltaValue(value, colNum)
	return nil
}
func (bt *BasicTable) BulkLoad(table Table) {
	recs := make([]InsertRec, 1)
	start := time.Now()
	for i, _ := range bt.data {
		part := &bt.data[i]
		recs[0].partNum = i
		for j, _ := range part.shardedMap {
			shard := &part.shardedMap[j]
			for k, v := range shard.rows {
				recs[0].k = k
				recs[0].rec = v
				table.InsertRecord(recs)
			}
		}
	}
	clog.Info("Basic Table Bulkload Takes %.2fs", time.Since(start).Seconds())
}

func checkSchema(v []Value, valueSchema []BTYPE) bool {
	if len(v) != len(valueSchema) {
		return false
	}

	for i := 0; i < len(v); i++ {
		switch v[i].(type) {
		case *IntValue:
			if valueSchema[i] != INTEGER {
				return false
			}
		case *StringValue:
			if valueSchema[i] != STRING {
				return false
			}
		case *FloatValue:
			if valueSchema[i] != FLOAT {
				return false
			}
		default:
			clog.Error("CheckSchema Not Supported Type %v\n", valueSchema[i])
			return false
		}
	}

	return true
}

func checkType(val Value, t BTYPE) bool {
	switch val.(type) {
	case *IntValue:
		if t != INTEGER {
			return false
		}
	case *StringValue:
		if t != STRING {
			return false
		}
	case *FloatValue:
		if t != FLOAT {
			return false
		}
	default:
		clog.Error("CheckType Not Supported Type %v\n", t)
		return false
	}
	return true
}
