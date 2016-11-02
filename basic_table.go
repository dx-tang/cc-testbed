package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/spinlock"
	//"sync"
	"time"
)

const (
	//SHARDCOUNT = 256
	SHARDCOUNT = 1000
	KEY0       = 0
	KEY1       = 1
	KEY2       = 2
	KEY3       = 3
)

type Table interface {
	CreateRecByID(k Key, partNum int, tuple Tuple, iaAR IndexAlloc) (Record, error)
	GetRecByID(k Key, partNum int) (Record, Bucket, uint64, error)
	GetValueBySec(k Key, partNum int, val Value) error
	PrepareDelete(k Key, partNum int) (Record, error)
	DeleteRecord(k Key, partNum int) error
	ReleaseDelete(k Key, partNum int)
	PrepareInsert(k Key, partNum int) error
	InsertRecord(recs []InsertRec, ia IndexAlloc) error
	ReleaseInsert(k Key, partNum int)
	SetLatch(useLatch bool)
	BulkLoad(table Table, ia IndexAlloc, begin int, end int, partitioner Partitioner)
	MergeLoad(table Table, ia IndexAlloc, begin int, end int, partitioner Partitioner)
	Reset()
	Clean()
}

type Shard struct {
	padding1 [PADDING]byte
	spinlock.RWSpinlock
	rows     map[Key]Record
	padding2 [PADDING]byte
}

type Partition struct {
	padding1 [PADDING]byte
	spinlock.Spinlock
	ht          *HashTable
	init_orders map[Key]int
	padding2    [PADDING]byte
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
	tableID     int
	padding2    [PADDING]byte
}

func NewBasicTable(numEntries int, nParts int, isPartition bool, useLatch []bool, tableID int) *BasicTable {

	// We allocate more space to make the array algined to cache line
	bt := &BasicTable{
		//valueSchema: make([]BTYPE, len(schemaStrs)-1),
		nKeys: 0,
		//name:        schemaStrs[0],
		isPartition: isPartition,
		nParts:      nParts,
		tableID:     tableID,
	}

	bt.data = make([]Partition, nParts)
	for k := 0; k < nParts; k++ {
		bt.data[k].ht = NewHashTable(numEntries, isPartition, useLatch[k], tableID)
		bt.data[k].init_orders = make(map[Key]int)
	}

	return bt

}

func (bt *BasicTable) CreateRecByID(k Key, partNum int, tuple Tuple, iaAR IndexAlloc) (Record, error) {

	if !bt.isPartition {
		partNum = 0
	}

	rec := MakeRecord(bt, k, tuple)

	ok := bt.data[partNum].ht.Put(k, rec, iaAR)

	if !ok {
		return nil, EDUPKEY
	}

	if WLTYPE == TPCCWL && bt.tableID == DISTRICT {
		bt.data[partNum].Lock()
		bt.data[partNum].init_orders[k] = tuple.(*DistrictTuple).d_next_o_id
		bt.data[partNum].Unlock()
	}

	return rec, nil
}

func (bt *BasicTable) GetRecByID(k Key, partNum int) (Record, Bucket, uint64, error) {

	if !bt.isPartition {
		partNum = 0
	}

	rec, ok := bt.data[partNum].ht.Get(k)

	if !ok {
		return nil, nil, 0, ENOKEY
	} else {
		return rec, nil, 0, nil
	}

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

func (bt *BasicTable) InsertRecord(recs []InsertRec, ia IndexAlloc) error {
	for i, _ := range recs {
		iRec := &recs[i]
		partNum := iRec.partNum
		if !bt.isPartition {
			partNum = 0
		}

		ok := bt.data[partNum].ht.Put(iRec.k, iRec.rec, ia)

		if !ok {
			return EDUPKEY
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

func (bt *BasicTable) SetLatch(useLatch bool) {
	for i, _ := range bt.data {
		bt.data[i].ht.SetLatch(useLatch)
	}
}

func (bt *BasicTable) BulkLoad(table Table, ia IndexAlloc, begin int, end int, partitioner Partitioner) {
	recs := make([]InsertRec, 1)
	start := time.Now()
	for i, _ := range bt.data {
		part := &bt.data[i]
		for j, _ := range part.ht.bucket {
			bucket := &part.ht.bucket[j]
			for bucket != nil {
				for p := 0; p < bucket.cur; p++ {
					k := bucket.keyArray[p]
					if WLTYPE == TPCCWL {
						if k[0] < begin || k[0] >= end {
							continue
						}
						recs[0].k = k
						recs[0].rec = bucket.recArray[p]
						recs[0].partNum = k[0]
						table.InsertRecord(recs, ia)
					} else {
						partNum := partitioner.GetPart(k)
						if partNum < begin || partNum >= end {
							continue
						}
						recs[0].k = k
						recs[0].rec = bucket.recArray[p]
						recs[0].partNum = partNum
						table.InsertRecord(recs, ia)
					}
				}
				bucket = bucket.next
			}
		}
	}
	clog.Debug("Basic Table Bulkload Takes %.2fs", time.Since(start).Seconds())
}

func (bt *BasicTable) MergeLoad(table Table, ia IndexAlloc, begin int, end int, partitioner Partitioner) {
	recs := make([]InsertRec, 1)
	start := time.Now()
	for i := begin; i < end; i++ {
		part := &bt.data[i]
		for j, _ := range part.ht.bucket {
			bucket := &part.ht.bucket[j]
			for bucket != nil {
				for p := 0; p < bucket.cur; p++ {
					k := bucket.keyArray[p]
					if WLTYPE == TPCCWL {
						recs[0].k = k
						recs[0].rec = bucket.recArray[p]
						recs[0].partNum = 0
						table.InsertRecord(recs, ia)
					} else {
						recs[0].k = k
						recs[0].rec = bucket.recArray[p]
						recs[0].partNum = 0
						table.InsertRecord(recs, ia)
					}
				}
				bucket = bucket.next
			}
		}
	}
	clog.Debug("Basic Table Merging Takes %.2fs", time.Since(start).Seconds())
}

func (bt *BasicTable) Reset() {
	if WLTYPE == TPCCWL && bt.tableID == DISTRICT {
		for i, _ := range bt.data {
			part := &bt.data[i]
			for k, init_order := range part.init_orders {
				rec, _ := part.ht.Get(k)
				rec.GetTuple().(*DistrictTuple).d_next_o_id = init_order
			}

		}
	}
}

func (bt *BasicTable) Clean() {
	for i := 0; i < 1; i++ {
		part := &bt.data[i]
		for j, _ := range part.ht.bucket {
			if j%100000 == 0 {
				clog.Info("Bucket %v", j)
			}
			bucket := &part.ht.bucket[j]
			bucket.cur = 0
			bucket.next = nil
		}
	}
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
