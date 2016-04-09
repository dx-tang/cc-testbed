package testbed

import (
	"bufio"
	"errors"
	"flag"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/spinlock"
	"github.com/totemtang/cc-testbed/spinlockopt"
	"github.com/totemtang/cc-testbed/wfmutex"
)

const (
	SLTRIAL = 500
)

const (
	CHUNKS = 256
)

const (
	PARTITION = iota
	OCC
	LOCKING
	ADAPTIVE

	LAST_MODE
)

const (
	SCHEMA   = "Schema\n"
	WORKLOAD = "Workload\n"
	KEYRANGE = "KeyRange\n"
)

const (
	PADDING      = 64 // cache size
	PADDINGBYTE  = 64
	PADDINGINT   = 16
	PADDINGINT64 = 8
	PADDINGKEY   = 2
)

const (
	INTEGER = iota // int64
	FLOAT          // floate64
	STRING         // string
)

var (
	EABORT  = errors.New("abort")
	ENOKEY  = errors.New("no entry")
	EDUPKEY = errors.New("key has existed")
	ENODEL  = errors.New("No Delete Key")

	// Error for Smallbank
	ELACKBALANCE = errors.New("Checking Balance Not Enough")
	ENEGSAVINGS  = errors.New("Negative Saving Balance")
)

type BTYPE int // Basic types: int64, float64, string
type TID uint64

var NumPart = flag.Int("ncores", 2, "number of partitions; equals to the number of cores")
var SysType = flag.Int("sys", PARTITION, "System Type we will use")
var SpinLock = flag.Bool("spinlock", true, "Use spinlock or mutexlock")
var NoWait = flag.Bool("nw", true, "Use Waitdie or NoWait for 2PL")

type Store struct {
	padding1     [PADDING]byte
	tables       []Table
	spinLock     []*SpinLockPad
	wfLock       []*WFMuTexPAD
	confLock     []*WDRWSpinlockPAD
	mutexLock    []*RWMutex
	nParts       int
	isPhysical   bool
	tableToIndex map[string]int
	padding2     [PADDING]byte
}

func NewStore(schema string, nParts int, isPhysical bool) *Store {

	// Open Schema File and Read Configuration
	sch, err := os.OpenFile(schema, os.O_RDONLY, 0600)
	if err != nil {
		clog.Error("Open File Error %s\n", err.Error())
	}
	defer sch.Close()
	sReader := bufio.NewReader(sch)

	var tc []byte
	for {
		tc, err = sReader.ReadBytes('\n')
		if err != nil {
			clog.Error("Read File %s Error, End of File\n", schema)
		}
		if strings.Compare(SCHEMA, string(tc)) == 0 {
			break
		}
	}

	// read the number of tables
	tc, err1 := sReader.ReadBytes('\n')
	if err1 != nil {
		clog.Error("Read File %s Error, No Data\n", schema)
	}
	tableCount, err2 := strconv.Atoi(string(tc[0 : len(tc)-1]))
	if err2 != nil {
		clog.Error("Schema File %s Wrong Format\n", schema)
	}

	s := &Store{
		tables:       make([]*Table, tableCount),
		tableToIndex: make(map[string]int),
		wfLock:       make([]*WFMuTexPAD, nParts),
		confLock:     make([]*WDRWSpinlockPAD, nParts),
		mutexLock:    make([]*RWMutex, nParts),
		spinLock:     make([]*SpinLockPad, nParts),
		nParts:       nParts,
		isPhysical:   isPhysical,
	}

	for i := 0; i < nParts; i++ {
		s.wfLock[i] = &WFMuTexPAD{}
		s.confLock[i] = &WDRWSpinlockPAD{}
		if *SpinLock {
			s.spinLock[i] = &SpinLockPad{}
			s.spinLock[i].SetTrial(SLTRIAL)
		} else {
			s.mutexLock[i] = &RWMutex{}
		}
	}

	var line []byte
	for i := 0; i < tableCount; i++ {
		line, err = sReader.ReadBytes('\n')
		if err != nil {
			clog.Error("Schema File %s Wrong Format at Line %d\n", schema, i+1)
		}
		schemaStrs := strings.Split(string(line[:len(line)-1]), ":")
		if len(schemaStrs) < 2 {
			clog.Error("Schema File %s Wrong Format at Line %d\n", schema, i+1)
		}

		// The first element is table name;
		s.tableToIndex[schemaStrs[0]] = i
		// We allocate more space to make the array algined to cache line
		s.tables[i] = &Table{
			nKeys:       0,
			name:        schemaStrs[0],
			valueSchema: make([]BTYPE, len(schemaStrs)-1),
		}

		for j := 0; j < len(schemaStrs)-1; j++ {
			switch schemaStrs[j+1] {
			case "int":
				s.tables[i].valueSchema[j] = INTEGER
			case "string":
				s.tables[i].valueSchema[j] = STRING
			case "float":
				s.tables[i].valueSchema[j] = FLOAT
			default:
				clog.Error("Schema File %s Wrong Value Type %s", schema, schemaStrs[j+1])
			}
		}

		if isPhysical {
			s.tables[i].data = make([]*Partition, nParts)
			for j := 0; j < nParts; j++ {
				part := &Partition{
					rows: make(map[Key]Record),
				}
				s.tables[i].data[j] = part
			}
		} else {
			s.tables[i].data = make([]*Partition, 1)
			part := &Partition{
				rows: make(map[Key]Record),
			}
			s.tables[i].data[0] = part
		}

	}
	return s
}

func (s *Store) CreateRecByName(tableName string, k Key, partNum int, tuple Tuple) (Record, error) {
	if partNum >= s.nParts {
		clog.Error("Partition Number %v Out of Index", partNum)
	}

	tableID, ok1 := s.tableToIndex[tableName]
	if !ok1 {
		clog.Error("Table %s, Not Recognized \n", tableName)
	}

	return s.CreateRecByID(tableID, k, partNum, tuple)
}

func (s *Store) CreateRecByID(tableID int, k Key, partNum int, tuple Tuple) (Record, error) {
	table := s.tables[tableID]
	return table.CreateRecByID(k, partNum, tuple)
}

func (s *Store) GetRecByID(tableID int, k Key, partNum int) (Record, error) {
	table := s.tables[tableID]
	return table.GetRecByID(k, partNum)
}

func (s *Store) GetValueByName(tableName string, k Key, partNum int, val Value, colNum int) error {
	if partNum >= s.nParts {
		clog.Error("Partition Number %v Out of Index", partNum)
	}

	tableID, ok1 := s.tableToIndex[tableName]
	if !ok1 {
		clog.Error("Table %s, Not Exist \n", tableName)
	}

	return s.GetValueByID(tableID, k, partNum, val, colNum)
}

func (s *Store) GetValueByID(tableID int, k Key, partNum int, val Value, colNum int) error {
	table := s.tables[tableID]
	return table.GetValueByID(k, partNum, val, colNum)
}

func (s *Store) SetValueByName(tableName string, k Key, partNum int, value Value, colNum int) error {
	if partNum >= s.nParts {
		clog.Error("Partition Number %v Out of Index", partNum)
	}

	tableID, ok1 := s.tableToIndex[tableName]
	if !ok1 {
		clog.Error("Table %s, Not Recognized \n", tableName)
	}

	return s.SetValueByID(tableID, k, partNum, value, colNum)
}

func (s *Store) SetValueByID(tableID int, k Key, partNum int, value Value, colNum int) error {
	table := s.tables[tableID]
	return table.SetValueByID(k, partNum, value, colNum)
}

func (s *Store) PrepareDelete(tableID int, k Key, partNum int) (Record, error) {
	table := s.tables[tableID]
	return table.PrepareDelete(k, partNum)
}

func (s *Store) DeleteRecord(tableID int, k Key, partNum int) error {
	table := s.tables[tableID]
	return table.DeleteRecord(k, partNum)
}

func (s *Store) PrepareInsert(tableID int, k Key, partNum int) error {
	table := s.tables[tableID]
	return table.PrepareInsert(k, partNum)
}

func (s *Store) InsertRecord(tableID int, k Key, partNum int, rec Record) error {
	table := s.tables[tableID]
	return table.InsertRecord(k, partNum, rec)
}

type Table interface {
	CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error)
	GetRecByID(k Key, partNum int) (Record, error)
	SetValueByID(k Key, partNum int, value Value, colNum int) error
	GetValueByID(k Key, partNum int, val Value, colNum int) error
	ReleaseDelete(k Key, partNum int)
	PrepareDelete(k Key, partNum int) (Record, error)
	DeleteRecord(k Key, partNum int) error
	ReleaseInsert(k Key, partNum int)
	PrepareInsert(k Key, partNum int) error
	InsertRecord(k Key, partNum int, rec Record) error
	GetValueBySec(k Key, partNum int, val Value) error
}

type Chunk struct {
	padding1 [PADDING]byte
	rows     map[Key]Record
	padding2 [PADDING]byte
}

type Partition struct {
	padding1 [PADDING]byte
	rows     map[Key]Record
	padding2 [PADDING]byte
}

type RWMutex struct {
	padding1 [PADDING]byte
	sync.RWMutex
	padding2 [PADDING]byte
}

type SpinLockPad struct {
	padding1 [PADDING]byte
	spinlock.Spinlock
	padding2 [PADDING]byte
}

type WFMuTexPAD struct {
	padding1 [PADDING]byte
	lock     wfmutex.WFMutex
	padding2 [PADDING]byte
}

type WDRWSpinlockPAD struct {
	padding1 [PADDING]byte
	lock     spinlockopt.WDRWSpinlock
	padding2 [PADDING]byte
}

type BasicTable struct {
	padding1    [PADDING]byte
	data        []*Partition
	valueSchema []BTYPE
	nKeys       int64
	name        string
	isPhysical  bool
	padding2    [PADDING]byte
}

func (bt *BasicTable) CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error) {
	bt.nKeys++

	var part *Partition
	if bt.isPhysical {
		part = table.data[partNum]
	} else {
		part = table.data[0]
	}

	if _, ok := part.rows[k]; ok {
		return nil, EDUPKEY //One record with that key has existed;
	}

	r := MakeRecord(bt, k, tuple)
	//chunk.rows[k] = r
	part.rows[k] = r
	return r, nil
}

func (bt *BasicTable) GetRecByID(k Key, partNum int) (Record, error) {
	var part *Partition
	if bt.isPhysical {
		part = table.data[partNum]
	} else {
		part = table.data[0]
	}
	r, ok := part.rows[k]
	if !ok {
		return nil, ENOKEY
	} else {
		return r, nil
	}
}

func (bt *BasicTable) SetValueByID(k Key, partNum int, value Value, colNum int) error {

	var part *Partition
	if s.isPhysical {
		part = table.data[partNum]
	} else {
		part = table.data[0]
	}
	r, ok := part.rows[k]
	if !ok {
		return ENOKEY // No such record; Fail
	}

	r.SetValue(value, colNum)
	return nil
}

func (bt *BasicTable) GetValueByID(k Key, partNum int, val Value, colNum int) error {
	var part *Partition
	if bt.isPhysical {
		part = table.data[partNum]
	} else {
		part = table.data[0]
	}
	r, ok := part.rows[k]
	if !ok {
		return ENOKEY
	}
	r.GetValue(val, colNum)
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

func (bt *BasicTable) PrepareInsert(k Key, partNum int) error {
	clog.Error("Basic Table Not Support PrepareInsert")
	return nil, nil
}

func (bt *BasicTable) InsertRecord(k Key, partNum int, rec Record) error {
	clog.Error("Basic Table Not Support InsertRecord")
	return nil
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
