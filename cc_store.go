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

	// Error for Smallbank
	ELACKBALANCE = errors.New("Checking Balance Not Enough")
	ENEGSAVINGS  = errors.New("Negative Saving Balance")
)

type BTYPE int // Basic types: int64, float64, string
type TID uint64

var NumPart = flag.Int("ncores", 2, "number of partitions; equals to the number of cores")
var SysType = flag.Int("sys", PARTITION, "System Type we will use")
var SpinLock = flag.Bool("spinlock", true, "Use spinlock or mutexlock")

type Chunk struct {
	padding1 [PADDING]byte
	rows     map[Key]Record
	padding2 [PADDING]byte
}

/*type Partition struct {
	padding1 [PADDING]byte
	partData []*Chunk
	padding2 [PADDING]byte
}*/

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

type Table struct {
	padding1    [PADDING]byte
	data        []*Partition
	valueSchema []BTYPE
	nKeys       int64
	name        string
	padding2    [PADDING]byte
}

type Store struct {
	padding1     [PADDING]byte
	tables       []*Table
	spinLock     []*SpinLockPad
	wfLock       []*WFMuTexPAD
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
		mutexLock:    make([]*RWMutex, nParts),
		spinLock:     make([]*SpinLockPad, nParts),
		nParts:       nParts,
		isPhysical:   isPhysical,
	}

	for i := 0; i < nParts; i++ {
		s.wfLock[i] = &WFMuTexPAD{}
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
	table.nKeys++

	//chunk := table.data[partNum].partData[k[0]]
	var part *Partition
	if s.isPhysical {
		part = table.data[partNum]
	} else {
		part = table.data[0]
	}
	/*if _, ok := chunk.rows[k]; ok {
		return nil, EDUPKEY //One record with that key has existed;
	}*/

	if _, ok := part.rows[k]; ok {
		return nil, EDUPKEY //One record with that key has existed;
	}

	r := MakeRecord(table, k, tuple)
	//chunk.rows[k] = r
	part.rows[k] = r
	return r, nil
}

func (s *Store) GetValueByID(tableID int, k Key, partNum int, colNum int) Value {
	table := s.tables[tableID]

	//chunk := table.data[partNum].partData[k[0]]
	//r, ok := chunk.rows[k]
	var part *Partition
	if s.isPhysical {
		part = table.data[partNum]
	} else {
		part = table.data[0]
	}
	r, ok := part.rows[k]
	if !ok {
		return nil
	}
	return r.GetValue(colNum)
}

func (s *Store) GetRecByID(tableID int, k Key, partNum int) Record {
	table := s.tables[tableID]

	//chunk := table.data[partNum].partData[k[0]]
	//r, ok := chunk.rows[k]
	//if s.nParts == 1 {
	//	clog.Info("ID %v, Key %v, PartNum %v, Part %v", tableID, k, partNum, len(table.data))
	//}
	var part *Partition
	if s.isPhysical {
		part = table.data[partNum]
	} else {
		part = table.data[0]
	}
	r, ok := part.rows[k]
	if !ok {
		return nil
	}

	return r
}

func (s *Store) GetValueByName(tableName string, k Key, partNum int, colNum int) Value {
	if partNum >= s.nParts {
		clog.Error("Partition Number %v Out of Index", partNum)
	}

	tableID, ok1 := s.tableToIndex[tableName]
	if !ok1 {
		clog.Error("Table %s, Not Exist \n", tableName)
	}

	return s.GetValueByID(tableID, k, partNum, colNum)
}

func (s *Store) SetValueByID(tableID int, k Key, partNum int, value Value, colNum int) bool {
	table := s.tables[tableID]

	if colNum >= len(table.valueSchema) {
		clog.Error("Column Number %v Out of Index of %s", colNum, tableID)
	}

	if !checkType(value, table.valueSchema[colNum]) {
		clog.Error("Column Type Not Match: Input %v, Require %v \n", value, table.valueSchema[colNum])
	}

	//chunk := table.data[partNum].partData[k[0]]
	//r, ok := chunk.rows[k]
	var part *Partition
	if s.isPhysical {
		part = table.data[partNum]
	} else {
		part = table.data[0]
	}
	r, ok := part.rows[k]
	if !ok {
		return false // No such record; Fail
	}

	r.SetValue(value, colNum)
	return true
}

// Update
func (s *Store) SetValueByName(tableName string, k Key, partNum int, value Value, colNum int) bool {
	if partNum >= s.nParts {
		clog.Error("Partition Number %v Out of Index", partNum)
	}

	tableID, ok1 := s.tableToIndex[tableName]
	if !ok1 {
		clog.Error("Table %s, Not Recognized \n", tableName)
	}

	return s.SetValueByID(tableID, k, partNum, value, colNum)
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
