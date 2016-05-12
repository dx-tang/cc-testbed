package testbed

import (
	"bufio"
	"errors"
	"flag"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/nowaitlock"
	"github.com/totemtang/cc-testbed/spinlock"
	"github.com/totemtang/cc-testbed/wfmutex"
)

const (
	INDEX_NONE = iota
	INDEX_CHANGING
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
	PADDINGBOOL  = 64
)

const (
	INTEGER = iota // int
	FLOAT          // floate32
	STRING         // string
	DATE           // date
)

var (
	EABORT  = errors.New("abort")
	ENOKEY  = errors.New("no entry")
	EDUPKEY = errors.New("key has existed")
	ENODEL  = errors.New("No Delete Key")

	// Error for Smallbank
	ELACKBALANCE = errors.New("Checking Balance Not Enough")
	ENEGSAVINGS  = errors.New("Negative Saving Balance")

	// Error for TPCC
	ENOORDER = errors.New("No Order For This Customer")
)

type BTYPE int // Basic types: int64, float64, string
type TID uint64

var NumPart = flag.Int("ncores", 2, "number of partitions; equals to the number of cores")
var SysType = flag.Int("sys", PARTITION, "System Type we will use")
var SpinLock = flag.Bool("spinlock", true, "Use spinlock or mutexlock")
var NoWait = flag.Bool("nw", true, "Use Waitdie or NoWait for 2PL")

type RWMutexPad struct {
	padding1 [PADDING]byte
	sync.RWMutex
	padding2 [PADDING]byte
}

type SpinLockPad struct {
	padding1 [PADDING]byte
	spinlock.Spinlock
	padding2 [PADDING]byte
}

type RWSpinLockPad struct {
	padding1 [PADDING]byte
	spinlock.RWSpinlock
	padding2 [PADDING]byte
}

type WFMuTexPad struct {
	padding1 [PADDING]byte
	lock     wfmutex.WFMutex
	padding2 [PADDING]byte
}

type NoWaitLockPad struct {
	padding1 [PADDING]byte
	lock     nowaitlock.NoWaitLock
	padding2 [PADDING]byte
}

type Store struct {
	padding1     [PADDING]byte
	priTables    []Table
	secTables    []Table
	state        int
	spinLock     []SpinLockPad
	wfLock       []WFMuTexPad
	confLock     []NoWaitLockPad
	mutexLock    []RWMutexPad
	tableToIndex map[string]int
	nParts       int
	mode         int
	padding2     [PADDING]byte
}

func NewStore(schema string, nParts int, isPartition bool, mode int) *Store {

	if nParts == 1 {
		isPartition = false
	}

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
		priTables:    make([]Table, tableCount),
		secTables:    make([]Table, tableCount),
		tableToIndex: make(map[string]int),
		wfLock:       make([]WFMuTexPad, nParts),
		confLock:     make([]NoWaitLockPad, nParts),
		mutexLock:    make([]RWMutexPad, nParts),
		spinLock:     make([]SpinLockPad, nParts),
		nParts:       nParts,
		mode:         mode,
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

		if strings.Compare(schemaStrs[0], "NEWORDER") == 0 {
			start := time.Now()
			s.priTables[i] = MakeNewOrderTable(*NumPart, isPartition, mode)
			s.secTables[i] = MakeNewOrderTable(*NumPart, !isPartition, mode)
			clog.Info("Making NewOrder %.2f", time.Since(start).Seconds())
		} else if strings.Compare(schemaStrs[0], "ORDER") == 0 {
			start := time.Now()
			s.priTables[i] = MakeOrderTable(nParts, *NumPart, isPartition, mode)
			s.secTables[i] = MakeOrderTable(*NumPart, *NumPart, !isPartition, mode)
			clog.Info("Making Order %.2f", time.Since(start).Seconds())
		} else if strings.Compare(schemaStrs[0], "CUSTOMER") == 0 {
			start := time.Now()
			s.priTables[i] = MakeCustomerTable(nParts, *NumPart, isPartition, mode)
			s.secTables[i] = MakeCustomerTable(*NumPart, *NumPart, !isPartition, mode)
			clog.Info("Making Customer %.2f", time.Since(start).Seconds())
		} else if strings.Compare(schemaStrs[0], "HISTORY") == 0 {
			start := time.Now()
			s.priTables[i] = MakeHistoryTable(nParts, *NumPart, isPartition, mode)
			s.secTables[i] = s.priTables[i]
			clog.Info("Making History %.2f", time.Since(start).Seconds())
		} else if strings.Compare(schemaStrs[0], "ORDERLINE") == 0 {
			start := time.Now()
			s.priTables[i] = MakeOrderLineTable(nParts, *NumPart, isPartition, mode)
			s.secTables[i] = MakeOrderLineTable(*NumPart, *NumPart, !isPartition, mode)
			clog.Info("Making OrderLine %.2f", time.Since(start).Seconds())
		} else if strings.Compare(schemaStrs[0], "ITEM") == 0 {
			start := time.Now()
			s.priTables[i] = NewBasicTable(schemaStrs, 1, false, mode, ITEM)
			s.secTables[i] = s.priTables[i]
			clog.Info("Making ITEM %.2f", time.Since(start).Seconds())
		} else {
			start := time.Now()
			s.priTables[i] = NewBasicTable(schemaStrs, nParts, isPartition, mode, i)
			s.secTables[i] = NewBasicTable(schemaStrs, *NumPart, !isPartition, mode, i)
			clog.Info("Making BasicTable %.2f", time.Since(start).Seconds())
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
	table := s.priTables[tableID]
	return table.CreateRecByID(k, partNum, tuple)
}

func (s *Store) GetRecByID(tableID int, k Key, partNum int) (Record, error) {
	if s.state == INDEX_NONE {
		table := s.priTables[tableID]
		return table.GetRecByID(k, partNum)
	} else { // INDEX_CHANGING
		table := s.secTables[tableID]
		rec, err := table.GetRecByID(k, partNum)
		if err == ENOKEY {
			return s.priTables[tableID].GetRecByID(k, partNum)
		}
		return rec, err
	}
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
	if s.state == INDEX_NONE {
		table := s.priTables[tableID]
		return table.GetValueByID(k, partNum, val, colNum)
	} else { // INDEX_CHANGING
		table := s.secTables[tableID]
		err := table.GetValueByID(k, partNum, val, colNum)
		if err == ENOKEY {
			return s.priTables[tableID].GetValueByID(k, partNum, val, colNum)
		}
		return err
	}
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
	//table := s.tables[tableID]
	//return table.SetValueByID(k, partNum, value, colNum)
	if s.state == INDEX_NONE {
		table := s.priTables[tableID]
		return table.SetValueByID(k, partNum, value, colNum)
	} else { // INDEX_CHANGING
		table := s.secTables[tableID]
		err := table.SetValueByID(k, partNum, value, colNum)
		if err == ENOKEY {
			return s.priTables[tableID].SetValueByID(k, partNum, value, colNum)
		}
		return err
	}
}

// Delete Not Supported
func (s *Store) PrepareDelete(tableID int, k Key, partNum int) (Record, error) {
	//table := s.priTables[tableID]
	//return table.PrepareDelete(k, partNum)
	clog.Error("Delete Not Support Yet")
	return nil, nil
}

func (s *Store) DeleteRecord(tableID int, k Key, partNum int) error {
	//table := s.priTables[tableID]
	//return table.DeleteRecord(k, partNum)
	clog.Error("Delete Not Support Yet")
	return nil
}

func (s *Store) ReleaseDelete(tableID int, k Key, partNum int) {
	//table := s.priTables[tableID]
	//table.ReleaseDelete(k, partNum)
	clog.Error("Delete Not Support Yet")
}

func (s *Store) PrepareInsert(tableID int, k Key, partNum int) error {
	table := s.priTables[tableID]
	return table.PrepareInsert(k, partNum)
}

func (s *Store) InsertRecord(tableID int, recs []InsertRec, ia IndexAlloc) error {
	table := s.priTables[tableID]
	return table.InsertRecord(recs, ia)
}

func (s *Store) ReleaseInsert(tableID int, k Key, partNum int) {
	table := s.priTables[tableID]
	table.ReleaseInsert(k, partNum)
}

func (s *Store) GetValueBySec(tableID int, k Key, partNum int, val Value) error {
	if s.state == INDEX_NONE {
		table := s.priTables[tableID]
		return table.GetValueBySec(k, partNum, val)
	} else { // INDEX_CHANGING
		table := s.secTables[tableID]
		err := table.GetValueBySec(k, partNum, val)
		if err == ENOKEY {
			return s.priTables[tableID].GetValueBySec(k, partNum, val)
		}
		return err
	}
}

func (s *Store) SetMode(mode int) {
	s.mode = mode
	for _, t := range s.priTables {
		t.SetMode(mode)
	}
}

func (s *Store) DeltaValueByID(tableID int, k Key, partNum int, value Value, colNum int) error {
	if s.state == INDEX_NONE {
		table := s.priTables[tableID]
		return table.DeltaValueByID(k, partNum, value, colNum)
	} else { // INDEX_CHANGING
		table := s.secTables[tableID]
		err := table.DeltaValueByID(k, partNum, value, colNum)
		if err == ENOKEY {
			return s.priTables[tableID].DeltaValueByID(k, partNum, value, colNum)
		}
		return err
	}
}

func (s *Store) GetTables() []Table {
	return s.priTables
}

func (s *Store) IndexPartition(iaAR []IndexAlloc, begin int, end int) {
	for j := 0; j < len(s.priTables); j++ {
		if j != ITEM && j != HISTORY {
			s.secTables[j].BulkLoad(s.priTables[j], iaAR[j], begin, end)
		}
	}
}
