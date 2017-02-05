package testbed

import (
	"bufio"
	"errors"
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
	MEDIATED

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

type Bucket interface{}

type Store struct {
	padding1     [PADDING]byte
	priTables    []Table
	secTables    []Table
	backTables   []Table
	state        int32
	spinLock     []SpinLockPad
	wfLock       []WFMuTexPad
	confLock     []NoWaitLockPad
	mutexLock    []RWMutexPad
	tableToIndex map[string]int
	nParts       int
	isPartition  bool
	double       bool
	padding2     [PADDING]byte
}

func NewStore(schema string, tableSize []int, nParts int, isPartition bool, mode int, double bool, useLatch []bool) *Store {

	if nParts == 1 {
		isPartition = false
	}

	// var useLatch bool
	// if mode == PARTITION {
	// 	useLatch = false
	// } else {
	// 	useLatch = true
	// }

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
		backTables:   make([]Table, tableCount),
		tableToIndex: make(map[string]int),
		wfLock:       make([]WFMuTexPad, *NumPart),
		confLock:     make([]NoWaitLockPad, *NumPart),
		mutexLock:    make([]RWMutexPad, *NumPart),
		spinLock:     make([]SpinLockPad, *NumPart),
		nParts:       nParts,
		isPartition:  isPartition,
		double:       double,
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
			s.priTables[i] = MakeNewOrderTable(*NumPart, isPartition, useLatch)
			s.secTables[i] = MakeNewOrderTable(*NumPart, !isPartition, useLatch)
			s.backTables[i] = MakeNewOrderTable(*NumPart, isPartition, useLatch)
			clog.Info("Making NewOrder %.2f", time.Since(start).Seconds())
		} else if strings.Compare(schemaStrs[0], "ORDER") == 0 {
			start := time.Now()
			s.priTables[i] = MakeOrderTable(nParts, *NumPart, isPartition, useLatch)
			if isPartition {
				s.secTables[i] = MakeOrderTable(1, *NumPart, !isPartition, useLatch)
			} else {
				s.secTables[i] = MakeOrderTable(*NumPart, *NumPart, !isPartition, useLatch)
			}
			s.backTables[i] = MakeOrderTable(nParts, *NumPart, isPartition, useLatch)
			clog.Info("Making Order %.2f", time.Since(start).Seconds())
		} else if strings.Compare(schemaStrs[0], "CUSTOMER") == 0 {
			start := time.Now()
			if isPartition {
				s.priTables[i] = MakeCustomerTable(CUSTOMERSIZE_PER_WAREHOUSE, nParts, *NumPart, isPartition, useLatch)
				s.secTables[i] = MakeCustomerTable(CUSTOMERSIZE_PER_WAREHOUSE*(*NumPart), 1, *NumPart, !isPartition, useLatch)
				s.backTables[i] = MakeCustomerTable(CUSTOMERSIZE_PER_WAREHOUSE, nParts, *NumPart, isPartition, useLatch)
			} else {
				s.priTables[i] = MakeCustomerTable(CUSTOMERSIZE_PER_WAREHOUSE*(*NumPart), nParts, *NumPart, isPartition, useLatch)
				s.secTables[i] = MakeCustomerTable(CUSTOMERSIZE_PER_WAREHOUSE, *NumPart, *NumPart, !isPartition, useLatch)
				s.backTables[i] = MakeCustomerTable(CUSTOMERSIZE_PER_WAREHOUSE*(*NumPart), nParts, *NumPart, isPartition, useLatch)
			}
			clog.Info("Making Customer %.2f", time.Since(start).Seconds())
		} else if strings.Compare(schemaStrs[0], "HISTORY") == 0 {
			start := time.Now()
			s.priTables[i] = MakeHistoryTable(nParts, *NumPart, isPartition, useLatch)
			s.secTables[i] = s.priTables[i]
			s.backTables[i] = MakeHistoryTable(nParts, *NumPart, isPartition, useLatch)
			clog.Info("Making History %.2f", time.Since(start).Seconds())
		} else if strings.Compare(schemaStrs[0], "ORDERLINE") == 0 {
			start := time.Now()
			s.priTables[i] = MakeOrderLineTable(nParts, *NumPart, isPartition, useLatch)
			if isPartition {
				s.secTables[i] = MakeOrderLineTable(1, *NumPart, !isPartition, useLatch)
			} else {
				s.secTables[i] = MakeOrderLineTable(*NumPart, *NumPart, !isPartition, useLatch)
			}
			s.backTables[i] = MakeOrderLineTable(nParts, *NumPart, isPartition, useLatch)
			clog.Info("Making OrderLine %.2f", time.Since(start).Seconds())
		} else if strings.Compare(schemaStrs[0], "ITEM") == 0 {
			start := time.Now()
			s.priTables[i] = NewBasicTable(ITEMSIZE, 1, false, useLatch, ITEM)
			s.secTables[i] = s.priTables[i]
			s.backTables[i] = s.priTables[i]
			clog.Info("Making ITEM %.2f", time.Since(start).Seconds())
		} else {
			start := time.Now()
			if isPartition {
				s.priTables[i] = NewBasicTable(tableSize[i]/(*NumPart), nParts, isPartition, useLatch, i)
				s.secTables[i] = NewBasicTable(tableSize[i], 1, !isPartition, useLatch, i)
				s.backTables[i] = NewBasicTable(tableSize[i]/(*NumPart), nParts, isPartition, useLatch, i)
			} else {
				s.priTables[i] = NewBasicTable(tableSize[i], nParts, isPartition, useLatch, i)
				s.secTables[i] = NewBasicTable(tableSize[i]/(*NumPart), *NumPart, !isPartition, useLatch, i)
				s.backTables[i] = NewBasicTable(tableSize[i], nParts, isPartition, useLatch, i)
			}
			clog.Info("Making BasicTable %.2f", time.Since(start).Seconds())
		}

	}
	return s
}

func (s *Store) CreateRecByID(tableID int, k Key, partNum int, tuple Tuple, ia IndexAlloc) (Record, error) {
	table := s.priTables[tableID]
	rec, err := table.CreateRecByID(k, partNum, tuple, ia)
	if err != nil {
		return nil, err
	}
	if s.double {
		if WLTYPE == TPCCWL && (tableID == ITEM || tableID == HISTORY) {
			return rec, err
		}
		_, err1 := s.secTables[tableID].CreateRecByID(k, partNum, tuple, ia)
		if err1 != nil {
			return nil, err1
		}
	}
	return rec, err
}

func (s *Store) GetRecByID(tableID int, k Key, partNum int) (Record, Bucket, uint64, error) {
	if s.state == INDEX_NONE {
		table := s.priTables[tableID]
		return table.GetRecByID(k, partNum)
	} else { // INDEX_CHANGING
		table := s.secTables[tableID]
		rec, _, _, err := table.GetRecByID(k, partNum)
		if err == ENOKEY {
			return s.priTables[tableID].GetRecByID(k, partNum)
		}
		return rec, nil, 0, err
	}
}

func (s *Store) PrepareDelete(tableID int, k Key, partNum int) (Record, error) {
	table := s.priTables[tableID]
	return table.PrepareDelete(k, partNum)
	//clog.Error("Delete Not Support Yet")
	//return nil, nil
}

func (s *Store) DeleteRecord(tableID int, k Key, partNum int) error {
	table := s.priTables[tableID]
	return table.DeleteRecord(k, partNum)
	//clog.Error("Delete Not Support Yet")
	//return nil
}

func (s *Store) ReleaseDelete(tableID int, k Key, partNum int) {
	table := s.priTables[tableID]
	table.ReleaseDelete(k, partNum)
	//clog.Error("Delete Not Support Yet")
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

func (s *Store) SetLatch(useLatch bool) {
	for i, t := range s.priTables {
		t.SetLatch(useLatch)
		s.secTables[i].SetLatch(useLatch)
	}
}

func (s *Store) GetTables() []Table {
	return s.priTables
}

func (s *Store) IndexPartition(iaAR []IndexAlloc, begin int, end int, partitioner []Partitioner) {
	for j := 0; j < len(s.priTables); j++ {
		if WLTYPE == TPCCWL {
			if j != ITEM && j != HISTORY {
				s.secTables[j].BulkLoad(s.priTables[j], iaAR[j], begin, end, nil)
			}
		} else {
			s.secTables[j].BulkLoad(s.priTables[j], nil, begin, end, partitioner[j])
		}
	}
}

func (s *Store) IndexMerge(iaAR []IndexAlloc, begin int, end int, partitioner []Partitioner) {
	for j := 0; j < len(s.priTables); j++ {
		if WLTYPE == TPCCWL {
			if j != ITEM && j != HISTORY {
				s.secTables[j].MergeLoad(s.priTables[j], iaAR[j], begin, end, nil)
			}
		} else {
			s.secTables[j].MergeLoad(s.priTables[j], nil, begin, end, partitioner[j])
		}
	}
}

func (s *Store) Switch() {
	if !s.double {
		clog.Error("No Double Table, Cannot Switch")
	}
	tmp := s.priTables
	s.priTables = s.secTables
	s.secTables = tmp
}
