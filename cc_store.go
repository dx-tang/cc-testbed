package testbed

import (
	"errors"
	"flag"
	"sync"

	"github.com/totemtang/cc-testbed/spinlock"
)

const (
	CHUNKS = 256
)

const (
	PARTITION = iota
	OCC
	LOCKING
)

var (
	EABORT = errors.New("abort")
	ENOKEY = errors.New("no entry")
)

type TID int64
type Key int64
type Value interface{}

var NumPart = flag.Int("ncores", 2, "number of partitions; equals to the number of cores")
var SysType = flag.Int("sys", PARTITION, "System Type we will use")
var SpinLock = flag.Bool("spinlock", true, "Use spinlock or mutexlock")

type Chunk struct {
	padding1 [128]byte
	rows     map[Key]Record
	padding2 [128]byte
}

type Partition struct {
	padding1  [128]byte
	data      []*Chunk
	mutexLock sync.RWMutex
	spinLock  spinlock.RWSpinlock
	padding2  [128]byte
}

func (p *Partition) Lock() {
	if *SpinLock {
		p.spinLock.Lock()
	} else {
		p.mutexLock.Lock()
	}
}

func (p *Partition) Unlock() {
	if *SpinLock {
		p.spinLock.Unlock()
	} else {
		p.mutexLock.Unlock()
	}
}

type Store struct {
	padding1 [128]byte
	store    []*Partition
	nKeys    int64
	padding2 [128]byte
}

func NewStore() *Store {
	if *SysType != PARTITION {
		*NumPart = 1
	}
	s := &Store{
		store: make([]*Partition, *NumPart),
	}

	var bb1 byte

	for i := 0; i < *NumPart; i++ {
		part := &Partition{
			data: make([]*Chunk, CHUNKS),
		}
		for j := 0; j < CHUNKS; j++ {
			chunk := &Chunk{
				rows: make(map[Key]Record),
			}
			bb1 = byte(j)
			part.data[bb1] = chunk
		}
		s.store[i] = part
	}
	return s
}

func (s *Store) CreateKV(k Key, v Value, rt RecType, partNum int) Record {
	s.nKeys++
	chunk := s.store[partNum].data[byte(k)]
	if _, ok := chunk.rows[k]; ok {
		return nil // One record with that key has existed; return nil to notify this
	}

	r := MakeRecord(k, v, rt)
	chunk.rows[k] = r
	return r
}

func (s *Store) GetRecord(k Key, partNum int) Record {
	chunk := s.store[partNum].data[byte(k)]
	r, ok := chunk.rows[k]
	if !ok {
		return nil
	}
	return r
}

// Update
func (s *Store) SetRecord(k Key, val Value, partNum int) bool {
	chunk := s.store[partNum].data[byte(k)]
	r, ok := chunk.rows[k]
	if !ok {
		return false // No such record; Fail
	}
	return r.UpdateValue(val)
}
