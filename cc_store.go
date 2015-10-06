package testbed

import (
	"flag"
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

type TID int64
type Key [16]byte
type Value interface{}

var NumPart = flag.Int("ncores", 1, "number of partitions; equals to the number of cores")
var SysType = flag.Int("sys", PARTITION, "System Type we will use")

type Chunk struct {
	padding1 [128]byte
	rows     map[Key]*BRecord
	padding2 [128]byte
}

type Partition struct {
	padding1 [128]byte
	spinlock.Spinlock
	data     []*Chunk
	padding2 [128]byte
}

type Store struct {
	padding1 [128]byte
	store    []*Partition
	padding2 [128]byte
}

func NewStore() *Store {
	return nil
}
