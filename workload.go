package testbed

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/totemtang/cc-testbed/clog"
)

const (
	SMALLBANK = iota
)

type Generator struct {
	keyGens     []KeyGen
	partitioner []Partitioner
	partIndex   int
	tableCount  int
}

func (g *Generator) GetKey(tableID int, partIndex int) Key {
	rank := g.keyGens[tableID].GetPartRank(partIndex)
	return g.partitioner[tableID].GetPartKey(partIndex, rank)
}

func (g *Generator) GetPart(tableID int, key Key) int {
	return g.partitioner[tableID].GetPart(key)
}

type BasicWorkload struct {
	store         *Store
	nParts        int
	isPartition   bool
	nWorkers      int
	nKeys         []int64 // index by table ID
	tableCount    int
	tableNames    []string // index by table ID
	tableNameToID map[string]int
	IDToKeyRange  [][]int64    // index by table ID
	generators    []*Generator // index by worker ID
}

func NewBasicWorkload(workload string, nParts int, isPartition bool, nWorkers int, s float64) *BasicWorkload {
	basic := &BasicWorkload{
		store:       NewStore(workload, nParts),
		nParts:      nParts,
		isPartition: isPartition,
		nWorkers:    nWorkers,
	}

	// Begin Populating Store
	// Open File
	wl, err := os.OpenFile(workload, os.O_RDONLY, 0600)
	if err != nil {
		clog.Error("Open File Error %s\n", err.Error())
	}
	defer wl.Close()
	wlReader := bufio.NewReader(wl)

	// Find Header KEYRANGE
	var tc []byte
	for {
		tc, err = wlReader.ReadBytes('\n')
		if err != nil {
			clog.Error("Read File %s Error, End of File\n", workload)
		}
		if strings.Compare(KEYRANGE, string(tc)) == 0 {
			break
		}
	}

	// Read Table Count
	tc, err = wlReader.ReadBytes('\n')
	if err != nil {
		clog.Error("Read File %s Error, No Data\n", workload)
	}
	tableCount, err1 := strconv.Atoi(string(tc[0 : len(tc)-1]))
	if err1 != nil {
		clog.Error("Workload File %s Wrong Format\n", workload)
	}

	basic.tableCount = tableCount
	basic.tableNames = make([]string, tableCount)
	basic.tableNameToID = make(map[string]int)
	basic.IDToKeyRange = make([][]int64, tableCount)
	basic.nKeys = make([]int64, tableCount)

	// Read Key Range
	var line []byte
	for i := 0; i < tableCount; i++ {
		line, err = wlReader.ReadBytes('\n')
		if err != nil {
			clog.Error("Workload File %s Wrong Format at Line %d\n", workload, i+1)
		}
		krStrs := strings.Split(string(line[:len(line)-1]), ":")
		if len(krStrs) < 2 {
			clog.Error("Workload File %s Wrong Format at Line %d\n", workload, i+1)
		}

		// Insert Table Name
		basic.tableNames[i] = krStrs[0]
		basic.tableNameToID[krStrs[0]] = i

		var nk int64 = 1
		var kr int
		keyRange := make([]int64, len(krStrs)-1)
		for j := 0; j < len(keyRange); j++ {
			kr, err = strconv.Atoi(krStrs[j+1])
			if err != nil {
				clog.Error("Workload File %s Wrong Format at Line %d\n", workload, i+1)
			}
			keyRange[j] = int64(kr)
			nk *= keyRange[j]
		}

		basic.nKeys[i] = nk
		basic.IDToKeyRange[i] = keyRange
	}

	basic.generators = make([]*Generator, basic.nWorkers)

	for i := 0; i < basic.nWorkers; i++ {
		gen := &Generator{
			tableCount:  tableCount,
			keyGens:     make([]KeyGen, tableCount),
			partitioner: make([]Partitioner, tableCount),
		}

		if basic.isPartition {
			gen.partIndex = i
		} else {
			gen.partIndex = 0
		}

		for j := 0; j < tableCount; j++ {
			p := NewHashPartitioner(int64(basic.nParts), basic.IDToKeyRange[j])
			var kg KeyGen
			if s == 1 {
				kg = NewUniformRand(i, basic.nKeys[j], basic.nParts, p.GetKeyArray(), basic.isPartition)
			} else if s > 1 {
				kg = NewZipfRand(i, basic.nKeys[j], basic.nParts, p.GetKeyArray(), s, basic.isPartition)
			} else {
				kg = NewHotColdRand(i, basic.nKeys[j], basic.nParts, p.GetKeyArray(), -s, basic.isPartition)
			}
			gen.keyGens[j] = kg
			gen.partitioner[j] = p
		}
		basic.generators[i] = gen
	}

	return basic
}
