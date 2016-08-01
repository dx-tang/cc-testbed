package testbed

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/totemtang/cc-testbed/clog"
)

var (
	WLTYPE int
)

const (
	NOPARTSKEW = 0
)

const (
	SINGLEWL = iota
	SMALLBANKWL
	TPCCWL
)

const (
	QUEUESIZE = 10
)

type Generator struct {
	keyGens     []KeyGen
	partitioner []Partitioner // Index by Table
	partGen     PartGen
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

func (g *Generator) GenOnePart() int {
	return g.partGen.GetOnePart()
}

type BasicWorkload struct {
	store         *Store
	nParts        int
	isPartition   bool
	nWorkers      int
	nKeys         []int // index by table ID
	tableCount    int
	tableNames    []string // index by table ID
	tableNameToID map[string]int
	IDToKeyRange  [][]int      // index by table ID
	generators    []*Generator // index by worker ID
}

func NewBasicWorkload(workload string, nParts int, isPartition bool, nWorkers int, s float64, ps float64, initMode int, double bool) *BasicWorkload {
	basic := &BasicWorkload{
		nParts:      *NumPart,
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
	basic.IDToKeyRange = make([][]int, tableCount)
	basic.nKeys = make([]int, tableCount)

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

		var nk int = 1
		var kr int
		keyRange := make([]int, len(krStrs)-1)
		for j := 0; j < len(keyRange); j++ {
			kr, err = strconv.Atoi(krStrs[j+1])
			if err != nil {
				clog.Error("Workload File %s Wrong Format at Line %d\n", workload, i+1)
			}
			keyRange[j] = kr
			nk *= keyRange[j]
		}

		basic.nKeys[i] = nk
		basic.IDToKeyRange[i] = keyRange
	}

	basic.NewGenerators(s, ps)

	basic.store = NewStore(workload, basic.nKeys, nParts, isPartition, initMode, double)

	return basic
}

func (basic *BasicWorkload) SetKeyGen(keyGens [][]KeyGen) {
	if len(basic.generators) != len(keyGens) {
		clog.Error("Key Generators Length not Match\n")
	}
	for i := 0; i < basic.nWorkers; i++ {
		for j := 0; j < len(basic.generators[i].keyGens); j++ {
			basic.generators[i].keyGens[j] = keyGens[i][j]
		}
	}
}

func (basic *BasicWorkload) SetPartGen(partGens []PartGen) {
	if len(basic.generators) != len(partGens) {
		clog.Error("Key Generators Length not Match\n")
	}
	for i := 0; i < basic.nWorkers; i++ {
		basic.generators[i].partGen = partGens[i]
	}
}

func (basicWL *BasicWorkload) GetGenerator() []*Generator {
	return basicWL.GetGenerator()
}

func (basic *BasicWorkload) NewGenerators(s float64, ps float64) []*Generator {

	basic.generators = make([]*Generator, basic.nWorkers)
	tableCount := basic.tableCount

	for i := 0; i < basic.nWorkers; i++ {
		gen := &Generator{
			tableCount:  tableCount,
			keyGens:     make([]KeyGen, tableCount),
			partitioner: make([]Partitioner, tableCount),
		}

		gen.partIndex = i

		for j := 0; j < tableCount; j++ {
			p := NewHashPartitioner(*NumPart, basic.IDToKeyRange[j])
			gen.partitioner[j] = p
		}

		basic.generators[i] = gen
	}

	keyGens := basic.NewKeyGen(s)
	partGens := basic.NewPartGen(ps)
	for i := 0; i < basic.nWorkers; i++ {
		basic.generators[i].keyGens = keyGens[i]
		basic.generators[i].partGen = partGens[i]
	}

	return basic.generators

}

func (basic *BasicWorkload) NewKeyGen(s float64) [][]KeyGen {
	keyGen := make([][]KeyGen, basic.nWorkers)
	tableCount := basic.tableCount

	for i := 0; i < basic.nWorkers; i++ {
		keyGen[i] = make([]KeyGen, tableCount)

		for j := 0; j < tableCount; j++ {
			p := basic.generators[i].partitioner[j]
			var kg KeyGen
			if s == 1 {
				kg = NewUniformRand(i, basic.nKeys[j], *NumPart, p.GetKeyArray(), basic.isPartition)
			} else if s > 1 {
				kg = NewZipfRandLarge(i, basic.nKeys[j], *NumPart, p.GetKeyArray(), s, basic.isPartition)
			} else if s < 0 {
				kg = NewHotColdRand(i, basic.nKeys[j], *NumPart, p.GetKeyArray(), -s, basic.isPartition)
			} else { // s >=0 && s < 1
				kg = NewZipfRandSmall(i, basic.nKeys[j], *NumPart, p.GetKeyArray(), s, basic.isPartition)
			}
			keyGen[i][j] = kg
		}
	}

	return keyGen
}

func (basic *BasicWorkload) NewPartGen(ps float64) []PartGen {
	partGen := make([]PartGen, basic.nWorkers)

	for i := 0; i < basic.nWorkers; i++ {
		if ps == 1 {
			partGen[i] = NewUniformRandPart(ps, i, *NumPart)
		} else if ps > 1 {
			partGen[i] = NewZipfRandLargePart(ps, i, *NumPart)
		} else if ps >= 0 {
			partGen[i] = NewZipfRandSmallPart(ps, i, *NumPart)
		} else {
			clog.Error("Part Access Generation: Skew Factor Should Not Be Negative\n")
		}
	}

	return partGen
}

func (basic *BasicWorkload) ResetPart(nParts int, isPartition bool) {
	//basic.nParts = nParts
	basic.isPartition = isPartition
	/*for i, gen := range basic.generators {
		if isPartition {
			gen.partIndex = i
		} else {
			gen.partIndex = 0
		}
		for i, keyGen := range gen.keyGens {
			keyGen.ResetPart(isPartition)
			gen.partitioner[i].ResetPart(nParts)
		}
	}
	basic.store.nParts = nParts*/
}
