package testbed

import (
//"github.com/totemtang/cc-testbed/clog"
)

type Partitioner interface {
	GetPart(key Key) int
	GetPartKey(partIndex int, rank int) Key
	GetWholeKey(rank int) Key
	GetKeyArray() []int
	ResetPart(nParts int)
}

type HashPartitioner struct {
	NParts     int
	pKeysArray []int
	keyLen     int
	keyRange   []int
	compKey    []int
}

func NewHashPartitioner(NParts int, keyRange []int) *HashPartitioner {
	hp := &HashPartitioner{
		NParts:     NParts,
		pKeysArray: make([]int, NParts),
		keyLen:     len(keyRange),
		keyRange:   make([]int, len(keyRange)),
		compKey:    make([]int, len(keyRange)+2*PADDINGINT),
	}
	hp.compKey = hp.compKey[PADDINGINT : hp.keyLen+PADDINGINT]

	var unit int = 1

	for i, v := range keyRange {
		hp.keyRange[i] = v
		if i != 0 {
			unit *= v
		}
	}

	r := hp.keyRange[0] / hp.NParts
	m := hp.keyRange[0] % hp.NParts

	for i := 0; i < hp.NParts; i++ {
		hp.pKeysArray[i] = r * unit
		if i < m {
			hp.pKeysArray[i] += unit
		}
	}

	return hp
}

func (hp *HashPartitioner) GetPart(key Key) int {
	k := ParseKey(key, 0)
	return k % hp.NParts
}

func (hp *HashPartitioner) GetPartKey(partIndex int, rank int) Key {
	if hp.NParts == 0 {
		hp.GetWholeKey(rank)
	}

	for i := len(hp.keyRange) - 1; i >= 0; i-- {
		hp.compKey[i] = rank % hp.keyRange[i]
		rank = rank / hp.keyRange[i]
	}

	hp.compKey[0] = hp.compKey[0]*hp.NParts + partIndex

	//clog.Info("CompKey %v\n", hp.compKey)

	return CKey(hp.compKey)
}

func (hp *HashPartitioner) GetWholeKey(rank int) Key {
	hp.compKey[0] = rank
	return CKey(hp.compKey)
}

func (hp *HashPartitioner) GetKeyArray() []int {
	return hp.pKeysArray
}

func (hp *HashPartitioner) ResetPart(nParts int) {
	hp.NParts = nParts
}
