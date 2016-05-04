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
	compKey    Key
}

func NewHashPartitioner(NParts int, keyRange []int) *HashPartitioner {
	hp := &HashPartitioner{
		NParts:     NParts,
		pKeysArray: make([]int, NParts),
		keyLen:     len(keyRange),
		keyRange:   make([]int, len(keyRange)),
	}

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
	return key[0] % hp.NParts
}

func (hp *HashPartitioner) GetPartKey(partIndex int, rank int) Key {
	var key Key

	if hp.NParts == 0 {
		hp.GetWholeKey(rank)
	}

	for i := len(hp.keyRange) - 1; i >= 0; i-- {
		key[i] = rank % hp.keyRange[i]
		rank = rank / hp.keyRange[i]
	}

	key[0] = key[0]*hp.NParts + partIndex

	//clog.Info("CompKey %v\n", hp.compKey)

	return key
}

func (hp *HashPartitioner) GetWholeKey(rank int) Key {
	var key Key
	key[0] = rank
	//return CKey(hp.compKey)
	return key
}

func (hp *HashPartitioner) GetKeyArray() []int {
	return hp.pKeysArray
}

func (hp *HashPartitioner) ResetPart(nParts int) {
	hp.NParts = nParts
}
