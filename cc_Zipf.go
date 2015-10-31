package testbed

import (
	"math/rand"
	"time"

	"github.com/totemtang/cc-testbed/clog"
)

type ZipfKey struct {
	partIndex    int
	nParts       int
	nKeys        int64
	pKeysArray   []int64
	isZipf       bool
	isPartition  bool
	hp           Partitioner
	wholeZipf    *rand.Zipf
	partZipf     []*rand.Zipf
	wholeUniform *rand.Rand
	partUniform  []*rand.Rand
}

// Index of partition starts from 0
// Integer Key starts from 0 also
func NewZipfKey(partIndex int, nKeys int64, nParts int, pKeysArray []int64, s float64, hp Partitioner) *ZipfKey {

	zk := &ZipfKey{
		partIndex:  partIndex,
		nParts:     nParts,
		nKeys:      nKeys,
		pKeysArray: pKeysArray,
		hp:         hp,
	}

	zk.isPartition = *SysType == PARTITION

	zk.wholeUniform = rand.New(rand.NewSource(time.Now().Unix()))

	if zk.isPartition {
		zk.partUniform = make([]*rand.Rand, nParts)
		for i := 0; i < nParts; i++ {
			zk.partUniform[i] = rand.New(rand.NewSource(time.Now().Unix()))
		}
	}

	// Uniform distribution
	if s == 1 {
		zk.isZipf = false
	} else {
		zk.isZipf = true

		// Generate Zipf for whole store
		zk.wholeZipf = rand.NewZipf(zk.wholeUniform, s, 1, uint64(nKeys-1))

		if zk.isPartition {
			// Generate Zipf for for each part
			zk.partZipf = make([]*rand.Zipf, nParts)
			for i := 0; i < nParts; i++ {
				zk.partZipf[i] = rand.NewZipf(zk.partUniform[i], s, 1, uint64(pKeysArray[i]-1))
			}
		}
	}

	return zk
}

func (zk *ZipfKey) GetKey() Key {
	if zk.isZipf {
		return Key(zk.wholeZipf.Uint64())
	} else {
		return Key(zk.wholeUniform.Int63n(zk.nKeys))
	}
}

func (zk *ZipfKey) GetSelfKey() Key {
	if !zk.isPartition {
		clog.Error("Should not be invoked for non-partition CC")
	}

	pi := zk.partIndex

	if zk.isZipf {
		rank := int64(zk.partZipf[pi].Uint64())
		return zk.hp.GetKey(pi, int64(rank))
	} else {
		return zk.hp.GetKey(pi, zk.partUniform[pi].Int63n(zk.pKeysArray[pi]))
	}
}

func (zk *ZipfKey) GetOtherKey(pi int) Key {
	if !zk.isPartition {
		clog.Error("Should not be invoked for non-partition CC")
	}

	if zk.isZipf {
		rank := int64(zk.partZipf[pi].Uint64())
		return zk.hp.GetKey(pi, int64(rank))
	} else {
		return zk.hp.GetKey(pi, zk.partUniform[pi].Int63n(zk.pKeysArray[pi]))
	}
}
