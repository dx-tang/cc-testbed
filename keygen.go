package testbed

import (
	"math/rand"
	"time"

	//"github.com/totemtang/cc-testbed/clog"
)

type KeyGen interface {
	GetWholeRank() int
	GetPartRank(pi int) int
	ResetPart(isPartition bool)
}

type HotColdRand struct {
	partIndex     int
	nParts        int
	hotKeys       int
	coldKeys      int
	hcRnd         *rand.Rand
	wholeHotRd    *rand.Rand
	hotKeysArray  []int
	coldKeysArray []int
	partHotRd     []*rand.Rand
	accessRate    int
	isPartition   bool
}

// s represents the hot/cold data distribution and access rate.
// e.g. 2080 says first 20% data is hot and 80% accesses will touch them
func NewHotColdRand(partIndex int, nKeys int, nParts int, pKeysArray []int, s float64, isPartition bool) *HotColdRand {
	hcr := &HotColdRand{}
	hcr.accessRate = int(s) % 100

	//hotPercent := int(s) / 100

	hcr.hcRnd = rand.New(rand.NewSource(time.Now().UnixNano() / int64(partIndex+3)))

	//hcr.hotKeys = int64(hotPercent) * nKeys / 100
	hcr.hotKeys = int(s) / 100
	hcr.coldKeys = nKeys - hcr.hotKeys
	hcr.wholeHotRd = rand.New(rand.NewSource(time.Now().UnixNano() / int64(partIndex+1)))
	hcr.isPartition = isPartition

	perHotKeys := hcr.hotKeys / nParts
	numExtraHot := int(hcr.hotKeys % nParts)
	hcr.hotKeysArray = make([]int, nParts)
	hcr.coldKeysArray = make([]int, nParts)
	hcr.partHotRd = make([]*rand.Rand, nParts)
	for i := 0; i < nParts; i++ {
		//hcr.hotKeysArray[i] = int64(hotPercent) * pKeysArray[i] / 100
		hcr.hotKeysArray[i] = perHotKeys
		if i < numExtraHot {
			hcr.hotKeysArray[i]++
		}
		hcr.coldKeysArray[i] = pKeysArray[i] - hcr.hotKeysArray[i]
		hcr.partHotRd[i] = rand.New(rand.NewSource(time.Now().UnixNano() / int64(partIndex*13+i*7+1)))
	}

	return hcr
}

func (hr *HotColdRand) GetWholeRank() int {
	hotrate := hr.hcRnd.Intn(100)
	if hotrate < hr.accessRate {
		// hot data
		return hr.wholeHotRd.Intn(hr.hotKeys)
	} else {
		// code data
		return hr.hotKeys + hr.wholeHotRd.Intn(hr.coldKeys)
	}
}

func (hr *HotColdRand) GetPartRank(pi int) int {
	hotrate := hr.hcRnd.Intn(100)
	if hotrate < hr.accessRate {
		// hot data
		return hr.partHotRd[pi].Intn(hr.hotKeysArray[pi])
	} else {
		// code data
		return hr.hotKeysArray[pi] + hr.partHotRd[pi].Intn(hr.coldKeysArray[pi])
	}
}

func (hr *HotColdRand) ResetPart(isPartition bool) {
	hr.isPartition = isPartition
}

type ZipfRandLarge struct {
	isPartition bool
	partIndex   int
	nParts      int
	nKeys       int
	wholeZipf   *rand.Zipf
	pKeysArray  []int
	partZipf    []*rand.Zipf
}

// Index of partition starts from 0
// Integer Key starts from 0 also
// s should be larger 1
func NewZipfRandLarge(partIndex int, nKeys int, nParts int, pKeysArray []int, s float64, isPartition bool) *ZipfRandLarge {

	zr := &ZipfRandLarge{
		isPartition: isPartition,
		partIndex:   partIndex,
		nParts:      nParts,
		nKeys:       nKeys,
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano() / int64(partIndex+1)))
	zr.wholeZipf = rand.NewZipf(rnd, s, 1, uint64(nKeys-1))

	zr.pKeysArray = make([]int, nParts)
	for i, k := range pKeysArray {
		zr.pKeysArray[i] = k
	}
	zr.partZipf = make([]*rand.Zipf, nParts)
	for i := 0; i < nParts; i++ {
		rnd = rand.New(rand.NewSource(time.Now().UnixNano() / int64(partIndex*13+i*7+1)))
		zr.partZipf[i] = rand.NewZipf(rnd, s, 1, uint64(pKeysArray[i]-1))
	}

	return zr
}

func (zr *ZipfRandLarge) GetWholeRank() int {
	return int(zr.wholeZipf.Uint64())
}

func (zr *ZipfRandLarge) GetPartRank(pi int) int {
	return int(zr.partZipf[pi].Uint64())
}

func (zr *ZipfRandLarge) ResetPart(isPartition bool) {
	zr.isPartition = isPartition
}

type ZipfRandSmall struct {
	isPartition bool
	partIndex   int
	nParts      int
	nKeys       int
	wholeZipf   *ZipfGenerator
	pKeysArray  []int
	partZipf    []*ZipfGenerator
}

// Index of partition starts from 0
// Integer Key starts from 0 also
// s should be smaller 1
func NewZipfRandSmall(partIndex int, nKeys int, nParts int, pKeysArray []int, s float64, isPartition bool) *ZipfRandSmall {

	zr := &ZipfRandSmall{
		isPartition: isPartition,
		partIndex:   partIndex,
		nParts:      nParts,
		nKeys:       nKeys,
	}

	//rnd := rand.New(rand.NewSource(time.Now().UnixNano() / int64(partIndex+1)))
	//zr.wholeZipf = rand.NewZipf(rnd, s, 1, uint64(nKeys-1))
	zr.wholeZipf = NewZipfGenerator(int64(nKeys), s, partIndex)

	zr.pKeysArray = make([]int, nParts)
	for i, k := range pKeysArray {
		zr.pKeysArray[i] = k
	}
	zr.partZipf = make([]*ZipfGenerator, nParts)
	for i := 0; i < nParts; i++ {
		//rnd = rand.New(rand.NewSource(time.Now().UnixNano() / int64(partIndex*13+i*7+1)))
		//zr.partZipf[i] = rand.NewZipf(rnd, s, 1, uint64(pKeysArray[i]-1))
		zr.partZipf[i] = NewZipfGenerator(int64(pKeysArray[i]), s, partIndex)
	}

	return zr
}

func (zr *ZipfRandSmall) GetWholeRank() int {
	return int(zr.wholeZipf.NextInt())
}

func (zr *ZipfRandSmall) GetPartRank(pi int) int {
	return int(zr.partZipf[pi].NextInt())
}

func (zr *ZipfRandSmall) ResetPart(isPartition bool) {
	zr.isPartition = isPartition
}

type UniformRand struct {
	isPartition  bool
	partIndex    int
	nParts       int
	nKeys        int
	wholeUniform *rand.Rand
	pKeysArray   []int
	partUniform  []*rand.Rand
}

func NewUniformRand(partIndex int, nKeys int, nParts int, pKeysArray []int, isPartition bool) *UniformRand {
	ur := &UniformRand{
		isPartition: isPartition,
		partIndex:   partIndex,
		nParts:      nParts,
		nKeys:       nKeys,
	}

	ur.wholeUniform = rand.New(rand.NewSource(time.Now().UnixNano() / int64(partIndex+1)))

	ur.pKeysArray = make([]int, nParts)
	for i, k := range pKeysArray {
		ur.pKeysArray[i] = k
	}
	ur.partUniform = make([]*rand.Rand, nParts)
	for i := 0; i < nParts; i++ {
		ur.partUniform[i] = rand.New(rand.NewSource(time.Now().UnixNano() / int64(partIndex*13+i*7+1)))
	}

	return ur
}

func (ur *UniformRand) GetWholeRank() int {
	return ur.wholeUniform.Intn(ur.nKeys)
}

func (ur *UniformRand) GetPartRank(pi int) int {
	return ur.partUniform[pi].Intn(ur.pKeysArray[pi])
}

func (ur *UniformRand) ResetPart(isPartition bool) {
	ur.isPartition = isPartition
}
