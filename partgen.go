package testbed

import (
	"math/rand"
	"time"

	//"github.com/totemtang/cc-testbed/clog"
)

type PartGen interface {
	GetOnePart() int
}

type ZipfRandLargePart struct {
	nParts    int
	wholeZipf *rand.Zipf
}

func NewZipfRandLargePart(s float64, partIndex int, nParts int) *ZipfRandLargePart {

	zp := &ZipfRandLargePart{
		nParts: nParts,
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano() / int64(partIndex+1)))
	zp.wholeZipf = rand.NewZipf(rnd, s, 1, uint64(nParts-1))

	return zp
}

func (zp *ZipfRandLargePart) GetOnePart() int {
	return int(zp.wholeZipf.Uint64())
}

type ZipfRandSmallPart struct {
	nParts    int
	wholeZipf *ZipfGenerator
}

func NewZipfRandSmallPart(s float64, partIndex int, nParts int) *ZipfRandSmallPart {

	zp := &ZipfRandSmallPart{
		nParts: nParts,
	}

	zp.wholeZipf = NewZipfGenerator(int64(nParts), s, partIndex)

	return zp
}

func (zp *ZipfRandSmallPart) GetOnePart() int {
	return int(zp.wholeZipf.NextInt())
}

type UniformRandPart struct {
	nParts       int
	wholeUniform *rand.Rand
}

func NewUniformRandPart(s float64, partIndex int, nParts int) *UniformRandPart {
	ur := &UniformRandPart{
		nParts: nParts,
	}

	ur.wholeUniform = rand.New(rand.NewSource(time.Now().UnixNano() / int64(partIndex+1)))

	return ur
}

func (ur *UniformRandPart) GetOnePart() int {
	return ur.wholeUniform.Intn(ur.nParts)
}
