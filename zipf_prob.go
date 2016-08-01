package testbed

import (
	"math"

	"github.com/totemtang/cc-testbed/clog"
)

type ZipfProb struct {
	padding1 [PADDING]byte
	sum      float64
	s        float64
	n        int
	padding2 [PADDING]byte
}

func NewZipfProb(s float64, n int) ZipfProb {
	var zp ZipfProb
	sum := float64(0)
	for i := 1; i <= n; i++ {
		sum += 1 / math.Pow(float64(i), s)
	}
	zp.n = n
	zp.s = s
	zp.sum = sum
	return zp
}

func (zp *ZipfProb) GetProb(k int) float64 {
	if k >= zp.n || k < 0 {
		clog.Error("Not Valid Rank %v", k)
	}
	e := 1 / math.Pow(float64(zp.n-k), zp.s)
	f := 1 / math.Pow(1, zp.s)
	return e / f
}

func (zp *ZipfProb) Reconf(s float64) {
	zp.s = s
	sum := float64(0)
	for i := 1; i <= zp.n; i++ {
		sum += 1 / math.Pow(float64(i), s)
	}
	zp.sum = sum
}
