package testbed

import (
	"math"
	"math/rand"
	"time"
)

const (
	ZIPFAN_CONSTANT = 0.99
)

type ZipfGenerator struct {
	rnd                    *rand.Rand
	items                  int64
	base                   int64
	zipfanconstant         float64
	alpha                  float64
	zetan                  float64
	eta                    float64
	theta                  float64
	zeta2theta             float64
	countforzeta           int64
	allowitemcountdecrease bool
}

func NewZipfGenerator(items int64, zipfanconstant float64, id int) *ZipfGenerator {
	zg := &ZipfGenerator{}
	zg.rnd = rand.New(rand.NewSource(time.Now().UnixNano() / int64((id+1)*89)))
	zg.items = items
	zg.base = 0
	zg.zipfanconstant = zipfanconstant
	zg.theta = zg.zipfanconstant
	zg.countforzeta = items
	zg.zeta2theta = computeZeta(0, 2, zg.theta)
	zg.alpha = 1.0 / (1.0 - zg.theta)
	zg.zetan = computeZeta(0, items, zg.theta)
	zg.eta = (1 - math.Pow(2.0/float64(items), 1-zg.theta)) / (1 - zg.zeta2theta/zg.zetan)

	return zg
}

func computeZeta(st int64, n int64, theta float64) float64 {
	sum := float64(0)
	for i := st; i < n; i++ {
		sum += 1 / math.Pow(float64(i+1), theta)
	}
	return sum
}

func (zg *ZipfGenerator) NextInt() int64 {
	itemcount := float64(zg.items)
	u := zg.rnd.Float64()
	uz := u * zg.zetan
	if uz < 1.0 {
		return zg.base
	}

	if uz < 1.0+math.Pow(0.5, zg.theta) {
		return zg.base + 1
	}

	ret := zg.base + int64(itemcount*math.Pow(zg.eta*u-zg.eta+1, zg.alpha))
	return ret
}
