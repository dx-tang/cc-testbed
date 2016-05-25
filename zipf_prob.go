package testbed

type ZipfProb struct {
	padding1 [PADDING]byte
	n        int
	id       int
	partprob [32]float64
	padding2 [PADDING]byte
}

func NewZipfProb(s float64, n int) ZipfProb {
	var zp ZipfProb
	zp.n = n
	zg := NewZipfGenerator(int64(n), s, 0)
	for i := 0; i < 10000000; i++ {
		zp.partprob[zg.NextInt()]++
	}
	for i := 31; i >= 0; i-- {
		zp.partprob[i] = zp.partprob[i] / zp.partprob[0]
	}
	return zp
}

func (zp *ZipfProb) GetProb(k int) float64 {
	return zp.partprob[k]
}

func (zp *ZipfProb) Reconf(s float64) {
	zg := NewZipfGenerator(int64(zp.n), s, 0)
	for i, _ := range zp.partprob {
		zp.partprob[i] = 0
	}
	for i := 0; i < 10000000; i++ {
		zp.partprob[zg.NextInt()]++
	}
	for i := 31; i >= 0; i-- {
		zp.partprob[i] = zp.partprob[i] / zp.partprob[0]
	}
}
