package testbed

import (
	"fmt"
	"testing"
)

func BenchmarkHotColdRand(b *testing.B) {
	NParts := int64(13)
	keyRange := []int64{100, 1000}
	hp := NewHashPartitioner(NParts, keyRange)
	pKeysArray := hp.GetKeyArray()
	nKeys := int64(keyRange[0] * keyRange[1])

	var kg KeyGen

	kg = NewHotColdRand(0, nKeys, int(NParts), pKeysArray, 180, true)

	hotRange := int64(1) * pKeysArray[0] / 100

	count := 0
	hotRate := 0

	for n := 0; n < b.N; n++ {
		rk := kg.GetPartRank(1)
		if rk < hotRange {
			hotRate++
		}
		count++

		key := hp.GetPartKey(1, rk)
		key[0] = 0
		//fmt.Printf("%v:%v\n", ParseKey(key, 0), ParseKey(key, 1))
	}
	fmt.Printf("%v Tests; %.2f Hot Data\n", count, float64(hotRate)/float64(count))
}

func BenchmarkZipfRand(b *testing.B) {
	NParts := int64(13)
	keyRange := []int64{100, 1000}
	hp := NewHashPartitioner(NParts, keyRange)
	pKeysArray := hp.GetKeyArray()
	nKeys := int64(keyRange[0] * keyRange[1])

	var kg KeyGen

	kg = NewZipfRand(0, nKeys, int(NParts), pKeysArray, 1.01, true)

	hotRange := int64(20) * pKeysArray[0] / 100

	count := 0
	hotRate := 0

	for n := 0; n < b.N; n++ {
		rk := kg.GetPartRank(0)
		if rk < hotRange {
			hotRate++
		}
		count++

		key := hp.GetPartKey(0, rk)
		key[0]++
	}
	fmt.Printf("%v Tests; %.2f Hot Data\n", count, float64(hotRate)/float64(count))
}

func BenchmarkUniformRand(b *testing.B) {
	NParts := int64(13)
	keyRange := []int64{100, 1000}
	hp := NewHashPartitioner(NParts, keyRange)
	pKeysArray := hp.GetKeyArray()
	nKeys := int64(keyRange[0] * keyRange[1])

	var kg KeyGen

	kg = NewUniformRand(0, nKeys, int(NParts), pKeysArray, true)

	hotRange := int64(20) * pKeysArray[0] / 100

	count := 0
	hotRate := 0

	for n := 0; n < b.N; n++ {
		rk := kg.GetPartRank(0)
		if rk < hotRange {
			hotRate++
		}
		count++

		key := hp.GetPartKey(0, rk)
		key[0]++
	}
	fmt.Printf("%v Tests; %.2f Hot Data\n", count, float64(hotRate)/float64(count))
}
