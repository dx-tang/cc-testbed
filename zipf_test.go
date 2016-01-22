package testbed

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/totemtang/cc-testbed/clog"
)

func BenchmarkZipfGenerator(b *testing.B) {
	numKeys := int64(100000)
	zg := NewZipfGenerator(numKeys, 0, 1)
	count := make([]int64, numKeys)

	for n := 0; n < b.N; n++ {
		k := zg.NextInt()
		if k >= numKeys {
			clog.Error("Out of Bound %v\n", k)
		}
		count[k]++
	}
	for i := 0; i < 10; i++ {
		fmt.Printf("%v:%v; ", i, count[i])
	}
	fmt.Printf("\n")
}

func BenchmarkZipfGeneratorGo(b *testing.B) {
	numKeys := int64(10)
	zg := rand.NewZipf(rand.New(rand.NewSource(1)), 1.01, 1, uint64(numKeys-1))
	count := make([]int64, numKeys)
	for n := 0; n < b.N; n++ {
		k := int64(zg.Uint64())
		if k >= numKeys {
			clog.Error("Out of Bound %v\n", k)
		}
		count[k]++
	}
	for i := 0; i < 10; i++ {
		fmt.Printf("%v:%v; ", i, count[i])
	}
	fmt.Printf("\n")
}
