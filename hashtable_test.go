package testbed

import (
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkHashTable(b *testing.B) {
	WLTYPE = SINGLEWL
	ht := NewHashTable(10000, false, true, 0)
	rec := &ARecord{}
	var k Key

	rnd := rand.New(rand.NewSource(1))

	numFail := 0
	numSuccess := 0

	for n := 0; n < b.N; n++ {
		k[0] = rnd.Intn(10000)
		if !ht.Put(k, rec, nil) {
			numFail++
		} else {
			numSuccess++
		}
		_, ok := ht.Get(k)
		if !ok {
			fmt.Printf("Error\n")
		}
	}

	fmt.Printf("Success %v; Fail %v\n", numSuccess, numFail)

}
