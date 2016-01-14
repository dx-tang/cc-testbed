package testbed

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestWorkload(t *testing.T) {
	nParts := 4
	nWorkers := 4

	wl := NewBasicWorkload("workload.txt", nParts, true, nWorkers, 1)

	pKeysArray := wl.generators[1].partitioner[1].GetKeyArray()

	for i, v := range pKeysArray {
		fmt.Printf("Part %v: %v keys\n", i, v)
	}

	rnd := rand.New(rand.NewSource(1))

	//for n := 0; n < b.N; n++ {
	for i := 0; i < 10; i++ {
		rndPart := rnd.Intn(nParts)
		fmt.Printf("part %v\n", rndPart)
		key := wl.generators[0].GetKey(CHECKING, rndPart)
		fmt.Printf("Key: %v:%v\n", ParseKey(key, 0), ParseKey(key, 1))
	}
	//}

}
