package testbed

import (
	"math/rand"
	"testing"
)

func BenchmarkPartitioner(b *testing.B) {
	NParts := int64(3)
	keyRange := []int64{8, 10}
	hp := NewHashPartitioner(NParts, keyRange)

	pKeysArray := hp.GetKeyArray()
	/*for i, v := range pKeysArray {
		fmt.Printf("Part %v: %v keys\n", i, v)
	}*/

	rnd := rand.New(rand.NewSource(1))

	for n := 0; n < b.N; n++ {
		//for i := 0; i < 10; i++ {
		rndPart := rnd.Int63n(NParts)
		rndRank := rnd.Int63n(pKeysArray[rndPart])
		//fmt.Printf("part %v; rank %v\n", rndPart, rndRank)
		key := hp.GetPartKey(int(rndPart), rndRank)
		key[0] = 0
		//fmt.Printf("Key: %v:%v\n", ParseKey(key, 0), ParseKey(key, 1))
		//}
	}
}
