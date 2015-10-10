package testbed

import (
	"fmt"
	"testing"
)

func TestHashPartitioner(t *testing.T) {
	nParts := int64(100)
	nKeys := int64(100000)
	hp := &HashPartitioner{
		NParts: nParts,
	}

	for i := int64(0); i < nKeys; i++ {
		k := CKey(i)
		p := i % nParts
		testP := hp.GetPartition(k)
		if testP != int(p) {
			t.Errorf("Error Partition %v for %v", testP, p)
		}
	}

	fmt.Printf("%v tests passed \n", nKeys)
}
