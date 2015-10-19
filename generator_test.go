package testbed

import (
	"fmt"
	"testing"
)

func TestGenerator(t *testing.T) {
	fmt.Println("====================")
	fmt.Println("Test Generator Begin")
	fmt.Println("====================")

	// Test a store with 100 keys
	// Without partition, RANDOM_UPDATE_INT
	fmt.Println("Without partition")

	*SysType = OCC

	nKeys := int64(100)
	var pKeysArray []int64
	s := float64(1.1)

	zk := NewZipfKey(0, nKeys, 1, pKeysArray, s, nil)

	rr := float64(50)
	txnLen := 16

	generator := NewTxnGen(RANDOM_UPDATE_INT, rr, txnLen, -1, zk)

	// Generator 3 queries
	for i := 0; i < 3; i++ {
		printOneQuery(generator.GenOneQuery())
	}

	// Test partition
	fmt.Println("With Partition")

	*SysType = PARTITION

	nParts := 6
	var p Partitioner
	p = &HashPartitioner{
		NParts: int64(nParts),
		NKeys:  nKeys,
	}

	pKeysArray = make([]int64, nParts)
	for i := int64(0); i < nKeys; i++ {
		key := CKey(i)
		pKeysArray[p.GetPartition(key)]++
	}

	// Only Test Partition 0
	zk = NewZipfKey(3, nKeys, nParts, pKeysArray, s, p)

	rr = float64(50)
	txnLen = 5
	//cr := float64(0)
	*cr = float64(50)
	maxParts := 5

	generator = NewTxnGen(RANDOM_UPDATE_STRING, rr, txnLen, maxParts, zk)

	// Generator 3 queries
	for i := 0; i < 3; i++ {
		printOneQuery(generator.GenOneQuery())
	}

	fmt.Println("==================")
	fmt.Println("Test Generator End")
	fmt.Println("==================")

}
