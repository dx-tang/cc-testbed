package testbed

import (
	"fmt"
	"testing"
)

func TestZipf(t *testing.T) {
	fmt.Println("===============")
	fmt.Println("Test Zipf Begin")
	fmt.Println("===============")

	// Test a store with 23 keys
	// First without partition
	fmt.Println("Without partition")

	*SysType = OCC

	nKeys := int64(23)
	pKeysArray := make([]int64, 0)
	s := float64(1.1)

	zk := NewZipfKey(0, nKeys, 1, pKeysArray, s, nil)

	statics := make([]int, nKeys)

	// Test 1000 times
	for i := 0; i < 1000; i++ {
		statics[zk.GetKey()]++
	}

	// Output Statistics
	fmt.Println("Output Statistics")
	for i := range statics {
		fmt.Printf("Key %v frequency: %v \n", i, float64(statics[i])/float64(1000))
	}

	// Now Test Partition; 5 partitions
	*SysType = PARTITION

	nParts := 5
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
	zk = NewZipfKey(0, nKeys, nParts, pKeysArray, s, p)

	// Try to get key from partition 0
	statics = make([]int, pKeysArray[0])

	// Test 100 times
	for i := 0; i < 100; i++ {
		statics[p.GetRank(zk.GetSelfKey())]++
	}

	// Output Statistics
	fmt.Println("Output Statistics")
	for i := range statics {
		fmt.Printf("Key %v frequency: %v \n", i, float64(statics[i])/float64(100))
	}

	// Try to get key from partition 4
	statics = make([]int, pKeysArray[4])

	// Test 100 times
	for i := 0; i < 100; i++ {
		statics[p.GetRank(zk.GetOtherKey(4))]++
	}

	// Output Statistics
	fmt.Println("Output Statistics")
	for i := range statics {
		fmt.Printf("Key %v frequency: %v \n", i, float64(statics[i])/float64(100))
	}

	fmt.Println("=============")
	fmt.Println("Test Zipf End")
	fmt.Println("=============")
}
