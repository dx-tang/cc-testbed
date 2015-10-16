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

func printOneQuery(q *Query) {
	if !q.isPartition {
		fmt.Printf("This transaction touches whole store \n")
	} else {
		fmt.Printf("This transaction touches %v partitions: ", len(q.accessParts))
		for _, p := range q.accessParts {
			fmt.Printf("%v ", p)
		}
		fmt.Printf("\n")
	}
	fmt.Printf("Read Keys Include: ")
	for _, k := range q.rKeys {
		fmt.Printf("%v ", ParseKey(k))
	}
	fmt.Printf("\nWrite Keys Include: ")
	for _, k := range q.wKeys {
		fmt.Printf("%v ", ParseKey(k))
	}
	fmt.Printf("\n")
	if q.TXN == RANDOM_UPDATE_INT {
		wValue := q.wValue.(*SingleIntValue)
		fmt.Printf("Write Values Include: ")
		for _, v := range wValue.intVals {
			fmt.Printf("%v ", v)
		}
		fmt.Printf("\n")
	} else if q.TXN == RANDOM_UPDATE_STRING {
		wValue := q.wValue.(*StringListValue)
		fmt.Printf("Write Values Include: \n")
		for _, v := range wValue.strVals {
			fmt.Printf("%v. %s \n", v.index, v.value)
		}
		fmt.Printf("\n")
	}

}
