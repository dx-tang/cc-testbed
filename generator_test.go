package testbed

import (
	"fmt"
	"testing"
)

func TestGenerator(t *testing.T) {
	fmt.Println("====================")
	fmt.Println("Test Generator Begin")
	fmt.Println("====================")

	// Test a store with 23 keys
	// Without partition, RANDOM_UPDATE_INT
	fmt.Println("Without partition")

	*SysType = OCC

	nKeys := int64(100)
	var pKeysArray []int64
	s := float64(1.1)

	zk := NewZipfKey(0, nKeys, 1, pKeysArray, s, nil)

	rr := float64(50)
	txnLen := 2

	generator := NewTxnGen(RANDOM_UPDATE_INT, rr, txnLen, -1, zk)

	// Generator 3 queries
	for i := 0; i < 10; i++ {
		printOneQuery(generator.GenOneQuery())
	}

	/*
		cr := float64()

		maxParts := 3
	*/
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
		fmt.Printf("Write Values Include: ")
		for _, v := range wValue.strVals {
			fmt.Printf("%v %s; ", v.index, v.value)
		}
		fmt.Printf("\n")
	}

}
