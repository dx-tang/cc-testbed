package testbed

import (
	"fmt"
	"testing"

	"github.com/totemtang/cc-testbed/clog"
)

func TestTransactions(t *testing.T) {

	fmt.Println("======================")
	fmt.Println("Test Transaction Begin")
	fmt.Println("======================")

	*SysType = PARTITION

	nKeys := int64(100)
	nParts := 6
	*NumPart = nParts
	s := float64(1)
	var pKeysArray []int64

	var p Partitioner
	p = &HashPartitioner{
		NParts: int64(nParts),
		NKeys:  nKeys,
	}

	store := NewStore()
	//Create Keys
	for i := int64(0); i < nKeys; i++ {
		k := CKey(i)
		partNum := p.GetPartition(k)
		store.CreateKV(k, int64(0), SINGLEINT, partNum)
	}

	pKeysArray = make([]int64, nParts)
	for i := int64(0); i < nKeys; i++ {
		key := CKey(i)
		pKeysArray[p.GetPartition(key)]++
	}

	zk := NewZipfKey(3, nKeys, nParts, pKeysArray, s, p)

	rr := float64(0)
	txnLen := 5
	*cr = float64(0)
	maxParts := 5

	generator := NewTxnGen(ADD_ONE, rr, txnLen, maxParts, zk)

	//New worker
	worker := NewWorker(3, store)

	var q *Query
	for i := 0; i < 100; i++ {
		q = generator.GenOneQuery()
		//printOneQuery(q)
		worker.One(q)
	}

	//PrintStore(store, nKeys, p)
	PrintPartition(store, nKeys, p, 3)

	fmt.Println("====================")
	fmt.Println("Test Transaction End")
	fmt.Println("====================")

}

func PrintPartition(s *Store, nKeys int64, p Partitioner, partNum int) {
	for i := int64(0); i < nKeys; i++ {
		k := CKey(i)
		if p.GetPartition(k) != partNum {
			continue
		}
		br := s.GetRecord(k, partNum)
		if br == nil {
			clog.Error("Error No Key")
		}
		intKey := int64(br.key[0]) + int64(br.key[1])<<8 + int64(br.key[2])<<16 + int64(br.key[3])<<24 + int64(br.key[4])<<32 + int64(br.key[5])<<40 + int64(br.key[6])<<48 + int64(br.key[7])<<56
		clog.Info("Key %v: %v", intKey, br.intVal)
	}
}
