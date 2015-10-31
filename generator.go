package testbed

import (
	"flag"
	"math/rand"
	"time"
)

var CrossPercent = flag.Float64("cr", 0.0, "percentage of cross-partition transactions")

type TxnGen struct {
	TXN         int
	nKeys       int64
	nParts      int
	partIndex   int
	rr          float64
	txnLen      int
	maxParts    int
	isPartition bool
	rnd         *rand.Rand
	zk          *ZipfKey
}

func NewTxnGen(TXN int, rr float64, txnLen int, maxParts int, zk *ZipfKey) *TxnGen {
	txnGen := &TxnGen{
		TXN:         TXN,
		nKeys:       zk.nKeys,
		nParts:      zk.nParts,
		partIndex:   zk.partIndex,
		rr:          rr,
		txnLen:      txnLen,
		maxParts:    maxParts,
		isPartition: *SysType == PARTITION,
		zk:          zk,
	}

	//txnGen.local_seed = uint32(rand.Intn(10000000))
	txnGen.rnd = rand.New(rand.NewSource(time.Now().Unix()))

	return txnGen
}

//Determine a read or a write operation
func insertRWKey(q *Query, k Key, rr float64, rnd *rand.Rand) {
	x := float64(rnd.Int63n(100))
	if x < rr {
		q.rKeys = append(q.rKeys, k)
	} else {
		q.wKeys = append(q.wKeys, k)
	}
}

func (tg *TxnGen) GenOneQuery() *Query {
	q := &Query{
		TXN:         tg.TXN,
		txnLen:      tg.txnLen,
		isPartition: tg.isPartition,
		partitioner: tg.zk.hp,
		rKeys:       make([]Key, 0, tg.txnLen),
		wKeys:       make([]Key, 0, tg.txnLen),
	}

	// Generate keys for different CC
	if tg.isPartition {
		//x := float64(RandN(&tg.local_seed, 100))
		x := float64(tg.rnd.Int63n(100))
		if x < *CrossPercent {
			// Generate how many partitions this txn will touch; more than 1
			var numAccess int
			if tg.maxParts < tg.txnLen {
				numAccess = tg.maxParts
			} else {
				numAccess = tg.txnLen
			}
			numAccess = tg.rnd.Intn(numAccess-1) + 2
			q.accessParts = make([]int, numAccess)

			// Generate partitions this txn will touch
			// For simplicity, only generate continuous partitions
			for i := 0; i < numAccess; i++ {
				if tg.partIndex+numAccess <= tg.nParts {
					q.accessParts[i] = tg.partIndex + i
				} else {
					tmp := tg.partIndex + i
					if tmp >= tg.nParts {
						q.accessParts[tmp-tg.nParts] = tmp - tg.nParts
					} else {
						q.accessParts[numAccess-tg.nParts+tg.partIndex+i] = tmp
					}
				}
			}

			var j int = 0
			for i := 0; i < tg.txnLen; i++ {
				insertRWKey(q, tg.zk.GetOtherKey(q.accessParts[j]), tg.rr, tg.rnd)
				j = (j + 1) % numAccess
			}

		} else {
			q.accessParts = make([]int, 1)
			q.accessParts[0] = tg.partIndex

			for i := 0; i < tg.txnLen; i++ {
				insertRWKey(q, tg.zk.GetSelfKey(), tg.rr, tg.rnd)
			}
		}

	} else {
		// Generate random keys
		for i := 0; i < tg.txnLen; i++ {
			insertRWKey(q, tg.zk.GetKey(), tg.rr, tg.rnd)
		}
	}

	// Generate values according to the transaction type
	q.GenValue(tg.rnd)

	return q
}
