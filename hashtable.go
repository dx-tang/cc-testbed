package testbed

/*import (
	"fmt"
)*/

import (
	"github.com/totemtang/cc-testbed/clog"
)


const (
	LOADFACTOR    = 0.75 // numEntries/numBuckets
	PERHASHBUCKET = 15
	LATCHNUM      = 4096
)

type HashBucket struct {
	padding1 [PADDING]byte
	cur      int
	recArray [PERHASHBUCKET]Record
	keyArray [PERHASHBUCKET]Key
	next     *HashBucket
	padding2 [PADDING]byte
}

type HashTable struct {
	padding1    [PADDING]byte
	bucket      []HashBucket
	latches     [LATCHNUM]RWSpinLockPad
	bucketNum   int
	hashCode    func(Key) int
	useLatch    bool
	isPartition bool
	tableID     int
	padding2    [PADDING]byte
}

func NewHashTable(numEntries int, isPartition bool, useLatch bool, tableID int) *HashTable {
	numBucket := int(float64(numEntries)/(PERHASHBUCKET*LOADFACTOR) + 1)
	ht := &HashTable{
		bucket:      make([]HashBucket, numBucket),
		bucketNum:   numBucket,
		useLatch:    useLatch,
		isPartition: isPartition,
		tableID: tableID,
	}

	if WLTYPE == TPCCWL {
		if isPartition {
			ht.hashCode = func(k Key) int {
				return k[KEY1]
			}
		} else {
			ht.hashCode = func(k Key) int {
				return k[KEY1]*(*NumPart) + k[KEY0]
			}
		}
		if tableID == ITEM {
			ht.hashCode = func(k Key) int {
				return k[KEY0]
			}
		} else if tableID == STOCK {
			if isPartition {
				ht.hashCode = func(k Key) int {
					return k[KEY1]
				}
			} else {
				ht.hashCode = func(k Key) int {
					return (k[KEY1]*(*NumPart) + k[KEY0])
				}
			}
		} else if tableID == CUSTOMER {
			if isPartition {
				ht.hashCode = func(k Key) int {
					return k[KEY2]*DIST_COUNT + k[KEY1]
				}
			} else {
				ht.hashCode = func(k Key) int {
					return k[KEY2]*(*NumPart)*DIST_COUNT + k[KEY0]*DIST_COUNT + k[KEY1]
				}
			}
		}
	} else {
		if isPartition {
			ht.hashCode = func(k Key) int {
				return k[KEY0] / (*NumPart)
			}
		} else {
			ht.hashCode = func(k Key) int {
				return k[KEY0]
			}
		}
	}

	return ht

}

func (ht *HashTable) SetLatch(useLatch bool) {
	ht.useLatch = useLatch
}

func (ht *HashTable) Put(k Key, rec Record, ia IndexAlloc) bool {
	bucketIndex := ht.hashCode(k) % ht.bucketNum
	bucket := &ht.bucket[bucketIndex]
	if ht.useLatch {
		ht.latches[bucketIndex%LATCHNUM].Lock()
	}
	var lessThan bool = false
	for {
		for i := 0; i < bucket.cur; i++ {
			//if k == bucket.keyArray[i] {
			//	if ht.useLatch {
			//		ht.latches[bucketIndex%LATCHNUM].Unlock()
			//	}
			//	return false
			//}
		 	if k[0] < bucket.keyArray[i][0] {
				lessThan = true
			} else if k[0] == bucket.keyArray[i][0] {
				if k[1] < bucket.keyArray[i][1] {
					lessThan = true
				}
			}
			if lessThan {
				for j := bucket.cur - 1; j >= i; j-- {
					bucket.keyArray[j+1] = bucket.keyArray[j]
					bucket.recArray[j+1] = bucket.recArray[j]
				}
				bucket.keyArray[i] = k
				bucket.recArray[i] = rec
				bucket.cur++
				if ht.useLatch {
					ht.latches[bucketIndex%LATCHNUM].Unlock()
				}
				return true
			}
		}
		
		/*if bucket.cur >= len(bucket.keyArray) {
			clog.Error("%v Index Out of Range: %v %v", ht.tableID, bucket.cur, len(bucket.keyArray))
		}*/

		bucket.keyArray[bucket.cur] = k
		bucket.recArray[bucket.cur] = rec
		bucket.cur++
		break

		/*
			if bucket.cur < PERHASHBUCKET {
				bucket.keyArray[bucket.cur] = k
				bucket.recArray[bucket.cur] = rec
				bucket.cur++
				break
			} else if bucket.next == nil {
				var newBucket *HashBucket
				if ia == nil {
					newBucket = &HashBucket{}
				} else {
					newBucket = ia.GetEntry().(*HashBucket)
				}
				bucket.next = newBucket
			}*/
		bucket = bucket.next
	}

	if ht.useLatch {
		ht.latches[bucketIndex%LATCHNUM].Unlock()
	}

	return true
}

func (ht *HashTable) Get(k Key) (Record, bool) {
	bucketIndex := ht.hashCode(k) % ht.bucketNum
	bucket := &ht.bucket[bucketIndex]
	if ht.useLatch {
		ht.latches[bucketIndex%LATCHNUM].RLock()
	}
	for bucket != nil {
		for i := 0; i < bucket.cur; i++ {
			if k == bucket.keyArray[i] {
				if ht.useLatch {
					ht.latches[bucketIndex%LATCHNUM].RUnlock()
				}
				return bucket.recArray[i], true
			}
		}
		bucket = bucket.next
	}
	if ht.useLatch {
		ht.latches[bucketIndex%LATCHNUM].RUnlock()
	}
	clog.Info("%v %v %v %v %v %v",k, bucketIndex, ht.useLatch, ht.bucket[bucketIndex].keyArray, ht.bucket[bucketIndex].cur, ht.bucketNum)
	return nil, false
}
