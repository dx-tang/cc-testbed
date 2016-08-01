package testbed

import (
	"github.com/totemtang/cc-testbed/spinlock"
)

const (
	LOADFACTOR    = 0.75 // numEntries/numBuckets
	PERHASHBUCKET = 5
	LATCHNUM      = 64
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
	padding1  [PADDING]byte
	bucket    []HashBucket
	latches   [LATCHNUM]RWSpinLockPad
	bucketNum int
	hashCode  func(Key) int
	useLatch  bool
	padding2  [PADDING]byte
}

func NewHashTable(numEntries int, isPartition bool, useLatch bool, tableID int) *HashTable {
	numBucket := int(float64(numEntries)/(PERHASHBUCKET*LOADFACTOR) + 1)
	ht := &HashTable{
		bucket:    make([]HashBucket, numBucket),
		bucketNum: numBucket,
		useLatch:  useLatch,
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
		ht.hashCode = func(k Key) int {
			return k[KEY0]
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
		defer ht.latches[bucketIndex%LATCHNUM].Unlock()
	}
	for {
		for i := 0; i < bucket.cur; i++ {
			if k == bucket.keyArray[i] {
				return false
			}
		}
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
		}
		bucket = bucket.next
	}

	return true
}

func (ht *HashTable) Get(k Key) (Record, bool) {
	bucketIndex := ht.hashCode(k) % ht.bucketNum
	bucket := &ht.bucket[bucketIndex]
	if ht.useLatch {
		ht.latches[bucketIndex%LATCHNUM].RLock()
		defer ht.latches[bucketIndex%LATCHNUM].RUnlock()
	}
	for bucket != nil {
		for i := 0; i < bucket.cur; i++ {
			if k == bucket.keyArray[i] {
				return bucket.recArray[i], true
			}
		}
		bucket = bucket.next
	}
	return nil, false
}
