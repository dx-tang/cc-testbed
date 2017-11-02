package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
)

type SSI_Entry struct {
	padding1 [PADDING]byte
	index    int
	val      [SSI_MAX_VERSION]Value
	col      [SSI_MAX_VERSION]Value
	padding2 [PADDING]byte
}

const (
	SSI_LOADFACTOR    = 0.75 // numEntries/numBuckets
	SSI_PERHASHBUCKET = 15
	SSI_MAX_VERSION   = 10
)

type SSI_Bucket struct {
	padding1 [PADDING]byte
	cur      int
	recArray [SSI_PERHASHBUCKET]SSI_Entry
	keyArray [SSI_PERHASHBUCKET]Key
	next     *SSI_Bucket
	padding2 [PADDING]byte
}

type SSI_HashTable struct {
	padding1  [PADDING]byte
	bucket    []SSI_Bucket
	latches   []SpinLockPad
	bucketNum int
	hashCode  func(Key) int
	padding2  [PADDING]byte
}

func NewSSITable(numEntries int) *SSI_HashTable {
	numBucket := int(float64(numEntries)/(SSI_PERHASHBUCKET*SSI_LOADFACTOR) + 1)
	sht := &SSI_HashTable{
		bucket:    make([]SSI_Bucket, numBucket),
		latches:   make([]SpinLockPad, numBucket),
		bucketNum: numBucket,
	}
	sht.hashCode = func(k Key) int {
		return k[KEY1]*(*NumPart) + k[KEY0]
	}

	return sht
}

func (sht *SSI_HashTable) Insert(k Key) {
	bucketIndex := sht.hashCode(k) % sht.bucketNum
	bucket := &sht.bucket[bucketIndex]
	for bucket.cur >= SSI_PERHASHBUCKET {
		bucket = bucket.next
	}
	bucket.keyArray[bucket.cur] = k
	bucket.recArray[bucket.cur].index = 0
	bucket.cur++
	if bucket.cur == SSI_PERHASHBUCKET {
		new_bucket := &SSI_Bucket{}
		new_bucket.cur = 0
		new_bucket.next = nil
		bucket.next = new_bucket
	}
}

func (sht *SSI_HashTable) Put(k Key, val Value, col Value) {
	bucketIndex := sht.hashCode(k) % sht.bucketNum
	bucket := &sht.bucket[bucketIndex]
	sht.latches[bucketIndex].Lock()
	for bucket != nil {
		for i := 0; i < bucket.cur; i++ {
			if k == bucket.keyArray[i] {
				version := bucket.recArray[i].index
				bucket.recArray[i].val[version] = val
				bucket.recArray[i].col[version] = col
				bucket.recArray[i].index = (bucket.recArray[i].index + 1) % SSI_MAX_VERSION
				sht.latches[bucketIndex].Unlock()
				return
			}
		}
		bucket = bucket.next
	}

	clog.Error("Not Found key, Impossible")
}
