package testbed

import (
	"flag"
	//"github.com/totemtang/cc-testbed/clog"
	"math"
)

var TopK = flag.Bool("topk", false, "use topk for each worker to monitor access contention")

const (
	BUCKETBUFSIZE = 1000
	ELEMSIZE      = 1000
	BIGK          = 1000
)

type TopKCounter struct {
	padding1  [PADDING]byte
	m         int
	numElem   int
	min       *CounterBucket
	max       *CounterBucket
	buckBuf   []CounterBucket
	buckIndex int
	elemBuf   []CounterElem
	keyAR     []Key
	countAR   []int
	padding2  [PADDING]byte
}

type CounterElem struct {
	padding1 [PADDING]byte
	initErr  int
	k        Key
	next     *CounterElem
	prev     *CounterElem
	padding2 [PADDING]byte
}

type CounterBucket struct {
	padding1 [PADDING]byte
	next     *CounterBucket
	prev     *CounterBucket
	value    int
	elemHead *CounterElem
	padding2 [PADDING]byte
}

func NewTopKCounter(m int, k int) *TopKCounter {
	tc := &TopKCounter{}
	tc.m = m
	tc.numElem = 0
	tc.buckBuf = make([]CounterBucket, BUCKETBUFSIZE)
	for i, _ := range tc.buckBuf {
		tc.buckBuf[i].value = 0
	}
	tc.buckIndex = 0
	tc.elemBuf = make([]CounterElem, m)

	// Always keep a min bucket
	tc.min = &CounterBucket{}
	tc.max = tc.min
	tc.min.next = nil
	tc.max.prev = nil
	tc.min.value = 1
	tc.min.elemHead = nil
	tc.keyAR = make([]Key, k)
	tc.countAR = make([]int, k)
	return tc
}

func (tc *TopKCounter) InsertHeader(bucket *CounterBucket, elem *CounterElem) {
	if bucket.elemHead == nil {
		bucket.elemHead = elem
		elem.prev = nil
		elem.next = nil
	} else {
		preHead := bucket.elemHead
		bucket.elemHead = elem
		elem.next = preHead
		elem.prev = nil
		preHead.prev = elem
	}
}

func (tc *TopKCounter) DeleteElem(elem *CounterElem) {
	prev := elem.prev
	next := elem.next
	if prev != nil {
		prev.next = next
	}
	if next != nil {
		next.prev = prev
	}
}

func (tc *TopKCounter) InsertAfter(newBucket *CounterBucket, oldBucket *CounterBucket) {
	next := oldBucket.next
	oldBucket.next = newBucket
	newBucket.prev = oldBucket
	newBucket.next = next
	if next != nil {
		next.prev = newBucket
	}
}

func (tc *TopKCounter) DeleteBucket(bucket *CounterBucket) {
	prev := bucket.prev
	next := bucket.next
	if prev != nil {
		prev.next = next
	}
	if next != nil {
		next.prev = prev
	}
}

func (tc *TopKCounter) InsertKey(k Key) {
	//clog.Info("Insert K %v", k)
	tmpBucket := tc.min

	for tmpBucket != nil {
		tmpElem := tmpBucket.elemHead
		for tmpElem != nil {
			if tmpElem.k == k {
				// Find it
				// Delete from current bucket
				tc.DeleteElem(tmpElem)

				if tmpElem == tmpBucket.elemHead {
					tmpBucket.elemHead = tmpElem.next
				}

				// Insert into the next bucket
				if tmpBucket.next != nil && tmpBucket.next.value == tmpBucket.value+1 {
					tc.InsertHeader(tmpBucket.next, tmpElem)
				} else {
					if len(tc.buckBuf) == tc.buckIndex {
						tc.buckIndex = 0
						tc.buckBuf = make([]CounterBucket, BUCKETBUFSIZE)
					}
					newBucket := &tc.buckBuf[tc.buckIndex]
					newBucket.next = nil
					newBucket.prev = nil
					tc.buckIndex++
					newBucket.value = tmpBucket.value + 1
					tc.InsertAfter(newBucket, tmpBucket)
					tc.InsertHeader(newBucket, tmpElem)
					if tmpBucket == tc.max {
						tc.max = newBucket
					}
				}

				if tmpBucket.elemHead == nil && tmpBucket.value != 1 {
					tc.DeleteBucket(tmpBucket)
				}
				//clog.Info("Insert K %v Finish", k)
				return
			}
			tmpElem = tmpElem.next
		}
		tmpBucket = tmpBucket.next
	}

	var tmpElem *CounterElem
	if tc.numElem != tc.m {
		elem := &tc.elemBuf[tc.numElem]
		elem.next = nil
		elem.prev = nil
		elem.initErr = 0
		elem.k = k
		tc.InsertHeader(tc.min, elem)
		tc.numElem++
	} else {
		// Evict an element
		tmpBucket = tc.min
		for tmpBucket != nil {
			if tmpBucket.elemHead != nil {
				tmpElem = tmpBucket.elemHead
				tmpElem.initErr = tmpBucket.value
				tmpElem.k = k
				tmpBucket.elemHead = tmpBucket.elemHead.next
				if tmpBucket.elemHead == nil && tmpBucket.value != 1 {
					tc.DeleteBucket(tmpBucket)
				}
				break
			}
			tmpBucket = tmpBucket.next
		}
		tc.InsertHeader(tc.min, tmpElem)
	}
	//clog.Info("Insert K %v Finish", k)
}

func (tc *TopKCounter) GetTopK(keyAR []Key, countAR []int, k int) int {
	//order := true
	//guaranteed := false
	tmpK := k
	if tc.numElem < k {
		tmpK = tc.numElem
	}
	min := math.MaxInt32
	initErr := 0
	for i := 0; i < tmpK; i++ {
		initErr = tc.OutputI(keyAR, countAR, i)
		if countAR[i]-initErr < min {
			min = countAR[i] - initErr
		}
	}
	if tc.numElem < k {
		return tc.numElem
	}
	return k
	/*
		trueK := k
		if tc.max.value <= min {
			return trueK
		} else {
			initErr = tc.OutputI(keyAR, countAR, trueK)
			for i := trueK + 1; i < tc.m; i++ {
				trueK = i
				if countAR[i-1]-initErr < min {
					min = countAR[i-1] - initErr
				}
				if tc.max.value <= min {
					break
				}
				initErr = tc.OutputI(keyAR, countAR, i)
			}
		}
		return trueK
	*/
}

func (tc *TopKCounter) OutputI(keyAR []Key, countAR []int, i int) int {
	keyAR[i] = tc.max.elemHead.k
	countAR[i] = tc.max.value
	initErr := tc.max.elemHead.initErr
	tc.max.elemHead = tc.max.elemHead.next
	if tc.max.elemHead == nil && tc.max != tc.min {
		tc.max = tc.max.prev
		tc.max.next = nil
	} else {
		if tc.max.elemHead != nil {
			tc.max.elemHead.prev = nil
		}
	}
	return initErr
}

func (tc *TopKCounter) Reset() {
	tc.numElem = 0
	tc.min.next = nil
	tc.min.prev = nil
	tc.min.elemHead = nil
	tc.max = tc.min
	tc.buckIndex = 0
}
