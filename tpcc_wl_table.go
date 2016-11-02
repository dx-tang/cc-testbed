package testbed

import (
	"time"

	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/spinlock"
)

const (
	DIST_COUNT = 10
)

type NoEntry struct {
	padding1   [PADDING]byte
	o_id_array [CAP_NEWORDER_ENTRY]int
	rec        Record
	next       *NoEntry
	h          int
	t          int
	padding2   [PADDING]byte
}

type NewOrderTable struct {
	padding1    [PADDING]byte
	head        []*NoEntry
	tail        []*NoEntry
	initKeys    []int
	nKeys       int
	isPartition bool
	useLatch    []bool
	delLock     []SpinLockPad
	insertLock  []RWSpinLockPad
	iLock       spinlock.Spinlock
	padding2    [PADDING]byte
}

func MakeNewOrderTable(warehouse int, isPartition bool, useLatch []bool) *NewOrderTable {
	noTable := &NewOrderTable{
		initKeys:    make([]int, warehouse*DIST_COUNT+2*PADDINGINT),
		nKeys:       0,
		isPartition: isPartition,
		delLock:     make([]SpinLockPad, warehouse*DIST_COUNT),
		insertLock:  make([]RWSpinLockPad, warehouse*DIST_COUNT),
	}

	noTable.initKeys = noTable.initKeys[PADDINGINT : PADDINGINT+warehouse*DIST_COUNT]

	noTable.head = make([]*NoEntry, warehouse*DIST_COUNT+2*PADDINGINT64)
	noTable.head = noTable.head[PADDINGINT64 : PADDINGINT64+warehouse*DIST_COUNT]
	noTable.tail = make([]*NoEntry, warehouse*DIST_COUNT+2*PADDINGINT64)
	noTable.tail = noTable.tail[PADDINGINT64 : PADDINGINT64+warehouse*DIST_COUNT]

	for i := 0; i < warehouse; i++ {
		noTable.useLatch[i] = useLatch[i]
		for j := 0; j < DIST_COUNT; j++ {
			dRec := &DRecord{}
			dRec.tuple = &NewOrderTuple{}
			entry := &NoEntry{}
			entry.rec = dRec
			entry.h = 0
			entry.t = 0

			noTable.head[i*DIST_COUNT+j] = entry
			noTable.tail[i*DIST_COUNT+j] = entry
		}
	}

	return noTable
}

func (no *NewOrderTable) CreateRecByID(k Key, partNum int, tuple Tuple, ia IndexAlloc) (Record, error) {
	w_id := k[KEY0]
	d_id := k[KEY1]
	index := w_id*DIST_COUNT + d_id
	noTuple := tuple.(*NewOrderTuple)
	no.initKeys[index]++
	entry := no.tail[index]
	var retRec Record
	if entry.t < CAP_NEWORDER_ENTRY { // Not Full in this entry
		entry.o_id_array[entry.t] = noTuple.no_o_id
		entry.t++
		retRec = entry.rec
		if entry.t == CAP_NEWORDER_ENTRY {
			newEntry := ia.GetEntry().(*NoEntry)
			tuple := newEntry.rec.GetTuple().(*NewOrderTuple)
			tuple.no_w_id = k[KEY0]
			tuple.no_d_id = k[KEY1]
			entry.next = newEntry
			no.tail[index] = newEntry
		}
	} else {
		// New a Entry
		newEntry := ia.GetEntry().(*NoEntry)
		tuple := newEntry.rec.GetTuple().(*NewOrderTuple)
		tuple.no_w_id = k[KEY0]
		tuple.no_d_id = k[KEY1]
		entry.next = newEntry
		no.tail[index] = newEntry
		// Add into new entry
		newEntry.o_id_array[newEntry.t] = noTuple.no_o_id
		newEntry.t++
		retRec = newEntry.rec
	}

	return retRec, nil
}

func (no *NewOrderTable) GetRecByID(k Key, partNum int) (Record, Bucket, uint64, error) {
	clog.Error("New Order Table Not Support GetRecByID")
	return nil, nil, 0, nil
}

func (no *NewOrderTable) PrepareDelete(k Key, partNum int) (Record, error) {
	index := k[KEY0]*DIST_COUNT + k[KEY1]

	if !no.isPartition {
		no.delLock[index].Lock() // Lock index
	}

	entry := no.head[index]

	if !no.isPartition {
		entry.rec.WLock(nil)
	}

	if entry.h != entry.t {
		noTuple := entry.rec.GetTuple().(*NewOrderTuple)
		noTuple.no_o_id = entry.o_id_array[entry.h]
		return entry.rec, nil
	} else {
		if !no.isPartition {
			entry.rec.WUnlock(nil, 0)
			no.delLock[index].Unlock()
		}
		return nil, ENODEL
	}
}

func (no *NewOrderTable) ReleaseDelete(k Key, partNum int) {
	if !no.isPartition {
		index := k[KEY0]*DIST_COUNT + k[KEY1]
		no.head[index].rec.WUnlock(nil, 0)
		no.delLock[index].Unlock()
	}
}

func (no *NewOrderTable) DeleteRecord(k Key, partNum int) error {
	index := k[KEY0]*DIST_COUNT + k[KEY1]
	entry := no.head[index]
	entry.h++
	if entry.h == CAP_NEWORDER_ENTRY && entry.next != nil { //No Data in this entry
		no.head[index] = entry.next
		entry.next = nil
	}

	if !no.isPartition {
		entry.rec.WUnlock(nil, 0)
		no.delLock[index].Unlock()
	}

	return nil
}

func (no *NewOrderTable) PrepareInsert(k Key, partNum int) error {
	/*if !no.isPartition {
		index := int(k[BIT0])*DIST_COUNT + int(k[BIT4])
		no.insertLock[index].RLock()
		entry := no.tail[index]
		no.insertLock[index].Unlock()
		//entry.rec.WLock(nil)
		//clog.Info("Get")
	}*/
	return nil
}

func (no *NewOrderTable) InsertRecord(recs []InsertRec, ia IndexAlloc) error {
	for i, _ := range recs {
		iRec := &recs[i]
		k := iRec.k

		index := k[KEY0]*DIST_COUNT + k[KEY1]
		//if no.mode != PARTITION {
		//	no.delLock[index].Lock()
		//}

		if no.useLatch[k[KEY0]] {
			no.insertLock[index].Lock()
		}

		entry := no.tail[index]

		entry.o_id_array[entry.t] = iRec.rec.GetTuple().(*NewOrderTuple).no_o_id
		entry.t++
		if entry.t == CAP_NEWORDER_ENTRY {
			newEntry := ia.GetEntry().(*NoEntry)
			tuple := newEntry.rec.GetTuple().(*NewOrderTuple)
			tuple.no_w_id = k[KEY0]
			tuple.no_d_id = k[KEY1]
			entry.next = newEntry
			no.tail[index] = newEntry
		}

		//if !no.isPartition {
		if no.useLatch[k[KEY0]] {
			//entry.rec.WUnlock(nil)
			no.insertLock[index].Unlock()
		}
	}

	return nil
}

func (no *NewOrderTable) ReleaseInsert(k Key, partNum int) {
	//if !no.isPartition {
	//	index := int(k[BIT0])*DIST_COUNT + int(k[BIT4])
	//no.delLock[index].Lock()
	//	no.tail[index].rec.WUnlock(nil)
	//}
}

func (no *NewOrderTable) GetValueBySec(k Key, partNum int, val Value) error {
	clog.Error("New Order Table Not Support GetValueBySec")
	return nil
}

func (no *NewOrderTable) SetLatch(useLatch bool) {
	for i := 0; i < len(no.useLatch); i++ {
		no.useLatch[i] = useLatch
	}
}

func (no *NewOrderTable) BulkLoad(table Table, ia IndexAlloc, begin int, end int, partitioner Partitioner) {
	var compKey Key
	tuple := &NewOrderTuple{}
	rec := MakeRecord(no, compKey, tuple)
	iRecs := make([]InsertRec, 1)
	iRecs[0].rec = rec
	start := time.Now()
	for i, entry := range no.head {
		if i/DIST_COUNT < begin || i/DIST_COUNT >= end {
			continue
		}
		tuple.no_w_id = i / DIST_COUNT
		tuple.no_d_id = i % DIST_COUNT
		iRecs[0].k[KEY0] = tuple.no_w_id
		iRecs[0].k[KEY1] = tuple.no_d_id
		iRecs[0].partNum = tuple.no_w_id
		for entry != nil {
			for _, k := range entry.o_id_array {
				tuple.no_o_id = k
				table.InsertRecord(iRecs, ia)
			}
			entry = entry.next
		}
	}
	clog.Debug("NewOrder Iteration Take %.2fs", time.Since(start).Seconds())
}

func (no *NewOrderTable) MergeLoad(table Table, ia IndexAlloc, begin int, end int, partitioner Partitioner) {
	start := time.Now()
	no.BulkLoad(table, ia, begin, end, partitioner)
	clog.Debug("NewOrder Merging Take %.2fs", time.Since(start).Seconds())
}

func (no *NewOrderTable) Reset() {
	for i := 0; i < len(no.head); i++ {
		initKeys := no.initKeys[i]
		head := no.head[i]
		for {
			if initKeys >= CAP_NEWORDER_ENTRY {
				initKeys -= CAP_NEWORDER_ENTRY
			} else {
				head.t = initKeys
				head.next = nil
				break
			}
			head = head.next
		}
		no.tail[i] = head
	}
}

func (no *NewOrderTable) Clean() {

}

type OrderSecPart struct {
	padding1 [PADDING]byte
	spinlock.RWSpinlock
	o_id_map      map[Key]*OrderSecEntry
	o_id_head_map map[Key]*OrderSecEntry
	initKey_map   map[Key]int
	padding2      [PADDING]byte
}

type OrderSecEntry struct {
	padding1   [PADDING]byte
	o_id_array [CAP_ORDER_SEC_ENTRY]int
	before     *OrderSecEntry
	next       *OrderSecEntry
	t          int
	padding2   [PADDING]byte
}

type OrderPart struct {
	padding1 [PADDING]byte
	uselatch bool
	buckets  []OrderBucket
	padding2 [PADDING]byte
}

type OrderBucket struct {
	padding1 [PADDING]byte
	spinlock.RWSpinlock
	iLock    IndexLock
	initKeys int
	tail     *OrderBucketEntry
	head     *OrderBucketEntry
	padding2 [PADDING]byte
}

type OrderBucketEntry struct {
	padding1 [PADDING]byte
	oRecs    [CAP_ORDER_BUCKET_ENTRY]Record
	keys     [CAP_ORDER_BUCKET_ENTRY]Key
	before   *OrderBucketEntry
	next     *OrderBucketEntry
	t        int
	padding2 [PADDING]byte
}

type OrderTable struct {
	padding1         [PADDING]byte
	data             []OrderPart
	secIndex         []OrderSecPart
	nKeys            int
	nParts           int
	isPartition      bool
	bucketHash       func(k Key, orderbucketcount int) int
	iLock            spinlock.Spinlock
	orderbucketcount int
	padding2         [PADDING]byte
}

func MakeOrderTable(nParts int, warehouse int, isPartition bool, useLatch []bool) *OrderTable {
	oTable := &OrderTable{
		data:        make([]OrderPart, nParts),
		secIndex:    make([]OrderSecPart, warehouse*DIST_COUNT),
		nKeys:       0,
		nParts:      nParts,
		isPartition: isPartition,
	}

	oTable.orderbucketcount = CAP_BUCKET_COUNT * DIST_COUNT * warehouse / nParts

	for k := 0; k < nParts; k++ {
		oTable.data[k].buckets = make([]OrderBucket, oTable.orderbucketcount)
		oTable.data[k].uselatch = useLatch[k]
		for i := 0; i < oTable.orderbucketcount; i++ {
			oTable.data[k].buckets[i].tail = &OrderBucketEntry{
				before: nil,
				next:   nil,
			}
			oTable.data[k].buckets[i].head = oTable.data[k].buckets[i].tail
		}
	}

	var cKey Key
	for i := 0; i < warehouse*DIST_COUNT; i++ {
		cKey[KEY0] = i / DIST_COUNT
		cKey[KEY1] = i % DIST_COUNT
		oTable.secIndex[i].o_id_map = make(map[Key]*OrderSecEntry)
		oTable.secIndex[i].initKey_map = make(map[Key]int)
		oTable.secIndex[i].o_id_head_map = make(map[Key]*OrderSecEntry)
		for j := 0; j < 3000; j++ {
			cKey[KEY2] = j
			entry := &OrderSecEntry{
				before: nil,
				next:   nil,
			}
			oTable.secIndex[i].o_id_map[cKey] = entry
			oTable.secIndex[i].o_id_head_map[cKey] = entry
			oTable.secIndex[i].initKey_map[cKey] = 0
		}
	}

	if isPartition {
		oTable.bucketHash = func(k Key, orderbucketcount int) int {
			oid := int64(k[KEY2])*DIST_COUNT + int64(k[KEY1])
			return int(oid % int64(orderbucketcount))
		}
	} else {
		oTable.bucketHash = func(k Key, orderbucketcount int) int {
			oid := int64(k[KEY2]*(*NumPart))*DIST_COUNT + int64(k[KEY0]*DIST_COUNT+k[KEY1])
			return int(oid % int64(orderbucketcount))
		}
	}

	return oTable

}

func (o *OrderTable) CreateRecByID(k Key, partNum int, tuple Tuple, ia IndexAlloc) (Record, error) {

	if !o.isPartition {
		partNum = 0
	}

	// Insert Order
	bucketNum := o.bucketHash(k, o.orderbucketcount)

	bucket := &o.data[partNum].buckets[bucketNum]

	if !o.isPartition {
		bucket.Lock()
	}

	bucket.initKeys++

	rec := MakeRecord(o, k, tuple)

	cur := bucket.tail.t
	if cur == CAP_ORDER_BUCKET_ENTRY {
		obe := ia.GetEntry().(*OrderBucketEntry)
		obe.before = bucket.tail
		bucket.tail.next = obe
		bucket.tail = obe
		cur = bucket.tail.t
	}
	bucket.tail.keys[cur] = k
	bucket.tail.oRecs[cur] = rec
	bucket.tail.t++

	// Insert OrderSecPart
	index := k[KEY0]*DIST_COUNT + k[KEY1]
	var cKey Key
	oTuple := tuple.(*OrderTuple)
	cKey[KEY0] = oTuple.o_w_id
	cKey[KEY1] = oTuple.o_d_id
	cKey[KEY2] = oTuple.o_c_id
	oPart := o.secIndex[index]
	oEntry, ok := oPart.o_id_map[cKey]
	if !ok {
		oEntry = &OrderSecEntry{
			before: nil,
			next:   nil,
			t:      0,
		}
		oPart.o_id_map[cKey] = oEntry
		oPart.o_id_head_map[cKey] = oEntry
		oPart.initKey_map[cKey] = 0
		oEntry.o_id_array[oEntry.t] = oTuple.o_id
		oEntry.t++
	} else {

		if oEntry.t == CAP_ORDER_SEC_ENTRY {
			nextEntry := ia.GetSecEntry().(*OrderSecEntry)
			nextEntry.o_id_array[nextEntry.t] = oTuple.o_id
			nextEntry.t++
			nextEntry.before = oEntry
			oEntry.next = nextEntry
			oPart.o_id_map[cKey] = nextEntry
		} else {
			oEntry.o_id_array[oEntry.t] = oTuple.o_id
			oEntry.t++
		}
		initKey := oPart.initKey_map[cKey]
		oPart.initKey_map[cKey] = initKey + 1

	}

	if !o.isPartition {
		bucket.Unlock()
	}

	return rec, nil

}

func (o *OrderTable) GetRecByID(k Key, partNum int) (Record, Bucket, uint64, error) {

	if !o.isPartition {
		partNum = 0
	}

	bucketNum := o.bucketHash(k, o.orderbucketcount)
	bucket := &o.data[partNum].buckets[bucketNum]

	//version := uint64(0)
	if o.data[partNum].uselatch {
		bucket.RLock()
	}

	/*
		if o.mode == LOCKING {
			bucket.RLock()
		} else if o.mode == OCC {
			version = bucket.iLock.Read()
		}
	*/

	tail := bucket.tail
	for tail != nil {
		for i := tail.t - 1; i >= 0; i-- {
			if tail.keys[i] == k {
				if o.data[partNum].uselatch {
					bucket.RUnlock()
				}
				return tail.oRecs[i], nil, 0, nil
			}
		}
		tail = tail.before
	}

	if o.data[partNum].uselatch {
		bucket.RUnlock()
	}

	return nil, nil, 0, ENOKEY

}

func (o *OrderTable) PrepareDelete(k Key, partNum int) (Record, error) {
	clog.Error("Order Table Not Support PrepareDelete")
	return nil, nil
}

func (o *OrderTable) DeleteRecord(k Key, partNum int) error {
	clog.Error("Order Table Not Support DeleteRecord")
	return nil
}

func (o *OrderTable) ReleaseDelete(k Key, partNum int) {
	clog.Error("Order Table Not Support ReleaseDelete")
}

func (o *OrderTable) PrepareInsert(k Key, partNum int) error {
	return nil
}

func (o *OrderTable) InsertRecord(recs []InsertRec, ia IndexAlloc) error {
	//o.nKeys += len(recs)

	for i, _ := range recs {
		iRec := &recs[i]
		partNum := iRec.partNum
		k := iRec.k
		rec := iRec.rec

		if !o.isPartition {
			partNum = 0
		}

		// Insert Order
		bucketNum := o.bucketHash(k, o.orderbucketcount)
		bucket := &o.data[partNum].buckets[bucketNum]

		index := k[KEY0]*DIST_COUNT + k[KEY1]
		oPart := &o.secIndex[index]

		if !o.isPartition {
			oPart.Lock()
		}

		if o.data[k[KEY0]].uselatch {
			bucket.Lock()
		} /*else if o.mode == OCC {
			bucket.iLock.Lock()
		}*/

		cur := bucket.tail.t
		if cur == CAP_ORDER_BUCKET_ENTRY {
			obe := ia.GetEntry().(*OrderBucketEntry)
			obe.before = bucket.tail
			bucket.tail.next = obe
			bucket.tail = obe
			cur = bucket.tail.t
		}
		bucket.tail.keys[cur] = k
		bucket.tail.oRecs[cur] = rec
		bucket.tail.t++

		// Insert OrderPart
		var cKey Key
		oTuple := rec.GetTuple().(*OrderTuple)
		cKey[KEY0] = oTuple.o_w_id
		cKey[KEY1] = oTuple.o_d_id
		cKey[KEY2] = oTuple.o_c_id

		oEntry, ok := oPart.o_id_map[cKey]
		if !ok {
			oEntry = &OrderSecEntry{
				before: nil,
				t:      0,
			}
			oPart.o_id_map[cKey] = oEntry
			oEntry.o_id_array[oEntry.t] = oTuple.o_id
			oEntry.t++
		} else {
			if oEntry.t == CAP_ORDER_SEC_ENTRY {
				nextEntry := ia.GetSecEntry().(*OrderSecEntry)
				nextEntry.o_id_array[nextEntry.t] = oTuple.o_id
				nextEntry.t++
				oEntry.next = nextEntry
				nextEntry.before = oEntry
				oPart.o_id_map[cKey] = nextEntry
			} else {
				oEntry.o_id_array[oEntry.t] = oTuple.o_id
				oEntry.t++
			}
		}

		if !o.isPartition {
			oPart.Unlock()
		}

		if o.data[k[KEY0]].uselatch {
			bucket.Unlock()
		} /* else if o.mode == OCC {
			bucket.iLock.Unlock()
		}*/

	}

	return nil
}

func (o *OrderTable) ReleaseInsert(k Key, partNum int) {
}

func (o *OrderTable) GetValueBySec(k Key, partNum int, val Value) error {
	index := k[KEY0]*DIST_COUNT + k[KEY1]
	oPart := &o.secIndex[index]

	if !o.isPartition {
		oPart.RLock()
	}

	iv := val.(*IntValue)
	oEntry, ok := oPart.o_id_map[k]
	if !ok || oEntry.t == 0 {
		if !o.isPartition {
			oPart.RUnlock()
		}
		return ENOORDER
	}
	iv.intVal = oEntry.o_id_array[oEntry.t-1]

	if !o.isPartition {
		oPart.RUnlock()
	}
	return nil
}

func (o *OrderTable) SetLatch(useLatch bool) {
	for i := 0; i < len(o.data); i++ {
		o.data[i].uselatch = useLatch
	}
}
func (o *OrderTable) BulkLoad(table Table, ia IndexAlloc, begin int, end int, partitioner Partitioner) {
	iRecs := make([]InsertRec, 1)
	start := time.Now()
	for i, _ := range o.data {
		part := &o.data[i]
		for j, _ := range part.buckets {
			bucket := &part.buckets[j]
			tail := bucket.tail
			for tail != nil {
				for p := tail.t - 1; p >= 0; p-- {
					if tail.keys[p][0] < begin || tail.keys[p][0] >= end {
						continue
					}
					iRecs[0].k = tail.keys[p]
					iRecs[0].rec = tail.oRecs[p]
					iRecs[0].partNum = iRecs[0].k[0]
					table.InsertRecord(iRecs, ia)
				}
				tail = tail.before
			}
		}
	}

	clog.Debug("OrderTable Bulkload Takes %.2fs", time.Since(start).Seconds())
}

func (o *OrderTable) MergeLoad(table Table, ia IndexAlloc, begin int, end int, partitioner Partitioner) {
	iRecs := make([]InsertRec, 1)
	start := time.Now()
	for i := begin; i < end; i++ {
		part := &o.data[i]
		for j, _ := range part.buckets {
			bucket := &part.buckets[j]
			tail := bucket.tail
			for tail != nil {
				for p := tail.t - 1; p >= 0; p-- {
					iRecs[0].k = tail.keys[p]
					iRecs[0].rec = tail.oRecs[p]
					iRecs[0].partNum = 0
					table.InsertRecord(iRecs, ia)
				}
				tail = tail.before
			}
		}
	}
	clog.Debug("OrderTable Merging Takes %.2fs", time.Since(start).Seconds())
}

func (o *OrderTable) Reset() {
	for i, _ := range o.data {
		part := &o.data[i]
		for j, _ := range part.buckets {
			bucket := &part.buckets[j]
			initKeys := bucket.initKeys
			head := bucket.head
			for {
				if initKeys > CAP_ORDER_BUCKET_ENTRY {
					initKeys -= CAP_ORDER_BUCKET_ENTRY
				} else {
					head.t = initKeys
					head.next = nil
					bucket.tail = head
					break
				}
				head = head.next
			}
		}
	}

	for i, _ := range o.secIndex {
		secPart := &o.secIndex[i]
		for k, oPart := range secPart.o_id_head_map {
			initKeys := secPart.initKey_map[k]
			head := oPart
			for {
				if initKeys >= CAP_ORDER_SEC_ENTRY {
					initKeys -= CAP_ORDER_SEC_ENTRY
				} else {
					head.t = initKeys
					head.next = nil
					secPart.o_id_map[k] = head
					break
				}
				head = head.next
			}
		}
	}
}

func (o *OrderTable) Clean() {

}

type CustomerPart struct {
	padding1 [PADDING]byte
	spinlock.Spinlock
	c_id_map map[Key]*CustomerEntry
	useLatch bool
	padding2 [PADDING]byte
}

type CustomerEntry struct {
	padding1 [PADDING]byte
	spinlock.Spinlock
	c_id_array [CAP_CUSTOMER_ENTRY]int
	next       *CustomerEntry
	t          int
	total      int
	padding2   [PADDING]byte
}

type CustomerTable struct {
	padding1    [PADDING]byte
	data        []Partition
	secIndex    []CustomerPart
	nKeys       int
	isPartition bool
	nParts      int
	shardHash   func(Key) int
	iLock       spinlock.Spinlock
	padding2    [PADDING]byte
}

func MakeCustomerTable(numEntries int, nParts int, warehouse int, isPartition bool, useLatch []bool) *CustomerTable {
	cTable := &CustomerTable{
		nKeys:       0,
		isPartition: isPartition,
		nParts:      nParts,
	}

	cTable.data = make([]Partition, nParts)
	cTable.secIndex = make([]CustomerPart, nParts)

	for k := 0; k < nParts; k++ {
		cTable.secIndex[k].c_id_map = make(map[Key]*CustomerEntry)
		cTable.secIndex[k].useLatch = useLatch[k]

		cTable.data[k].ht = NewHashTable(numEntries, isPartition, useLatch[k], CUSTOMER)
	}

	var cKey [KEYLENTH]int
	for i := 0; i < warehouse*DIST_COUNT; i++ {
		cKey[0] = i / DIST_COUNT
		cKey[1] = i % DIST_COUNT
		for j := 0; j < C_LAST_PER_DIST; j++ {
			cKey[2] = j
			ce := &CustomerEntry{}
			if isPartition {
				cTable.secIndex[cKey[0]].c_id_map[cKey] = ce
			} else {
				cTable.secIndex[0].c_id_map[cKey] = ce
			}
		}
	}

	/*if isPartition {
		cTable.shardHash = func(k Key) int {
			hash := int64(k[KEY2]*DIST_COUNT) + int64(k[KEY1])
			return int(hash % SHARDCOUNT)
		}
	} else {
		cTable.shardHash = func(k Key) int {
			hash := int64(k[KEY2]*(*NumPart)*DIST_COUNT) + int64(k[KEY0]*DIST_COUNT+k[KEY1])
			return int(hash % SHARDCOUNT)
		}
	}*/

	return cTable

}

func (c *CustomerTable) CreateRecByID(k Key, partNum int, tuple Tuple, iaAR IndexAlloc) (Record, error) {

	if !c.isPartition {
		partNum = 0
	}

	cPart := &c.secIndex[partNum]

	if !c.isPartition {
		cPart.Lock()
		defer cPart.Unlock()
	}

	rec := MakeRecord(c, k, tuple)
	c.data[partNum].ht.Put(k, rec, iaAR)

	// Insert CustomerPart
	var cKey Key
	cTuple := tuple.(*CustomerTuple)
	cKey[KEY0] = cTuple.c_w_id
	cKey[KEY1] = cTuple.c_d_id
	cKey[KEY2] = cTuple.c_last
	//for i := 0; i < cTuple.len_c_last; i++ {
	//	cKey[i+16] = cTuple.c_last[i]
	//}
	cEntry, ok := cPart.c_id_map[cKey]
	if !ok {
		cEntry = &CustomerEntry{
			next: nil,
			t:    0,
		}
		cPart.c_id_map[cKey] = cEntry
		cEntry.c_id_array[cEntry.t] = cTuple.c_id
		cEntry.t++
		cEntry.total++
	} else {
		cEntry.total++
		for cEntry.next != nil {
			cEntry = cEntry.next
		}

		if cEntry.t == CAP_CUSTOMER_ENTRY {
			nextEntry := &CustomerEntry{
				next: nil,
				t:    0,
			}
			nextEntry.c_id_array[nextEntry.t] = cTuple.c_id
			nextEntry.t++
			cEntry.next = nextEntry
		} else {
			cEntry.c_id_array[cEntry.t] = cTuple.c_id
			cEntry.t++
		}

	}

	return rec, nil

}

func (c *CustomerTable) GetRecByID(k Key, partNum int) (Record, Bucket, uint64, error) {

	if !c.isPartition {
		partNum = 0
	}

	rec, ok := c.data[partNum].ht.Get(k)

	if !ok {
		return nil, nil, 0, ENOKEY
	} else {
		return rec, nil, 0, nil
	}
}

func (c *CustomerTable) PrepareDelete(k Key, partNum int) (Record, error) {
	clog.Error("Customer Table Not Support PrepareDelete")
	return nil, nil
}

func (c *CustomerTable) ReleaseDelete(k Key, partNum int) {
	clog.Error("Customer Table Not Support ReleaseDelete")
}

func (c *CustomerTable) DeleteRecord(k Key, partNum int) error {
	clog.Error("Customer Table Not Support DeleteRecord")
	return nil
}

func (c *CustomerTable) ReleaseInsert(k Key, partNum int) {
	clog.Error("Customer Table Not Support ReleaseInsert")
}

func (c *CustomerTable) PrepareInsert(k Key, partNum int) error {
	clog.Error("Customer Table Not Support PrepareInsert")
	return nil
}

func (c *CustomerTable) InsertRecord(recs []InsertRec, ia IndexAlloc) error {
	for i, _ := range recs {
		rec := recs[i].rec
		k := recs[i].k
		partNum := recs[i].partNum
		tuple := rec.GetTuple()

		//c.nKeys++

		if !c.isPartition {
			partNum = 0
		}

		c.data[partNum].ht.Put(k, rec, ia)

		// Insert CustomerPart
		var cKey Key
		cTuple := tuple.(*CustomerTuple)
		cKey[KEY0] = cTuple.c_w_id
		cKey[KEY1] = cTuple.c_d_id
		cKey[KEY2] = cTuple.c_last
		//for i := 0; i < cTuple.len_c_last; i++ {
		//	cKey[i+16] = cTuple.c_last[i]
		//}
		cPart := &c.secIndex[partNum]
		cEntry, ok := cPart.c_id_map[cKey]
		if !ok {
			cEntry = &CustomerEntry{
				next: nil,
				t:    0,
			}
			cPart.c_id_map[cKey] = cEntry
			cEntry.c_id_array[cEntry.t] = cTuple.c_id
			cEntry.t++
			cEntry.total++
		} else {
			immEntry := cEntry
			if cPart.useLatch {
				immEntry.Lock()
			}
			cEntry.total++
			for cEntry.next != nil {
				cEntry = cEntry.next
			}

			if cEntry.t == CAP_CUSTOMER_ENTRY {
				nextEntry := &CustomerEntry{
					next: nil,
					t:    0,
				}
				nextEntry.c_id_array[nextEntry.t] = cTuple.c_id
				nextEntry.t++
				cEntry.next = nextEntry
			} else {
				cEntry.c_id_array[cEntry.t] = cTuple.c_id
				cEntry.t++
			}
			if cPart.useLatch {
				immEntry.Unlock()
			}
		}

	}
	return nil
}

func (c *CustomerTable) GetValueBySec(k Key, partNum int, val Value) error {

	if !c.isPartition {
		partNum = 0
	}

	cPart := &c.secIndex[partNum]
	cEntry := cPart.c_id_map[k]
	pos := cEntry.total / 2

	iv := val.(*IntValue)
	for pos >= CAP_CUSTOMER_ENTRY {
		pos -= CAP_CUSTOMER_ENTRY
		cEntry = cEntry.next
	}
	iv.intVal = cEntry.c_id_array[pos]
	return nil
}

func (c *CustomerTable) SetLatch(useLatch bool) {
	for i, _ := range c.data {
		c.secIndex[i].useLatch = useLatch
		c.data[i].ht.SetLatch(useLatch)
	}
}

func (c *CustomerTable) BulkLoad(table Table, ia IndexAlloc, begin int, end int, partitioner Partitioner) {
	recs := make([]InsertRec, 1)
	start := time.Now()
	for i, _ := range c.data {
		part := &c.data[i]
		for j, _ := range part.ht.bucket {
			bucket := &part.ht.bucket[j]
			for bucket != nil {
				for p := 0; p < bucket.cur; p++ {
					k := bucket.keyArray[p]
					if k[0] < begin || k[0] >= end {
						continue
					}
					recs[0].k = k
					recs[0].rec = bucket.recArray[p]
					recs[0].partNum = k[0]
					table.InsertRecord(recs, ia)
				}
				bucket = bucket.next
			}
		}
	}
	clog.Debug("CustomerTable Bulkload Takes %.2fs", time.Since(start).Seconds())
}

func (c *CustomerTable) MergeLoad(table Table, ia IndexAlloc, begin int, end int, partitioner Partitioner) {
	recs := make([]InsertRec, 1)
	start := time.Now()
	for i := begin; i < end; i++ {
		part := &c.data[i]
		for j, _ := range part.ht.bucket {
			bucket := &part.ht.bucket[j]
			for bucket != nil {
				for p := 0; p < bucket.cur; p++ {
					k := bucket.keyArray[p]
					recs[0].k = k
					recs[0].rec = bucket.recArray[p]
					recs[0].partNum = 0
					table.InsertRecord(recs, ia)
				}
				bucket = bucket.next
			}
		}
	}
	clog.Debug("CustomerTable Merging Takes %.2fs", time.Since(start).Seconds())

}

func (c *CustomerTable) Reset() {
}

func (c *CustomerTable) Clean() {

}

type HistoryEntry struct {
	padding1 [PADDING]byte
	data     [CAP_HISTORY_ENTRY]Record
	index    int
	next     *HistoryEntry
	padding2 [PADDING]byte
}

type HistoryShard struct {
	padding1 [PADDING]byte
	latch    spinlock.Spinlock
	initKeys int
	head     *HistoryEntry
	tail     *HistoryEntry
	numEntry int
	padding2 [PADDING]byte
}

type HistoryTable struct {
	padding1    [PADDING]byte
	shards      [SHARDCOUNT]HistoryShard
	iLock       spinlock.Spinlock
	shardHash   func(Key) int
	isPartition bool
	padding2    [PADDING]byte
}

func MakeHistoryTable(nParts int, warehouse int, isPartition bool, useLatch []bool) *HistoryTable {
	ht := &HistoryTable{
		isPartition: isPartition,
	}

	ht.shardHash = func(k Key) int {
		return (k[KEY2]*(*NumPart*DIST_COUNT) + k[KEY0]*DIST_COUNT + k[KEY1]) % SHARDCOUNT
	}
	for i := 0; i < SHARDCOUNT; i++ {
		shard := &ht.shards[i]
		he := &HistoryEntry{
			index: 0,
			next:  nil,
		}
		shard.head = he
		shard.tail = he
	}

	return ht
}

func (h *HistoryTable) CreateRecByID(k Key, partNum int, tuple Tuple, ia IndexAlloc) (Record, error) {

	shard := &h.shards[h.shardHash(k)]
	shard.latch.Lock()
	defer shard.latch.Unlock()

	shard.initKeys++
	cur := shard.tail
	if cur.index == CAP_HISTORY_ENTRY {
		he := &HistoryEntry{
			index: 0,
			next:  nil,
		}
		cur.next = he
		shard.tail = he
		cur = he
	}
	rec := MakeRecord(h, k, tuple)
	cur.data[cur.index] = rec
	cur.index++

	return rec, nil
}

func (h *HistoryTable) GetRecByID(k Key, partNum int) (Record, Bucket, uint64, error) {
	clog.Error("HistoryTable Table Not Support GetRecByID")
	return nil, nil, 0, nil
}

func (h *HistoryTable) PrepareDelete(k Key, partNum int) (Record, error) {
	clog.Error("HistoryTable Table Not Support PrepareDelete")
	return nil, nil
}

func (h *HistoryTable) ReleaseDelete(k Key, partNum int) {
	clog.Error("HistoryTable Table Not Support ReleaseDelete")
}

func (h *HistoryTable) DeleteRecord(k Key, partNum int) error {
	clog.Error("HistoryTable Table Not Support DeleteRecord")
	return nil
}

func (h *HistoryTable) ReleaseInsert(k Key, partNum int) {
}

func (h *HistoryTable) PrepareInsert(k Key, partNum int) error {
	return nil
}

func (h *HistoryTable) InsertRecord(recs []InsertRec, ia IndexAlloc) error {
	for i, _ := range recs {
		k := recs[i].k
		rec := recs[i].rec

		shard := &h.shards[h.shardHash(k)]
		shard.latch.Lock()
		cur := shard.tail
		if cur.index == CAP_HISTORY_ENTRY {
			he := ia.GetEntry().(*HistoryEntry)
			cur.next = he
			shard.tail = he
			cur = he
		}
		cur.data[cur.index] = rec
		cur.index++
		shard.latch.Unlock()
	}

	return nil
}

func (h *HistoryTable) GetValueBySec(k Key, partNum int, val Value) error {
	clog.Error("History Table Not Support GetValueBySec")
	return nil
}

func (h *HistoryTable) SetLatch(useLatch bool) {
	return
}

func (h *HistoryTable) BulkLoad(table Table, ia IndexAlloc, begin int, end int, partitioner Partitioner) {

}

func (h *HistoryTable) MergeLoad(table Table, ia IndexAlloc, begin int, end int, partitioner Partitioner) {

}

func (h *HistoryTable) Reset() {
	for i, _ := range h.shards {
		shard := &h.shards[i]
		initKeys := shard.initKeys
		head := shard.head
		for {
			if initKeys > CAP_HISTORY_ENTRY {
				initKeys -= CAP_HISTORY_ENTRY
			} else {
				head.index = initKeys
				head.next = nil
				shard.tail = head
				break
			}
			head = head.next
		}
	}
}

func (h *HistoryTable) Clean() {

}

type OrderLinePart struct {
	padding1 [PADDING]byte
	buckets  []OrderLineBucket
	useLatch bool
	padding2 [PADDING]byte
}

type OrderLineBucket struct {
	padding1 [PADDING]byte
	spinlock.RWSpinlock
	iLock    IndexLock
	tail     *OrderLineBucketEntry
	head     *OrderLineBucketEntry
	initKeys int
	nKeys    int
	padding2 [PADDING]byte
}

type OrderLineBucketEntry struct {
	padding1 [PADDING]byte
	oRecs    [CAP_ORDERLINE_BUCKET_ENTRY]Record
	keys     [CAP_ORDERLINE_BUCKET_ENTRY]Key
	next     *OrderLineBucketEntry
	before   *OrderLineBucketEntry
	t        int
	padding2 [PADDING]byte
}

type OrderLineTable struct {
	padding1      [PADDING]byte
	data          []OrderLinePart
	nKeys         int
	nParts        int
	isPartition   bool
	useLatch      bool
	bucketHash    func(k Key, orderbucketcount int) int
	iLock         spinlock.Spinlock
	olbucketcount int
	padding2      [PADDING]byte
}

func MakeOrderLineTable(nParts int, warehouse int, isPartition bool, useLatch []bool) *OrderLineTable {
	olTable := &OrderLineTable{
		data:        make([]OrderLinePart, nParts),
		nKeys:       0,
		nParts:      nParts,
		isPartition: isPartition,
	}

	olTable.olbucketcount = CAP_BUCKET_COUNT * DIST_COUNT * warehouse / nParts

	for k := 0; k < nParts; k++ {
		olTable.data[k].useLatch = useLatch[k]
		olTable.data[k].buckets = make([]OrderLineBucket, olTable.olbucketcount)
		for i := 0; i < olTable.olbucketcount; i++ {
			olTable.data[k].buckets[i].tail = &OrderLineBucketEntry{
				next:   nil,
				before: nil,
			}
			olTable.data[k].buckets[i].head = olTable.data[k].buckets[i].tail
		}
	}

	if isPartition {
		olTable.bucketHash = func(k Key, olbucketcount int) int {
			oid := int64(k[KEY2])*DIST_COUNT + int64(k[KEY1])
			return int(oid % int64(olbucketcount))
		}
	} else {
		olTable.bucketHash = func(k Key, olbucketcount int) int {
			oid := int64(k[KEY2]*(*NumPart))*DIST_COUNT + int64(k[KEY0]*DIST_COUNT+k[KEY1])
			return int(oid % int64(olbucketcount))
		}
	}

	return olTable

}
func (ol *OrderLineTable) CreateRecByID(k Key, partNum int, tuple Tuple, ia IndexAlloc) (Record, error) {

	if !ol.isPartition {
		partNum = 0
	}

	// Insert Order
	bucketNum := ol.bucketHash(k, ol.olbucketcount)
	bucket := &ol.data[partNum].buckets[bucketNum]
	if !ol.isPartition {
		bucket.Lock()
		defer bucket.Unlock()
	}

	bucket.nKeys++
	bucket.initKeys++

	rec := MakeRecord(ol, k, tuple)

	cur := bucket.tail.t
	if cur == CAP_ORDERLINE_BUCKET_ENTRY {
		obe := ia.GetEntry().(*OrderLineBucketEntry)
		obe.before = bucket.tail
		bucket.tail.next = obe
		bucket.tail = obe
		cur = bucket.tail.t
	}
	bucket.tail.keys[cur] = k
	bucket.tail.oRecs[cur] = rec
	bucket.tail.t++

	return rec, nil
}

func (ol *OrderLineTable) GetRecByID(k Key, partNum int) (Record, Bucket, uint64, error) {

	if !ol.isPartition {
		partNum = 0
	}

	bucketNum := ol.bucketHash(k, ol.olbucketcount)
	bucket := &ol.data[partNum].buckets[bucketNum]

	//version := uint64(0)

	/*if ol.mode == LOCKING {
		bucket.RLock()
	} else if ol.mode == OCC {
		version = bucket.iLock.Read()
	}*/

	if ol.data[partNum].useLatch {
		bucket.RLock()
	}

	tail := bucket.tail
	for tail != nil {
		for i := tail.t - 1; i >= 0; i-- {
			if tail.keys[i] == k {
				if ol.useLatch {
					bucket.RUnlock()
				}
				return tail.oRecs[i], nil, 0, nil
			}
		}
		tail = tail.before
	}

	if ol.data[partNum].useLatch {
		bucket.RUnlock()
	}

	return nil, nil, 0, ENOKEY

}

func (ol *OrderLineTable) PrepareDelete(k Key, partNum int) (Record, error) {
	clog.Error("OrderLine Table Not Support PrepareDelete")
	return nil, nil
}

func (ol *OrderLineTable) DeleteRecord(k Key, partNum int) error {
	clog.Error("OrderLine Table Not Support DeleteRecord")
	return nil
}

func (ol *OrderLineTable) ReleaseDelete(k Key, partNum int) {
	clog.Error("OrderLine Table Not Support ReleaseDelete")
}

func (ol *OrderLineTable) PrepareInsert(k Key, partNum int) error {
	return nil
}

func (ol *OrderLineTable) InsertRecord(recs []InsertRec, ia IndexAlloc) error {
	//ol.nKeys += len(recs)

	partNum := recs[0].partNum
	if !ol.isPartition {
		partNum = 0
	}

	bucketNum := ol.bucketHash(recs[0].k, ol.olbucketcount)
	bucket := &ol.data[partNum].buckets[bucketNum]

	if ol.data[partNum].useLatch {
		bucket.Lock()
	}

	for i, _ := range recs {
		iRec := &recs[i]
		k := iRec.k
		rec := iRec.rec

		cur := bucket.tail.t
		if cur == CAP_ORDERLINE_BUCKET_ENTRY {
			obe := ia.GetEntry().(*OrderLineBucketEntry)
			obe.before = bucket.tail
			bucket.tail.next = obe
			bucket.tail = obe
			cur = bucket.tail.t
		}
		bucket.tail.keys[cur] = k
		bucket.tail.oRecs[cur] = rec
		bucket.tail.t++

	}

	if ol.data[partNum].useLatch {
		bucket.Unlock()
	}

	return nil
}

func (ol *OrderLineTable) ReleaseInsert(k Key, partNum int) {
}

func (ol *OrderLineTable) GetValueBySec(k Key, partNum int, val Value) error {
	clog.Error("OrderLine Table Not Support GetValueBySec")
	return nil
}

func (ol *OrderLineTable) SetLatch(useLatch bool) {
	for i := 0; i < len(ol.data); i++ {
		ol.data[i].useLatch = useLatch
	}
}

func (ol *OrderLineTable) BulkLoad(table Table, ia IndexAlloc, begin int, end int, partitioner Partitioner) {
	iRecs := make([]InsertRec, 1)
	start := time.Now()
	for i, _ := range ol.data {
		part := &ol.data[i]
		for j, _ := range part.buckets {
			bucket := &part.buckets[j]
			tail := bucket.tail
			for tail != nil {
				for p := tail.t - 1; p >= 0; p-- {
					if tail.keys[p][0] < begin || tail.keys[p][0] >= end {
						continue
					}
					iRecs[0].k = tail.keys[p]
					iRecs[0].rec = tail.oRecs[p]
					iRecs[0].partNum = iRecs[0].k[0]
					table.InsertRecord(iRecs, ia)
				}
				tail = tail.before
			}
		}
	}

	clog.Debug("OrderLineTable Bulkload Takes %.2fs", time.Since(start).Seconds())
}

func (ol *OrderLineTable) MergeLoad(table Table, ia IndexAlloc, begin int, end int, partitioner Partitioner) {
	iRecs := make([]InsertRec, 1)
	start := time.Now()
	for i := begin; i < end; i++ {
		part := &ol.data[i]
		for j, _ := range part.buckets {
			bucket := &part.buckets[j]
			tail := bucket.tail
			for tail != nil {
				for p := tail.t - 1; p >= 0; p-- {
					iRecs[0].k = tail.keys[p]
					iRecs[0].rec = tail.oRecs[p]
					iRecs[0].partNum = 0
					table.InsertRecord(iRecs, ia)
				}
				tail = tail.before
			}
		}
	}
	clog.Debug("OrderLineTable Merging Takes %.2fs", time.Since(start).Seconds())
}

func (ol *OrderLineTable) Reset() {
	for i := 0; i < len(ol.data); i++ {
		olPart := &ol.data[i]
		for j := 0; j < len(olPart.buckets); j++ {
			bucket := &olPart.buckets[j]
			head := bucket.head
			initKeys := bucket.initKeys
			bucket.nKeys = initKeys
			for {
				if initKeys > CAP_ORDERLINE_BUCKET_ENTRY {
					initKeys -= CAP_ORDERLINE_BUCKET_ENTRY
				} else {
					head.t = initKeys
					head.next = nil
					bucket.tail = head
					break
				}
				head = head.next
			}
		}
	}
}

func (ol *OrderLineTable) Clean() {

}
