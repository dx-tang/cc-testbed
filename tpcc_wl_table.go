package testbed

import (
	"time"

	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/spinlock"
)

const (
	CAP_NEWORDER_ENTRY = 1000
	DIST_COUNT         = 10
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
	nKeys       int
	isPartition bool
	mode        int
	delLock     []SpinLockPad
	insertLock  []RWSpinLockPad
	iLock       spinlock.Spinlock
	padding2    [PADDING]byte
}

func MakeNewOrderTable(warehouse int, isPartition bool, mode int) *NewOrderTable {
	noTable := &NewOrderTable{
		nKeys:       0,
		isPartition: isPartition,
		mode:        mode,
		delLock:     make([]SpinLockPad, warehouse*DIST_COUNT),
		insertLock:  make([]RWSpinLockPad, warehouse*DIST_COUNT),
	}

	noTable.head = make([]*NoEntry, warehouse*DIST_COUNT+2*PADDINGINT64)
	noTable.head = noTable.head[PADDINGINT64 : PADDINGINT64+warehouse*DIST_COUNT]
	noTable.tail = make([]*NoEntry, warehouse*DIST_COUNT+2*PADDINGINT64)
	noTable.tail = noTable.tail[PADDINGINT64 : PADDINGINT64+warehouse*DIST_COUNT]

	for i := 0; i < warehouse; i++ {
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

func (no *NewOrderTable) CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error) {
	no.iLock.Lock()
	defer no.iLock.Unlock()

	w_id := k[KEY0]
	d_id := k[KEY1]
	index := w_id*DIST_COUNT + d_id
	noTuple := tuple.(*NewOrderTuple)
	entry := no.tail[index]
	var retRec Record
	if entry.t < CAP_NEWORDER_ENTRY { // Not Full in this entry
		entry.o_id_array[entry.t] = noTuple.no_o_id
		entry.t++
		retRec = entry.rec
	} else {
		// New a Entry
		dRec := &DRecord{}
		dRec.tuple = &NewOrderTuple{
			no_w_id: w_id,
			no_d_id: d_id,
		}
		newEntry := &NoEntry{}
		newEntry.rec = dRec
		newEntry.h = 0
		newEntry.t = 0
		entry.next = newEntry
		no.tail[index] = newEntry
		// Add into new entry
		newEntry.o_id_array[newEntry.t] = noTuple.no_o_id
		newEntry.t++
		retRec = dRec
	}

	return retRec, nil
}

func (no *NewOrderTable) GetRecByID(k Key, partNum int) (Record, error) {
	clog.Error("New Order Table Not Support GetRecByID")
	return nil, nil
}

func (no *NewOrderTable) SetValueByID(k Key, partNum int, value Value, colNum int) error {
	clog.Error("New Order Table Not Support SetValueByID")
	return nil
}

func (no *NewOrderTable) GetValueByID(k Key, partNum int, val Value, colNum int) error {
	clog.Error("New Order Table Not Support GetValueByID")
	return nil
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
			entry.rec.WUnlock(nil)
			no.delLock[index].Unlock()
		}
		return nil, ENODEL
	}
}

func (no *NewOrderTable) ReleaseDelete(k Key, partNum int) {
	if !no.isPartition {
		index := k[KEY0]*DIST_COUNT + k[KEY1]
		no.head[index].rec.WUnlock(nil)
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
		entry.rec.WUnlock(nil)
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

func (no *NewOrderTable) InsertRecord(recs []InsertRec) error {
	for i, _ := range recs {
		iRec := &recs[i]
		k := iRec.k

		index := k[KEY0]*DIST_COUNT + k[KEY1]
		//if !no.isPartition {
		//	no.delLock[index].Lock()
		//}

		if !no.isPartition {
			no.insertLock[index].Lock()
		}

		entry := no.tail[index]
		entry.o_id_array[entry.t] = iRec.rec.GetTuple().(*NewOrderTuple).no_o_id
		entry.t++
		if entry.t == CAP_NEWORDER_ENTRY {
			// New a Entry
			dRec := &DRecord{}
			dRec.tuple = &NewOrderTuple{
				no_w_id: k[KEY0],
				no_d_id: k[KEY1],
			}
			newEntry := &NoEntry{}
			newEntry.rec = dRec
			newEntry.h = 0
			newEntry.t = 0
			entry.next = newEntry
			no.tail[index] = newEntry
		}

		if !no.isPartition {
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

func (no *NewOrderTable) SetMode(mode int) {
	no.mode = mode
}

func (no *NewOrderTable) DeltaValueByID(k Key, partNum int, value Value, colNum int) error {
	clog.Error("New Order Table Not Support DeltaValueByID")
	return nil
}

func (no *NewOrderTable) BulkLoad(table Table) {
	var compKey Key
	tuple := &NewOrderTuple{}
	rec := MakeRecord(no, compKey, tuple)
	iRecs := make([]InsertRec, 1)
	iRecs[0].rec = rec
	start := time.Now()
	for i, entry := range no.head {
		tuple.no_w_id = i / DIST_COUNT
		tuple.no_d_id = i % DIST_COUNT
		iRecs[0].k[KEY0] = tuple.no_w_id
		iRecs[0].k[KEY1] = tuple.no_d_id
		iRecs[0].partNum = tuple.no_w_id
		for entry != nil {
			for _, k := range entry.o_id_array {
				tuple.no_o_id = k
				table.InsertRecord(iRecs)
			}
			entry = entry.next
		}
	}
	clog.Info("NewOrder Iteration Take %.2fs", time.Since(start).Seconds())
}

const (
	CAP_ORDER_SEC_ENTRY    = 5
	CAP_ORDER_BUCKET_ENTRY = 5
	CAP_BUCKET_COUNT       = 2000
)

var orderbucketcount int

type OrderSecPart struct {
	padding1 [PADDING]byte
	spinlock.RWSpinlock
	o_id_map map[Key]*OrderSecEntry
	padding2 [PADDING]byte
}

type OrderSecEntry struct {
	padding1   [PADDING]byte
	o_id_array [CAP_ORDER_SEC_ENTRY]int
	before     *OrderSecEntry
	t          int
	padding2   [PADDING]byte
}

type OrderPart struct {
	padding1 [PADDING]byte
	buckets  []OrderBucket
	padding2 [PADDING]byte
}

type OrderBucket struct {
	padding1 [PADDING]byte
	spinlock.RWSpinlock
	tail     *OrderBucketEntry
	padding2 [PADDING]byte
}

type OrderBucketEntry struct {
	padding1 [PADDING]byte
	oRecs    [CAP_ORDER_BUCKET_ENTRY]Record
	keys     [CAP_ORDER_BUCKET_ENTRY]Key
	before   *OrderBucketEntry
	t        int
	padding2 [PADDING]byte
}

type OrderTable struct {
	padding1    [PADDING]byte
	data        []OrderPart
	secIndex    []OrderSecPart
	nKeys       int
	nParts      int
	isPartition bool
	mode        int
	bucketHash  func(k Key) int
	iLock       spinlock.Spinlock
	padding2    [PADDING]byte
}

func MakeOrderTable(nParts int, warehouse int, isPartition bool, mode int) *OrderTable {
	oTable := &OrderTable{
		data:        make([]OrderPart, nParts),
		secIndex:    make([]OrderSecPart, warehouse*DIST_COUNT),
		nKeys:       0,
		nParts:      nParts,
		isPartition: isPartition,
		mode:        mode,
	}

	orderbucketcount = CAP_BUCKET_COUNT * DIST_COUNT * warehouse / nParts

	for k := 0; k < nParts; k++ {
		oTable.data[k].buckets = make([]OrderBucket, orderbucketcount)
		for i := 0; i < orderbucketcount; i++ {
			oTable.data[k].buckets[i].tail = &OrderBucketEntry{}
		}
	}

	var cKey Key
	for i := 0; i < warehouse*DIST_COUNT; i++ {
		cKey[KEY0] = i / DIST_COUNT
		cKey[KEY1] = i % DIST_COUNT
		oTable.secIndex[i].o_id_map = make(map[Key]*OrderSecEntry)
		for j := 0; j < 3000; j++ {
			cKey[KEY2] = j
			entry := &OrderSecEntry{}
			oTable.secIndex[i].o_id_map[cKey] = entry
		}
	}

	if isPartition {
		oTable.bucketHash = func(k Key) int {
			oid := int64(k[KEY2])*DIST_COUNT + int64(k[KEY1])
			return int(oid % int64(orderbucketcount))
		}
	} else {
		oTable.bucketHash = func(k Key) int {
			oid := int64(k[KEY2]*(*NumPart))*DIST_COUNT + int64(k[KEY0]*DIST_COUNT+k[KEY1])
			return int(oid % int64(orderbucketcount))
		}
	}

	return oTable

}

func (o *OrderTable) CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error) {
	o.iLock.Lock()
	defer o.iLock.Unlock()

	o.nKeys++

	if !o.isPartition {
		partNum = 0
	}

	// Insert Order
	bucketNum := o.bucketHash(k)
	bucket := &o.data[partNum].buckets[bucketNum]

	rec := MakeRecord(o, k, tuple)

	cur := bucket.tail.t
	if cur == CAP_ORDER_BUCKET_ENTRY {
		obe := &OrderBucketEntry{
			before: bucket.tail,
		}
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
			t:      0,
		}
		oPart.o_id_map[cKey] = oEntry
		oEntry.o_id_array[oEntry.t] = oTuple.o_id
		oEntry.t++
	} else {

		if oEntry.t == CAP_ORDER_SEC_ENTRY {
			nextEntry := &OrderSecEntry{
				before: nil,
				t:      0,
			}
			nextEntry.o_id_array[nextEntry.t] = oTuple.o_id
			nextEntry.t++
			nextEntry.before = oEntry
			oPart.o_id_map[cKey] = nextEntry
		} else {
			oEntry.o_id_array[oEntry.t] = oTuple.o_id
			oEntry.t++
		}

	}

	return rec, nil

}

func (o *OrderTable) GetRecByID(k Key, partNum int) (Record, error) {

	if !o.isPartition {
		partNum = 0
	}

	bucketNum := o.bucketHash(k)
	bucket := &o.data[partNum].buckets[bucketNum]

	if o.mode != PARTITION {
		bucket.RLock()
	}

	tail := bucket.tail
	for tail != nil {
		for i := tail.t - 1; i >= 0; i-- {
			if tail.keys[i] == k {
				if o.mode != PARTITION {
					bucket.RUnlock()
				}
				return tail.oRecs[i], nil
			}
		}
		tail = tail.before
	}

	if o.mode != PARTITION {
		bucket.RUnlock()
	}

	return nil, ENOKEY

}

func (o *OrderTable) SetValueByID(k Key, partNum int, value Value, colNum int) error {

	if !o.isPartition {
		partNum = 0
	}

	bucketNum := o.bucketHash(k)
	bucket := &o.data[partNum].buckets[bucketNum]

	if o.mode != PARTITION {
		bucket.RLock()
	}

	tail := bucket.tail
	for tail != nil {
		for i := tail.t - 1; i >= 0; i-- {
			if tail.keys[i] == k {
				tail.oRecs[i].SetValue(value, colNum)
				if o.mode != PARTITION {
					bucket.RUnlock()
				}
				return nil
			}
		}
		tail = tail.before
	}

	if o.mode != PARTITION {
		bucket.RUnlock()
	}

	return ENOKEY
}

func (o *OrderTable) GetValueByID(k Key, partNum int, value Value, colNum int) error {
	if !o.isPartition {
		partNum = 0
	}

	bucketNum := o.bucketHash(k)
	bucket := &o.data[partNum].buckets[bucketNum]

	if o.mode != PARTITION {
		bucket.RLock()
	}

	tail := bucket.tail
	for tail != nil {
		for i := tail.t - 1; i >= 0; i-- {
			if tail.keys[i] == k {
				tail.oRecs[i].GetValue(value, colNum)
				if o.mode != PARTITION {
					bucket.RUnlock()
				}
				return nil
			}
		}
		tail = tail.before
	}

	if o.mode != PARTITION {
		bucket.RUnlock()
	}

	return ENOKEY
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

func (o *OrderTable) InsertRecord(recs []InsertRec) error {
	o.nKeys += len(recs)

	for i, _ := range recs {
		iRec := &recs[i]
		partNum := iRec.partNum
		k := iRec.k
		rec := iRec.rec

		if !o.isPartition {
			partNum = 0
		}

		// Insert Order
		bucketNum := o.bucketHash(k)
		bucket := &o.data[partNum].buckets[bucketNum]

		index := k[KEY0]*DIST_COUNT + k[KEY1]
		oPart := &o.secIndex[index]

		if !o.isPartition {
			oPart.Lock()
		}

		if o.mode != PARTITION {
			bucket.Lock()
		}

		cur := bucket.tail.t
		if cur == CAP_ORDER_BUCKET_ENTRY {
			obe := &OrderBucketEntry{
				before: bucket.tail,
			}
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
				nextEntry := &OrderSecEntry{
					before: nil,
					t:      0,
				}
				nextEntry.o_id_array[nextEntry.t] = oTuple.o_id
				nextEntry.t++
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
		if o.mode != PARTITION {
			bucket.Unlock()
		}

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

func (o *OrderTable) SetMode(mode int) {
	o.mode = mode
}

func (o *OrderTable) DeltaValueByID(k Key, partNum int, value Value, colNum int) error {

	if !o.isPartition {
		partNum = 0
	}

	bucketNum := o.bucketHash(k)
	bucket := &o.data[partNum].buckets[bucketNum]

	if o.mode != PARTITION {
		bucket.RLock()
	}

	tail := bucket.tail
	for tail != nil {
		for i := tail.t; i >= 0; i-- {
			if tail.keys[i] == k {
				tail.oRecs[i].DeltaValue(value, colNum)
				if o.mode != PARTITION {
					bucket.RUnlock()
				}
				return nil
			}
		}
		tail = tail.before
	}

	if o.mode != PARTITION {
		bucket.RUnlock()
	}

	return ENOKEY
}

func (o *OrderTable) BulkLoad(table Table) {
	iRecs := make([]InsertRec, 1)
	start := time.Now()
	for i, _ := range o.data {
		part := &o.data[i]
		iRecs[0].partNum = i
		for j, _ := range part.buckets {
			bucket := &part.buckets[j]
			tail := bucket.tail
			for tail != nil {
				for p := tail.t - 1; p >= 0; p-- {
					iRecs[0].k = tail.keys[p]
					iRecs[0].rec = tail.oRecs[p]
					table.InsertRecord(iRecs)
				}
				tail = tail.before
			}
		}
	}

	/*
		for i, _ := range o.secIndex {
			secPart := &o.secIndex[i]
			for k, _ := range secPart.o_id_map {
				if k == compKey {
					nKeys++
				}
			}
		}
	*/
	clog.Info("OrderTable Bulkload Takes %.2fs", time.Since(start).Seconds())
}

const (
	CAP_CUSTOMER_ENTRY = 5
)

type CustomerPart struct {
	padding1 [PADDING]byte
	c_id_map map[Key]*CustomerEntry
	padding2 [PADDING]byte
}

type CustomerEntry struct {
	padding1   [PADDING]byte
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
	mode        int
	shardHash   func(Key) int
	iLock       spinlock.Spinlock
	padding2    [PADDING]byte
}

func MakeCustomerTable(nParts int, warehouse int, isPartition bool, mode int) *CustomerTable {
	cTable := &CustomerTable{
		nKeys:       0,
		isPartition: isPartition,
		nParts:      nParts,
		mode:        mode,
	}

	cTable.data = make([]Partition, nParts)
	cTable.secIndex = make([]CustomerPart, nParts)

	for k := 0; k < nParts; k++ {
		cTable.secIndex[k].c_id_map = make(map[Key]*CustomerEntry)

		cTable.data[k].shardedMap = make([]Shard, SHARDCOUNT)
		for i := 0; i < SHARDCOUNT; i++ {
			cTable.data[k].shardedMap[i].rows = make(map[Key]Record)
		}
	}

	if isPartition {
		cTable.shardHash = func(k Key) int {
			hash := int64(k[KEY2]*DIST_COUNT) + int64(k[KEY1])
			return int(hash % SHARDCOUNT)
		}
	} else {
		cTable.shardHash = func(k Key) int {
			hash := int64(k[KEY2]*(*NumPart)*DIST_COUNT) + int64(k[KEY0]*DIST_COUNT+k[KEY1])
			return int(hash % SHARDCOUNT)
		}
	}

	return cTable

}

func (c *CustomerTable) CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error) {
	c.iLock.Lock()
	defer c.iLock.Unlock()

	// Insert Partition
	c.nKeys++

	if !c.isPartition {
		partNum = 0
	}

	shardNum := c.shardHash(k)
	shard := &c.data[partNum].shardedMap[shardNum]
	rec := MakeRecord(c, k, tuple)
	shard.rows[k] = rec

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

func (c *CustomerTable) GetRecByID(k Key, partNum int) (Record, error) {

	if !c.isPartition {
		partNum = 0
	}

	shardNum := c.shardHash(k)
	shard := &c.data[partNum].shardedMap[shardNum]

	r, ok := shard.rows[k]
	if !ok {
		return nil, ENOKEY
	} else {
		return r, nil
	}
}

func (c *CustomerTable) SetValueByID(k Key, partNum int, value Value, colNum int) error {

	if !c.isPartition {
		partNum = 0
	}

	shardNum := c.shardHash(k)
	shard := &c.data[partNum].shardedMap[shardNum]

	r, ok := shard.rows[k]
	if !ok {
		return ENOKEY
	}

	r.SetValue(value, colNum)
	return nil
}

func (c *CustomerTable) GetValueByID(k Key, partNum int, value Value, colNum int) error {

	if !c.isPartition {
		partNum = 0
	}

	shardNum := c.shardHash(k)
	shard := &c.data[partNum].shardedMap[shardNum]

	r, ok := shard.rows[k]
	if !ok {
		return ENOKEY
	}

	r.GetValue(value, colNum)
	return nil
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

func (c *CustomerTable) InsertRecord(recs []InsertRec) error {
	for i, _ := range recs {
		rec := recs[i].rec
		k := recs[i].k
		partNum := recs[i].partNum
		tuple := rec.GetTuple()

		c.nKeys++

		if !c.isPartition {
			partNum = 0
		}

		shardNum := c.shardHash(k)
		shard := &c.data[partNum].shardedMap[shardNum]
		shard.rows[k] = rec

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

func (c *CustomerTable) SetMode(mode int) {
	c.mode = mode
}

func (c *CustomerTable) DeltaValueByID(k Key, partNum int, value Value, colNum int) error {

	if !c.isPartition {
		partNum = 0
	}

	shardNum := c.shardHash(k)
	shard := &c.data[partNum].shardedMap[shardNum]

	r, ok := shard.rows[k]
	if !ok {
		return ENOKEY
	}

	r.DeltaValue(value, colNum)
	return nil
}

func (c *CustomerTable) BulkLoad(table Table) {
	iRecs := make([]InsertRec, 1)
	start := time.Now()
	for i, _ := range c.data {
		part := &c.data[i]
		iRecs[0].partNum = i
		for j, _ := range part.shardedMap {
			shard := &part.shardedMap[j]
			for k, v := range shard.rows {
				iRecs[0].k = k
				iRecs[0].rec = v
				table.InsertRecord(iRecs)
			}
		}
	}
	clog.Info("CustomerTable Bulkload Takes %.2fs", time.Since(start).Seconds())
}

const (
	CAP_HISTORY_ENTRY = 1000
)

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
	head     *HistoryEntry
	tail     *HistoryEntry
	padding2 [PADDING]byte
}

type HistoryTable struct {
	padding1  [PADDING]byte
	shards    [SHARDCOUNT]HistoryShard
	iLock     spinlock.Spinlock
	shardHash func(Key) int
	padding2  [PADDING]byte
}

func MakeHistoryTable(nParts int, warehouse int, isPartition bool, mode int) *HistoryTable {
	ht := &HistoryTable{}
	ht.shardHash = func(k Key) int {
		return (int(k[KEY0])*3 + int(k[KEY1])*11 + int(k[KEY2])) % SHARDCOUNT
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

func (h *HistoryTable) CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error) {
	h.iLock.Lock()
	defer h.iLock.Unlock()

	shard := h.shards[h.shardHash(k)]
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

func (h *HistoryTable) GetRecByID(k Key, partNum int) (Record, error) {
	clog.Error("HistoryTable Table Not Support GetRecByID")
	return nil, nil
}

func (h *HistoryTable) SetValueByID(k Key, partNum int, value Value, colNum int) error {
	clog.Error("HistoryTable Table Not Support SetValueByID")
	return nil
}

func (h *HistoryTable) GetValueByID(k Key, partNum int, value Value, colNum int) error {
	clog.Error("HistoryTable Table Not Support GetValueByID")
	return nil
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

func (h *HistoryTable) InsertRecord(recs []InsertRec) error {
	for i, _ := range recs {
		k := recs[i].k
		rec := recs[i].rec

		shard := h.shards[h.shardHash(k)]
		shard.latch.Lock()
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

func (h *HistoryTable) SetMode(mode int) {
	return
}

func (h *HistoryTable) DeltaValueByID(k Key, partNum int, value Value, colNum int) error {
	clog.Error("HistoryTable Table Not Support SetValueByID")
	return nil
}

func (h *HistoryTable) BulkLoad(table Table) {
}

var olbucketcount int

const (
	CAP_ORDERLINE_BUCKET_ENTRY = 50
)

type OrderLinePart struct {
	padding1 [PADDING]byte
	buckets  []OrderLineBucket
	padding2 [PADDING]byte
}

type OrderLineBucket struct {
	padding1 [PADDING]byte
	spinlock.RWSpinlock
	tail     *OrderLineBucketEntry
	nKeys    int
	padding2 [PADDING]byte
}

type OrderLineBucketEntry struct {
	padding1 [PADDING]byte
	oRecs    [CAP_ORDERLINE_BUCKET_ENTRY]Record
	keys     [CAP_ORDERLINE_BUCKET_ENTRY]Key
	before   *OrderLineBucketEntry
	t        int
	padding2 [PADDING]byte
}

type OrderLineTable struct {
	padding1    [PADDING]byte
	data        []OrderLinePart
	nKeys       int
	nParts      int
	isPartition bool
	mode        int
	bucketHash  func(k Key) int
	iLock       spinlock.Spinlock
	padding2    [PADDING]byte
}

func MakeOrderLineTable(nParts int, warehouse int, isPartition bool, mode int) *OrderLineTable {
	olTable := &OrderLineTable{
		data:        make([]OrderLinePart, nParts),
		nKeys:       0,
		nParts:      nParts,
		isPartition: isPartition,
		mode:        mode,
	}

	olbucketcount = CAP_BUCKET_COUNT * DIST_COUNT * warehouse / nParts

	for k := 0; k < nParts; k++ {
		olTable.data[k].buckets = make([]OrderLineBucket, olbucketcount)
		for i := 0; i < olbucketcount; i++ {
			olTable.data[k].buckets[i].tail = &OrderLineBucketEntry{}
		}
	}

	if isPartition {
		olTable.bucketHash = func(k Key) int {
			oid := int64(k[KEY2])*DIST_COUNT + int64(k[KEY1])
			return int(oid % int64(orderbucketcount))
		}
	} else {
		olTable.bucketHash = func(k Key) int {
			oid := int64(k[KEY2]*(*NumPart))*DIST_COUNT + int64(k[KEY0]*DIST_COUNT+k[KEY1])
			return int(oid % int64(orderbucketcount))
		}
	}

	return olTable

}
func (ol *OrderLineTable) CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error) {
	ol.iLock.Lock()
	defer ol.iLock.Unlock()

	ol.nKeys++

	if !ol.isPartition {
		partNum = 0
	}

	// Insert Order
	bucketNum := ol.bucketHash(k)
	bucket := &ol.data[partNum].buckets[bucketNum]
	bucket.nKeys++

	rec := MakeRecord(ol, k, tuple)

	cur := bucket.tail.t
	if cur == CAP_ORDERLINE_BUCKET_ENTRY {
		obe := &OrderLineBucketEntry{
			before: bucket.tail,
		}
		bucket.tail = obe
		cur = bucket.tail.t
	}
	bucket.tail.keys[cur] = k
	bucket.tail.oRecs[cur] = rec
	bucket.tail.t++

	return rec, nil
}

func (ol *OrderLineTable) GetRecByID(k Key, partNum int) (Record, error) {

	if !ol.isPartition {
		partNum = 0
	}

	bucketNum := ol.bucketHash(k)
	bucket := &ol.data[partNum].buckets[bucketNum]

	if ol.mode != PARTITION {
		bucket.RLock()
	}

	tail := bucket.tail
	for tail != nil {
		for i := tail.t - 1; i >= 0; i-- {
			if tail.keys[i] == k {
				if ol.mode != PARTITION {
					bucket.RUnlock()
				}
				return tail.oRecs[i], nil
			}
		}
		tail = tail.before
	}

	if ol.mode != PARTITION {
		bucket.RUnlock()
	}

	return nil, ENOKEY

}

func (ol *OrderLineTable) SetValueByID(k Key, partNum int, value Value, colNum int) error {

	if !ol.isPartition {
		partNum = 0
	}

	bucketNum := ol.bucketHash(k)
	bucket := &ol.data[partNum].buckets[bucketNum]

	if ol.mode != PARTITION {
		bucket.RLock()
	}

	tail := bucket.tail
	for tail != nil {
		for i := tail.t - 1; i >= 0; i-- {
			if tail.keys[i] == k {
				tail.oRecs[i].SetValue(value, colNum)
				if ol.mode != PARTITION {
					bucket.RUnlock()
				}
				return nil
			}
		}
		tail = tail.before
	}

	if ol.mode != PARTITION {
		bucket.RUnlock()
	}

	return ENOKEY
}

func (ol *OrderLineTable) GetValueByID(k Key, partNum int, value Value, colNum int) error {
	if !ol.isPartition {
		partNum = 0
	}

	bucketNum := ol.bucketHash(k)
	bucket := &ol.data[partNum].buckets[bucketNum]

	if ol.mode != PARTITION {
		bucket.RLock()
	}

	tail := bucket.tail
	for tail != nil {
		for i := tail.t - 1; i >= 0; i-- {
			if tail.keys[i] == k {
				tail.oRecs[i].GetValue(value, colNum)
				if ol.mode != PARTITION {
					bucket.RUnlock()
				}
				return nil
			}
		}
		tail = tail.before
	}

	if ol.mode != PARTITION {
		bucket.RUnlock()
	}

	return ENOKEY
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

func (ol *OrderLineTable) InsertRecord(recs []InsertRec) error {
	ol.nKeys += len(recs)

	partNum := recs[0].partNum
	if !ol.isPartition {
		partNum = 0
	}

	bucketNum := ol.bucketHash(recs[0].k)
	bucket := &ol.data[partNum].buckets[bucketNum]

	if ol.mode != PARTITION {
		bucket.Lock()
	}

	for i, _ := range recs {
		iRec := &recs[i]
		k := iRec.k
		rec := iRec.rec

		cur := bucket.tail.t
		if cur == CAP_ORDERLINE_BUCKET_ENTRY {
			obe := &OrderLineBucketEntry{
				before: bucket.tail,
			}
			bucket.tail = obe
			cur = bucket.tail.t
		}
		bucket.tail.keys[cur] = k
		bucket.tail.oRecs[cur] = rec
		bucket.tail.t++

	}

	if ol.mode != PARTITION {
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

func (ol *OrderLineTable) SetMode(mode int) {
	ol.mode = mode
}

func (ol *OrderLineTable) DeltaValueByID(k Key, partNum int, value Value, colNum int) error {

	if !ol.isPartition {
		partNum = 0
	}

	bucketNum := ol.bucketHash(k)
	bucket := &ol.data[partNum].buckets[bucketNum]

	if ol.mode != PARTITION {
		bucket.RLock()
	}

	tail := bucket.tail
	for tail != nil {
		for i := tail.t - 1; i >= 0; i-- {
			if tail.keys[i] == k {
				tail.oRecs[i].DeltaValue(value, colNum)
				if ol.mode != PARTITION {
					bucket.RUnlock()
				}
				return nil
			}
		}
		tail = tail.before
	}

	if ol.mode != PARTITION {
		bucket.RUnlock()
	}

	return ENOKEY
}

func (ol *OrderLineTable) BulkLoad(table Table) {
	iRecs := make([]InsertRec, 1)
	start := time.Now()
	for i, _ := range ol.data {
		part := &ol.data[i]
		iRecs[0].partNum = i
		for j, _ := range part.buckets {
			bucket := &part.buckets[j]
			tail := bucket.tail
			for tail != nil {
				for p := tail.t - 1; p >= 0; p-- {
					iRecs[0].k = tail.keys[p]
					iRecs[0].rec = tail.oRecs[p]
					table.InsertRecord(iRecs)
				}
				tail = tail.before
			}
		}
	}

	clog.Info("OrderLineTable Iteration Takes %.2fs", time.Since(start).Seconds())
}
