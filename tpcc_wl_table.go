package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/spinlock"
)

const (
	CAP_NEWORDER_ENTRY = 10000
	DIST_COUNT         = 10
)

type NoEntry struct {
	padding1   [PADDING]byte
	o_id_array [CAP_NEWORDER_ENTRY]int64
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
	nKeys       int64
	isPartition bool
	mode        int
	delLock     []SpinLockPad
	padding2    [PADDING]byte
}

func MakeNewOrderTable(warehouse int, isPartition bool, mode int) *NewOrderTable {
	noTable := &NewOrderTable{
		nKeys:       0,
		isPartition: isPartition,
		mode:        mode,
		delLock:     make([]SpinLockPad, warehouse*DIST_COUNT),
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
	w_id := int64(k[BIT0])
	d_id := int64(k[BIT8])
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
	index := int(k[BIT0])*DIST_COUNT + int(k[BIT8])

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
		index := int(k[BIT0])*DIST_COUNT + int(k[BIT8])
		no.head[index].rec.WUnlock(nil)
		no.delLock[index].Unlock()
	}
}

func (no *NewOrderTable) DeleteRecord(k Key, partNum int) error {
	index := int(k[BIT0])*DIST_COUNT + int(k[BIT8])
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
	if !no.isPartition {
		index := int(k[BIT0])*DIST_COUNT + int(k[BIT8])
		entry := no.tail[index]
		entry.rec.WLock(nil)
	}
	return nil
}

func (no *NewOrderTable) InsertRecord(k Key, partNum int, rec Record) error {
	index := int(k[BIT0])*DIST_COUNT + int(k[BIT8])
	entry := no.tail[index]
	entry.o_id_array[entry.t] = rec.GetTuple().(*NewOrderTuple).no_o_id
	entry.t++
	if entry.t == CAP_NEWORDER_ENTRY {
		// New a Entry
		dRec := &DRecord{}
		dRec.tuple = &NewOrderTuple{
			no_w_id: int64(k[0]),
			no_d_id: int64(k[8]),
		}
		newEntry := &NoEntry{}
		newEntry.rec = dRec
		newEntry.h = 0
		newEntry.t = 0
		entry.next = newEntry
		no.tail[index] = newEntry
	}

	if !no.isPartition {
		entry.rec.WUnlock(nil)
	}

	return nil
}

func (no *NewOrderTable) ReleaseInsert(k Key, partNum int) {
	if !no.isPartition {
		index := int(k[0])*DIST_COUNT + int(k[8])
		no.tail[index].rec.WUnlock(nil)
	}
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

const (
	CAP_ORDER_ENTRY = 100
)

type OrderPart struct {
	padding1 [PADDING]byte
	spinlock.RWSpinlock
	o_id_map map[Key]*OrderEntry
	padding2 [PADDING]byte
}

type OrderEntry struct {
	padding1   [PADDING]byte
	o_id_array [CAP_ORDER_ENTRY]int64
	next       *OrderEntry
	t          int
	padding2   [PADDING]byte
}

type OrderTable struct {
	padding1    [PADDING]byte
	data        []Partition
	secIndex    []OrderPart
	nKeys       int64
	nParts      int
	isPartition bool
	mode        int
	shardHash   func(Key) int
	padding2    [PADDING]byte
}

func MakeOrderTable(nParts int, warehouse int, isPartition bool, mode int) *OrderTable {
	oTable := &OrderTable{
		data:        make([]Partition, nParts),
		secIndex:    make([]OrderPart, warehouse*DIST_COUNT),
		nKeys:       0,
		nParts:      nParts,
		isPartition: isPartition,
		mode:        mode,
	}

	for k := 0; k < nParts; k++ {
		oTable.data[k].shardedMap = make([]Shard, SHARDCOUNT)
		for i := 0; i < SHARDCOUNT; i++ {
			oTable.data[k].shardedMap[i].rows = make(map[Key]Record)
		}
	}

	for i := 0; i < warehouse*DIST_COUNT; i++ {
		oTable.secIndex[i].o_id_map = make(map[Key]*OrderEntry)
	}

	oTable.shardHash = func(k Key) int {
		return (int(k[BIT0])*3 + int(k[BIT8])*11 + int(k[BIT16])*13) % SHARDCOUNT
	}

	return oTable

}

func (o *OrderTable) CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error) {
	o.nKeys++

	if !o.isPartition {
		partNum = 0
	}

	// Insert Order
	shardNum := o.shardHash(k)
	shard := &o.data[partNum].shardedMap[shardNum]

	if _, ok := shard.rows[k]; ok {
		return nil, EDUPKEY //One record with that key has existed;
	}

	rec := MakeRecord(o, k, tuple)
	shard.rows[k] = rec

	// Insert OrderPart
	index := int(k[0])*DIST_COUNT + int(k[8])
	var keyAr [KEYLENTH]int64
	var cKey Key
	oTuple := tuple.(*OrderTuple)
	keyAr[0] = oTuple.o_w_id
	keyAr[1] = oTuple.o_d_id
	keyAr[2] = oTuple.o_c_id
	UKey(keyAr, &cKey)
	oPart := o.secIndex[index]
	oEntry, ok := oPart.o_id_map[cKey]
	if !ok {
		oEntry = &OrderEntry{
			next: nil,
			t:    0,
		}
		oPart.o_id_map[cKey] = oEntry
		oEntry.o_id_array[oEntry.t] = oTuple.o_id
		oEntry.t++
	} else {
		for oEntry.next != nil {
			oEntry = oEntry.next
		}

		if oEntry.t == CAP_ORDER_ENTRY {
			nextEntry := &OrderEntry{
				next: nil,
				t:    0,
			}
			nextEntry.o_id_array[nextEntry.t] = oTuple.o_id
			nextEntry.t++
			oEntry.next = nextEntry
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

	shardNum := o.shardHash(k)
	shard := &o.data[partNum].shardedMap[shardNum]

	if o.mode != PARTITION {
		shard.RLock()
		defer shard.RUnlock()
	}

	r, ok := shard.rows[k]
	if !ok {
		return nil, ENOKEY
	} else {
		return r, nil
	}
}

func (o *OrderTable) SetValueByID(k Key, partNum int, value Value, colNum int) error {

	if !o.isPartition {
		partNum = 0
	}

	shardNum := o.shardHash(k)
	shard := &o.data[partNum].shardedMap[shardNum]

	if o.mode != PARTITION {
		shard.RLock()
		defer shard.RUnlock()
	}

	r, ok := shard.rows[k]
	if !ok {
		return ENOKEY
	}

	r.SetValue(value, colNum)
	return nil
}

func (o *OrderTable) GetValueByID(k Key, partNum int, value Value, colNum int) error {

	if !o.isPartition {
		partNum = 0
	}

	shardNum := o.shardHash(k)
	shard := &o.data[partNum].shardedMap[shardNum]

	if o.mode != PARTITION {
		shard.RLock()
		defer shard.RUnlock()
	}

	r, ok := shard.rows[k]
	if !ok {
		return ENOKEY
	}

	r.GetValue(value, colNum)
	return nil
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

func (o *OrderTable) InsertRecord(k Key, partNum int, rec Record) error {
	o.nKeys++

	if !o.isPartition {
		partNum = 0
	}

	// Insert Order
	shardNum := o.shardHash(k)
	shard := &o.data[partNum].shardedMap[shardNum]

	index := int(k[0])*DIST_COUNT + int(k[8])
	oPart := &o.secIndex[index]

	//clog.Info("Write Waiting %v ", index)

	if !o.isPartition {
		oPart.Lock()
		defer oPart.Unlock()
		//defer clog.Info("Write Unlock %v", index)
	}

	if o.mode != PARTITION {
		shard.Lock()
		defer shard.Unlock()
	}

	if _, ok := shard.rows[k]; ok {
		return EDUPKEY //One record with that key has existed;
	}

	shard.rows[k] = rec

	// Insert OrderPart
	var keyAr [KEYLENTH]int64
	var cKey Key
	oTuple := rec.GetTuple().(*OrderTuple)
	keyAr[0] = oTuple.o_w_id
	keyAr[1] = oTuple.o_d_id
	keyAr[2] = oTuple.o_c_id
	UKey(keyAr, &cKey)

	oEntry, ok := oPart.o_id_map[cKey]
	if !ok {
		oEntry = &OrderEntry{
			next: nil,
			t:    0,
		}
		oPart.o_id_map[cKey] = oEntry
		oEntry.o_id_array[oEntry.t] = oTuple.o_id
		oEntry.t++
	} else {
		for oEntry.next != nil {
			oEntry = oEntry.next
		}

		if oEntry.t == CAP_ORDER_ENTRY {
			nextEntry := &OrderEntry{
				next: nil,
				t:    0,
			}
			nextEntry.o_id_array[nextEntry.t] = oTuple.o_id
			nextEntry.t++
			oEntry.next = nextEntry
		} else {
			oEntry.o_id_array[oEntry.t] = oTuple.o_id
			oEntry.t++
		}
	}

	return nil
}

func (o *OrderTable) ReleaseInsert(k Key, partNum int) {
}

func (o *OrderTable) GetValueBySec(k Key, partNum int, val Value) error {
	index := int(k[0])*DIST_COUNT + int(k[8])
	oPart := &o.secIndex[index]
	//clog.Info("Wating %v", index)

	if !o.isPartition {
		oPart.RLock()
		defer oPart.RUnlock()
		//defer clog.Info("Read UnLock %v", index)
	}

	iv := val.(*IntValue)
	oEntry, ok := oPart.o_id_map[k]
	if !ok {
		return ENOORDER
	}
	iv.intVal = oEntry.o_id_array[0]
	return nil
}

func (o *OrderTable) SetMode(mode int) {
	o.mode = mode
}

func (o *OrderTable) DeltaValueByID(k Key, partNum int, value Value, colNum int) error {

	if !o.isPartition {
		partNum = 0
	}

	shardNum := o.shardHash(k)
	shard := &o.data[partNum].shardedMap[shardNum]

	if o.mode != PARTITION {
		shard.RLock()
		defer shard.RUnlock()
	}

	r, ok := shard.rows[k]
	if !ok {
		return ENOKEY
	}

	r.DeltaValue(value, colNum)
	return nil
}

/*

type OrderLineTable struct {
	padding1    [PADDING]byte
	data        []*ShardedPart
	nKeys       int64
	isPhysical  bool
	isPartition bool
	mode        int
	padding2    [PADDING]byte
}

func MakeOrderLineTable(isPartition bool, isPhysical bool, mode int, warehouse int) *OrderLineTable {
	olTable := &OrderLineTable{
		nKeys:       0,
		isPartition: isPartition,
		isPhysical:  isPhysical,
		mode:        mode,
	}

	olTable.data = make([]*ShardedPart, warehouse*DIST_COUNT)
	for i := 0; i < warehouse*DIST_COUNT; i++ {
		sPart := &ShardedPart{
			datastore: make(map[Key]Record),
		}
		sPart.rwLock.SetTrial(SLTRIAL)
		oTable.data[i] = sPart
	}

	return oTable

}

func (ol *OrderLineTable) CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error) {
	index := int(k[0])*DIST_COUNT + int(k[8])

	// Insert ShardedPart
	sPart := ol.data[index]
	rec := MakeRecord(ol, k, tuple)
	sPart.datastore[k] = rec

	return rec, nil

}

func (ol *OrderLineTable) GetRecByID(k Key, partNum int) (Record, error) {
	index := int(k[0])*DIST_COUNT + int(k[8])
	sPart := ol.data[index]
	sPart.rwLock.RLock()
	defer sPart.rwLock.Unlock()

	rec, ok := sPart.datastore[k]
	if !ok {
		return nil, ENOKEY
	} else {
		return rec, nil
	}
}

func (ol *OrderLineTable) SetValueByID(k Key, partNum int, value Value, colNum int) error {
	index := int(k[0])*DIST_COUNT + int(k[8])
	sPart := ol.data[index]
	sPart.rwLock.RLock()
	defer sPart.rwLock.Unlock()

	rec, ok := sPart.datastore[k]
	if !ok {
		return ENOKEY
	} else {
		rec.SetValue(value, colNum)
		return nil
	}

}

func (ol *OrderLineTable) GetValueByID(k Key, partNum int, val Value, colNum int) error {
	index := int(k[0])*DIST_COUNT + int(k[8])
	sPart := ol.data[index]
	sPart.rwLock.RLock()
	defer sPart.rwLock.Unlock()

	rec, ok := sPart.datastore[k]
	if !ok {
		return ENOKEY
	} else {
		rec.GetValue(value, colNum)
		return nil
	}
}

func (ol *OrderLineTable) PrepareDelete(k Key, partNum int) (Record, error) {
	clog.Error("Order Table Not Support PrepareDelete")
	return nil, nil
}

func (ol *OrderLineTable) ReleaseDelete(k Key, partNum int) {
	clog.Error("Order Table Not Support ReleaseDelete")
}

func (ol *OrderLineTable) DeleteRecord(k Key, partNum int) error {
	clog.Error("Order Table Not Support DeleteRecord")
	return nil
}

func (ol *OrderLineTable) ReleaseInsert(k Key, partNum int) {
	clog.Error("Order Table Not Support ReleaseInsert")
}

func (ol *OrderLineTable) PrepareInsert(k Key, partNum int) error {
	clog.Error("Order Table Not Support PrepareInsert")
	return nil
}

func (ol *OrderLineTable) InsertRecord(k Key, partNum int, rec Record) error {
	index := int(k[0])*DIST_COUNT + int(k[8])

	// Insert ShardedPart
	sPart := ol.data[index]
	sPart.rwLock.Lock()
	defer sPart.rwLock.Unlock()

	_, ok := sPart.datastore[k]
	if ok {
		return EDUPKEY
	}

	sPart.datastore[k] = rec

	return nil
}

func (ol *OrderLineTable) GetValueBySec(k Key, partNum int, val Value) error {
	clog.Error("Order Line Table Not Support GetValueBySec")
} */

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
	c_id_array [CAP_CUSTOMER_ENTRY]int64
	next       *CustomerEntry
	t          int
	total      int
	padding2   [PADDING]byte
}

type CustomerTable struct {
	padding1    [PADDING]byte
	data        []Partition
	secIndex    []CustomerPart
	nKeys       int64
	isPartition bool
	nParts      int
	mode        int
	shardHash   func(Key) int
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

	cTable.shardHash = func(k Key) int {
		return (int(k[BIT0])*3 + int(k[BIT8])*11 + int(k[BIT16])*13) % SHARDCOUNT
	}

	return cTable

}

func (c *CustomerTable) CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error) {
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
	var keyAr [KEYLENTH]int64
	var cKey Key
	cTuple := tuple.(*CustomerTuple)
	keyAr[0] = cTuple.c_w_id
	keyAr[1] = cTuple.c_d_id
	UKey(keyAr, &cKey)
	for i := 0; i < cTuple.len_c_last; i++ {
		cKey[i+16] = cTuple.c_last[i]
	}
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

func (c *CustomerTable) InsertRecord(k Key, partNum int, rec Record) error {
	clog.Error("Customer Table Not Support InsertRecord")
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
	shardHash func(Key) int
	padding2  [PADDING]byte
}

func MakeHistoryTable(nParts int, warehouse int, isPartition bool, mode int) *HistoryTable {
	ht := &HistoryTable{}
	ht.shardHash = func(k Key) int {
		return (int(k[BIT0])*3 + int(k[BIT8])*11 + int(k[BIT16])*13) % SHARDCOUNT
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

func (h *HistoryTable) InsertRecord(k Key, partNum int, rec Record) error {
	shard := h.shards[h.shardHash(k)]
	shard.latch.Lock()
	defer shard.latch.Unlock()
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
