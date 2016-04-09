package testbed

import (
	"container/list"

	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/spinlock"
)

const (
	CAP_NEWORDER_ENTRY = 1000
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
	isPhysical  bool
	isPartition bool
	delLock     []SpinLockPad
	padding2    [PADDING]byte
}

func MakeNewOrderTable(isPartition bool, isPhysical bool, warehouse int) *NewOrderTable {
	noTable := &NewOrderTable{
		head:        make([]*NoEntry, warehouse*DIST_COUNT),
		tail:        make([]*NoEntry, warehouse*DIST_COUNT),
		nKeys:       0,
		isPhysical:  isPhysical,
		isPartition: isPartition,
		delLock:     make([]SpinLockPad, warehouse*DIST_COUNT),
	}

	for i := 0; i < warehouse; i++ {
		for j := 0; j < DIST_COUNT; j++ {
			dRec := &DRecord{}
			dRec.lock.SetTrial(SLTRIAL)
			dRec.tuple = &NewOrderTuple{}
			entry := &NoEntry{}
			entry.rec = dRec
			entry.h = 0
			entry.t = 0

			noTable.head[i*DIST_COUNT+j] = entry
			noTable.tail[i*DIST_COUNT+j] = entry
			noTable.delLock[i*DIST_COUNT+j].SetTrial(SLTRIAL)
		}
	}

	return noTable
}

func (no *NewOrderTable) CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error) {
	w_id := int64(k[0])
	d_id := int64(k[8])
	noTuple := tuple.(*NewOrderTuple)
	entry := no.tail[w_id*DIST_COUNT+d_id]
	if entry.t < CAP_NEWORDER_ENTRY { // Not Full in this entry
		entry.o_id_array[entry.t] = noTuple.no_o_id
		entry.t++
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
		no.tail[k] = newEntry
		// Add into new entry
		newEntry.o_id_array[newEntry.t] = noTuple.no_o_id
		newEntry.t++
	}
}

func (no *NewOrderTable) GetRecByID(k Key, partNum int) (Record, error) {
	clog.Error("New Order Table Not Support GetRecByID")
}

func (no *NewOrderTable) SetValueByID(k Key, partNum int, value Value, colNum int) error {
	clog.Error("New Order Table Not Support SetValueByID")
}

func (no *NewOrderTable) GetValueByID(k Key, partNum int, val Value, colNum int) error {
	clog.Error("New Order Table Not Support GetValueByID")
}

func (no *NewOrderTable) PrepareDelete(k Key, partNum int) (Record, error) {
	index := int(k[0])*DIST_COUNT + int(k[8])
	no.delLock[index].Lock() // Lock index

	entry := no.head[index]
	entry.rec.WLock(0)
	if entry.h != entry.t {
		noTuple := entry.rec.GetTuple().(*NewOrderTuple)
		noTuple.no_o_id = entry.o_id_array[entry.h]
		return entry.rec
	} else {
		entry.rec.WUnlock()
		no.delLock[index].Unlock()
		return nil, ENODEL
	}
}

func (no *NewOrderTable) ReleaseDelete(k Key, partNum int) {
	index := int(k[0])*DIST_COUNT + int(k[8])
	no.head[index].rec.WUnlock()
	no.delLock[index].Unlock()
	return nil
}

func (no *NewOrderTable) DeleteRecord(k Key, partNum int) error {
	index := int(k[0])*DIST_COUNT + int(k[8])
	entry := no.head[index]
	entry.h++
	if entry.h == CAP_NEWORDER_ENTRY && entry.next != nil { //No Data in this entry
		no.head[index] = entry.next
		entry.next = nil
	}

	entry.rec.WUnlock()
	no.delLock[index].Unlock()
	return nil
}

func (no *NewOrderTable) ReleaseInsert(k Key, partNum int) {
	index := int(k[0])*DIST_COUNT + int(k[8])
	no.tail[index].rec.WUnlock()
	return nil
}

func (no *NewOrderTable) PrepareInsert(k Key, partNum int) error {
	index := int(k[0])*DIST_COUNT + int(k[8])

	entry := no.tail[index]
	entry.rec.WLock(0)
	return nil
}

func (no *NewOrderTable) InsertRecord(k Key, partNum int, rec Record) error {
	index := int(k[0])*DIST_COUNT + int(k[8])
	entry := no.tail[index]
	entry.o_id_array[t] = rec.GetTuple().(*NewOrderTuple).no_o_id
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
		no.tail[k] = newEntry
	}
	entry.rec.WUnlock()
}

func (no *NewOrderTable) GetValueBySec(k Key, partNum int, val Value) error {
	clog.Error("New Order Table Not Support GetValueBySec")
}

const (
	CAP_ORDER_ENTRY = 100
)

type ShardedPart struct {
	padding1  [PADDING]byte
	rwLock    spinlock.RWSpinlock
	datastore map[Key]Record
	padding2  [PADDING]byte
}

type OrderPart struct {
	padding1 [PADDING]byte
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
	data        []*ShardedPart
	secIndex    []*OrderPart
	nKeys       int64
	isPhysical  bool
	isPartition bool
	mode        int
	padding2    [PADDING]byte
}

func MakeOrderTable(isPartition bool, isPhysical bool, mode int, warehouse int) *OrderTable {
	oTable := &OrderTable{
		data:        make([]*ShardedPart, warehouse*DIST_COUNT),
		secIndex:    make([]*OrderPart, warehouse*DIST_COUNT),
		nKeys:       0,
		isPartition: isPartition,
		isPhysical:  isPhysical,
		mode:        mode,
	}

	for i := 0; i < warehouse*DIST_COUNT; i++ {
		sPart := &ShardedPart{
			datastore: make(map[Key]Record),
		}
		sPart.rwLock.SetTrial(SLTRIAL)
		oPart := &OrderPart{
			o_id_map: make(map[Key]*OrderEntry),
		}
		oTable.data[i] = sPart
		oTable.secIndex[i] = oPart
	}

	return oTable

}

func (o *OrderTable) CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error) {
	index := int(k[0])*DIST_COUNT + int(k[8])

	// Insert ShardedPart
	sPart := o.data[index]
	rec := MakeRecord(o, k, tuple)
	sPart.datastore[k] = rec

	// Insert OrderPart
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
	index := int(k[0])*DIST_COUNT + int(k[8])
	sPart := o.data[index]
	sPart.rwLock.RLock()
	defer sPart.rwLock.Unlock()

	rec, ok := sPart.datastore[k]
	if !ok {
		return nil, ENOKEY
	} else {
		return rec, nil
	}
}

func (o *OrderTable) SetValueByID(k Key, partNum int, value Value, colNum int) error {
	index := int(k[0])*DIST_COUNT + int(k[8])
	sPart := o.data[index]
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

func (o *OrderTable) GetValueByID(k Key, partNum int, val Value, colNum int) error {
	index := int(k[0])*DIST_COUNT + int(k[8])
	sPart := o.data[index]
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

func (o *OrderTable) PrepareDelete(k Key, partNum int) (Record, error) {
	clog.Error("Order Table Not Support PrepareDelete")
	return nil, nil
}

func (o *OrderTable) ReleaseDelete(k Key, partNum int) {
	clog.Error("Order Table Not Support ReleaseDelete")
}

func (o *OrderTable) DeleteRecord(k Key, partNum int) error {
	clog.Error("Order Table Not Support DeleteRecord")
	return nil
}

func (o *OrderTable) ReleaseInsert(k Key, partNum int) {
	clog.Error("Order Table Not Support ReleaseInsert")
}

func (o *OrderTable) PrepareInsert(k Key, partNum int) error {
	clog.Error("Order Table Not Support PrepareInsert")
	return nil
}

func (o *OrderTable) InsertRecord(k Key, partNum int, rec Record) error {
	index := int(k[0])*DIST_COUNT + int(k[8])

	// Insert ShardedPart
	sPart := o.data[index]
	oPart := o.secIndex[index]
	sPart.rwLock.Lock()
	defer sPart.rwLock.Unlock()

	_, ok := sPart.datastore[k]
	if ok {
		return EDUPKEY
	}

	sPart.datastore[k] = rec

	// Insert OrderPart
	var keyAr [KEYLENTH]int64
	var cKey Key
	oTuple := tuple.(*OrderTuple)
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

func (o *OrderTable) GetValueBySec(k Key, partNum int, val Value) error {
	index := int(k[0])*DIST_COUNT + int(k[8])
	sPart := o.data[index]
	sPart.rwLock.RLock()
	defer sPart.rwLock.Unlock()
	oPart := o.secIndex[index]

	iv := val.(*IntValue)
	oEntry, ok := oPart.o_id_map[k]
	if !ok {
		return NENOKEY
	}
	iv.intVal = oEntry.o_id_array[0]
	return nil
}

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
	c_id_array [CAP_CUSTOMER_ENTRY]int64
	next       *CustomerEntry
	t          int
	total      int
	padding2   [PADDING]byte
}

type CustomerTable struct {
	padding1    [PADDING]byte
	data        []*Partition
	secIndex    []*CustomerPart
	nKeys       int64
	isPhysical  bool
	isPartition bool
	mode        int
	padding2    [PADDING]byte
}

func MakeCustomerTable(isPartition bool, isPhysical bool, mode int, warehouse int) *CustomerTable {
	cTable := &CustomerTable{
		nKeys:       0,
		isPartition: isPartition,
		isPhysical:  isPhysical,
		mode:        mode,
	}

	//if isPartition {
	cTable.data = make([]*Partition, warehouse)
	cTable.secIndex = make([]*CustomerPart, warehouse)
	//}

	for i := 0; i < warehouse; i++ {
		part := &Partition{
			rows: make(map[Key]Record),
		}
		cPart := &CustomerPart{
			c_id_map: make(map[Key]*CustomerEntry),
		}
		cTable.data[i] = part
		cTable.secIndex[i] = cPart
	}

	return cTable

}

func (c *CustomerTable) CreateRecByID(k Key, partNum int, tuple Tuple) (Record, error) {
	// Insert Partition
	part := c.data[partNum]
	rec := MakeRecord(c, k, tuple)
	part.rows[k] = rec

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
	cPart := c.secIndex[partNum]
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
	part := c.data[partNum]
	rec, ok := part.rows[k]
	if !ok {
		return nil, ENOKEY
	} else {
		return rec, nil
	}
}

func (c *CustomerTable) SetValueByID(k Key, partNum int, value Value, colNum int) error {
	part := c.data[partNum]
	rec, ok := part.rows[k]
	if !ok {
		return ENOKEY
	} else {
		rec.SetValue(value, colNum)
		return nil
	}
}

func (c *CustomerTable) GetValueByID(k Key, partNum int, val Value, colNum int) error {
	part := c.data[partNum]
	rec, ok := part.rows[k]
	if !ok {
		return ENOKEY
	} else {
		rec.GetValue(val, colNum)
		return nil
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

func (c *CustomerTable) InsertRecord(k Key, partNum int, rec Record) error {
	clog.Error("Customer Table Not Support InsertRecord")
	return nil
}

func (c *CustomerTable) GetValueBySec(k Key, partNum int, val Value) error {
	cPart := c.secIndex[index]
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
