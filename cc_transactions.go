package testbed

import (
	"strconv"
	"time"

	"github.com/totemtang/cc-testbed/clog"

	//"runtime/debug"
)

const (
	PENALTY = 1000 // Microseconds
)

const (
	// Smallbank Workload
	SMALLBANKBASE = iota
	AMALGAMATE
	SENDPAYMENT
	BALANCE
	WRITECHECK
	DEPOSITCHECKING
	TRANSACTIONSAVINGS

	SINGLEBASE
	ADDONE
	UPDATEINT

	TPCC_BASE
	TPCC_NEWORDER
	TPCC_PAYMENT_ID
	TPCC_PAYMENT_LAST
	TPCC_ORDERSTATUS_ID
	TPCC_ORDERSTATUS_LAST
	TPCC_STOCKLEVEL
	TPCC_DELIVERY

	LAST_TXN
)

type Trans interface {
	GetTXN() int
	GetAccessParts() []int
	SetTID(tid TID)
	SetTrial(trials int)
	GetTrial() int
	DecTrial()
	SetPenalty(penalty time.Time)
	GetPenalty() time.Time
	isHome() bool
	getHome() int
	SetStartTime(start time.Time)
	GetStartTime() time.Time
	AddStartTime(genTime time.Duration)
}

type DummyTrans struct {
	t time.Time
}

func (d *DummyTrans) GetTXN() int {
	return -1
}

func (d *DummyTrans) GetAccessParts() []int {
	return nil
}

func (d *DummyTrans) SetTID(tid TID) {

}

func (d *DummyTrans) SetTrial(trials int) {

}

func (d *DummyTrans) GetTrial() int {
	return 0
}

func (d *DummyTrans) DecTrial() {

}

func (d *DummyTrans) GetPenalty() time.Time {
	return d.t
}

func (d *DummyTrans) SetPenalty(penalty time.Time) {

}

func (d *DummyTrans) isHome() bool {
	return false
}

func (d *DummyTrans) getHome() int {
	return -1
}

func (d *DummyTrans) SetStartTime(start time.Time) {
}

func (d *DummyTrans) GetStartTime() time.Time {
	return d.t
}

func (d *DummyTrans) AddStartTime(genTime time.Duration) {
	return
}

type TransGen interface {
	GenOneTrans(mode int) Trans
	ReleaseOneTrans(t Trans)
}

type TransQueue struct {
	queue []Trans
	head  int
	tail  int
	count int
	size  int
}

func NewTransQueue(size int) *TransQueue {
	tq := &TransQueue{
		queue: make([]Trans, size),
		size:  size,
		head:  -1,
		tail:  0,
		count: 0,
	}
	return tq
}

func (tq *TransQueue) Executable() Trans {
	if tq.IsEmpty() {
		return nil
	}

	if tq.IsFull() {
		return tq.Dequeue()
	}

	if time.Now().After(tq.queue[tq.tail].GetPenalty()) {
		return tq.Dequeue()
	} else {
		return nil
	}
}

func (tq *TransQueue) IsFull() bool {
	if tq.size == tq.count {
		return true
	}
	return false
}

func (tq *TransQueue) IsEmpty() bool {
	if tq.count == 0 {
		return true
	}
	return false
}

func (tq *TransQueue) Enqueue(t Trans) {
	if tq.count == tq.size {
		clog.Error("Queue Full\n")
	}
	next := (tq.head + 1) % tq.size
	tq.queue[next] = t
	tq.head = next
	tq.count++
}

func (tq *TransQueue) Dequeue() Trans {
	if tq.count == 0 {
		clog.Error("Queue Empty\n")
	}
	next := (tq.tail + 1) % tq.size
	t := tq.queue[tq.tail]
	tq.tail = next
	tq.count--
	return t
}

func (tq *TransQueue) AddGen(genTime time.Duration) {
	for i := tq.tail; i < tq.tail+tq.count; i++ {
		tq.queue[i%tq.size].AddStartTime(genTime)
	}
}

func NewOrder(t Trans, exec ETransaction) (Value, error) {
	noTrans := t.(*NewOrderTrans)

	store := exec.Store()
	table := store.priTables[ITEM].(*BasicTable)

	distRead := false
	var k Key

	floatRB := &noTrans.floatRB
	intRB := &noTrans.intRB
	req := &noTrans.req

	var rec Record
	var val Value
	var err error
	var allLocal int

	if len(noTrans.accessParts) > 1 {
		allLocal = 0
	} else {
		allLocal = 1
	}

	isHome := t.isHome()

	partNum := noTrans.w_id

	// Get W_TAX
	w_id := noTrans.w_id
	k[0] = w_id
	k[3] = noTrans.w_hot_bit
	rec, err = exec.GetRecord(WAREHOUSE, k, partNum, req, isHome)
	if err != nil {
		return nil, err
	}
	rec.GetValue(floatRB, W_TAX)
	w_tax := floatRB.floatVal

	// Get D_TAX and D_NEXT_O_ID
	d_id := noTrans.d_id
	k[1] = d_id
	k[3] = noTrans.d_hot_bit
	rec, err = exec.GetRecord(DISTRICT, k, partNum, req, isHome)
	if err != nil {
		return nil, err
	}

	distTuple := rec.GetTuple().(*DistrictTuple)
	d_tax := distTuple.d_tax
	d_next_o_id := distTuple.d_next_o_id

	// Increment D_NEXT_O_ID
	wb_next_o_id := &noTrans.wb_next_o_id
	wb_next_o_id.intVal = d_next_o_id + 1
	err = exec.WriteValue(DISTRICT, k, partNum, wb_next_o_id, D_NEXT_O_ID, req, false, isHome, rec)
	if err != nil {
		return nil, err
	}

	// Get One Record from Customer
	c_id := noTrans.c_id
	k[2] = c_id
	k[3] = noTrans.c_hot_bit
	rec, err = exec.GetRecord(CUSTOMER, k, partNum, req, isHome)
	if err != nil {
		return nil, err
	}
	k[3] = 0

	rec.GetValue(floatRB, C_DISCOUNT)
	c_discount := floatRB.floatVal

	//rb_c_last := &noTrans.rb_c_last
	//rec.GetValue(rb_c_last, C_LAST)
	rec.GetValue(intRB, C_LAST)

	rb_c_credit := &noTrans.rb_c_credit
	rec.GetValue(rb_c_credit, C_CREDIT)

	// Insert into NewOrder Table
	noTuple := noTrans.noRec.GetTuple().(*NewOrderTuple)
	noTuple.no_w_id = w_id
	noTuple.no_d_id = d_id
	noTuple.no_o_id = d_next_o_id
	k[2] = 0
	err = exec.InsertRecord(NEWORDER, k, partNum, noTrans.noRec)
	if err != nil {
		return nil, err
	}

	// Insert into Order Table
	oTuple := noTrans.oRec.GetTuple().(*OrderTuple)
	oTuple.o_id = d_next_o_id
	oTuple.o_d_id = d_id
	oTuple.o_w_id = w_id
	oTuple.o_c_id = c_id
	oTuple.o_entry_d = noTrans.o_entry_d
	oTuple.o_carrier_id = -1
	oTuple.o_ol_cnt = noTrans.ol_cnt
	oTuple.o_all_local = allLocal
	k[2] = d_next_o_id
	err = exec.InsertRecord(ORDER, k, partNum, noTrans.oRec)
	if err != nil {
		return nil, err
	}

	// Insert Order-Line and Update Stock
	var sKey Key
	k[1] = 0
	k[2] = 0
	var totalAmount float32
	rb_o_dist := &noTrans.rb_o_dist
	var iTuple *ItemTuple

	for i := 0; i < int(noTrans.ol_cnt); i++ {
		k[0] = noTrans.ol_i_id[i]
		rec, _ = table.data[0].ht.Get(k)
		iTuple = rec.GetTuple().(*ItemTuple)

		sKey[0] = noTrans.ol_supply_w_id[i]
		sKey[1] = noTrans.ol_i_id[i]
		sKey[2] = 0
		sKey[3] = noTrans.ol_hot_bit[i]

		if !distRead {
			_, _, _, err = exec.ReadValue(STOCK, sKey, sKey[0], rb_o_dist, S_DIST_01+d_id, req, isHome)
			if err != nil {
				return nil, err
			}
			distRead = true
		}

		// Update s_quantity
		rec, val, _, err = exec.ReadValue(STOCK, sKey, sKey[0], intRB, S_QUANTITY, req, isHome)
		if err != nil {
			return nil, err
		}
		s_quantity := val.(*IntValue).intVal
		if s_quantity > noTrans.ol_quantity[i]+10 {
			s_quantity -= noTrans.ol_quantity[i]
		} else {
			s_quantity = s_quantity - noTrans.ol_quantity[i] + 91
		}
		noTrans.wb_s_quantity[i].intVal = s_quantity
		err = exec.WriteValue(STOCK, sKey, sKey[0], &noTrans.wb_s_quantity[i], S_QUANTITY, req, false, isHome, rec)
		if err != nil {
			return nil, err
		}

		// Update S_YTD
		//val, _, err = exec.ReadValue(STOCK, sKey, int(sKeyAr[0]), intRB, S_YTD, req)
		//if err != nil {
		//	return nil, err
		//}
		noTrans.wb_s_ytd[i].intVal = 1
		err = exec.WriteValue(STOCK, sKey, sKey[0], &noTrans.wb_s_ytd[i], S_YTD, req, true, isHome, rec)
		if err != nil {
			return nil, err
		}

		// Update S_ORDER_CNT
		//val, _, err = exec.ReadValue(STOCK, sKey, int(sKeyAr[0]), intRB, S_ORDER_CNT, req)
		//if err != nil {
		//	return nil, err
		//}
		noTrans.wb_s_order_cnt[i].intVal = 1
		err = exec.WriteValue(STOCK, sKey, sKey[0], &noTrans.wb_s_order_cnt[i], S_ORDER_CNT, req, true, isHome, rec)
		if err != nil {
			return nil, err
		}

		// Update S_REMOTE_CNT
		if sKey[0] != w_id { // remote
			//val, _, err = exec.ReadValue(STOCK, sKey, int(sKeyAr[0]), intRB, S_REMOTE_CNT, req)
			//if err != nil {
			//	return nil, err
			//}
			noTrans.wb_s_remote_cnt[i].intVal = 1
			err = exec.WriteValue(STOCK, sKey, sKey[0], &noTrans.wb_s_remote_cnt[i], S_REMOTE_CNT, req, true, isHome, rec)
			if err != nil {
				return nil, err
			}
		}

		olTuple := noTrans.olRec[i].GetTuple().(*OrderLineTuple)
		olTuple.ol_o_id = d_next_o_id
		olTuple.ol_d_id = d_id
		olTuple.ol_w_id = w_id
		olTuple.ol_number = i
		olTuple.ol_i_id = noTrans.ol_i_id[i]
		olTuple.ol_supply_w_id = noTrans.ol_supply_w_id[i]
		olTuple.ol_quantity = noTrans.ol_quantity[i]
		olTuple.ol_amount = float32(noTrans.ol_quantity[i]) * iTuple.i_price

		ol_dist_info := olTuple.ol_dist_info[:CAP_DIST]
		copy(ol_dist_info, rb_o_dist.stringVal)

		sKey[0] = w_id
		sKey[1] = d_id
		sKey[2] = d_next_o_id
		sKey[3] = i
		exec.InsertRecord(ORDERLINE, sKey, sKey[0], noTrans.olRec[i])

		totalAmount += olTuple.ol_amount
	}

	noTrans.retVal.floatVal = totalAmount * (1 - c_discount) * (1 + w_tax + d_tax)

	if exec.Commit(req, isHome) == 0 {
		return nil, EABORT
	}

	return &noTrans.retVal, nil
}

func Payment(t Trans, exec ETransaction) (Value, error) {
	payTrans := t.(*PaymentTrans)

	var k Key

	//floatRB := &payTrans.floatRB
	intRB := &payTrans.intRB
	req := &payTrans.req

	var rec Record
	//var val Value
	var err error

	partNum := payTrans.w_id
	remotePart := payTrans.c_w_id

	isHome := t.isHome()

	// Increment w_ytd in warehouse
	k[0] = payTrans.w_id
	k[3] = payTrans.w_hot_bit
	rec, err = exec.GetRecord(WAREHOUSE, k, partNum, req, isHome)
	if err != nil {
		return nil, err
	}
	wTuple := rec.GetTuple().(*WarehouseTuple)
	payTrans.wb_w_ytd.floatVal = wTuple.w_ytd + payTrans.h_amount
	err = exec.WriteValue(WAREHOUSE, k, partNum, &payTrans.wb_w_ytd, W_YTD, req, false, isHome, rec)
	if err != nil {
		return nil, err
	}

	// Increment D_YTD in district
	k[1] = payTrans.d_id
	k[3] = payTrans.d_hot_bit
	rec, err = exec.GetRecord(DISTRICT, k, partNum, req, isHome)
	if err != nil {
		return nil, err
	}
	dTuple := rec.GetTuple().(*DistrictTuple)
	payTrans.wb_d_ytd.floatVal = dTuple.d_ytd + payTrans.h_amount
	err = exec.WriteValue(DISTRICT, k, partNum, &payTrans.wb_d_ytd, D_YTD, req, false, isHome, rec)
	if err != nil {
		return nil, err
	}
	k[3] = 0

	k[0] = payTrans.c_w_id
	k[1] = payTrans.d_id
	if payTrans.isLast {
		k[2] = payTrans.c_last
		//for i, b := range payTrans.c_last {
		//	k[i+16] = b
		//}
		err = exec.GetKeysBySecIndex(CUSTOMER, k, remotePart, intRB)
		if err != nil {
			return nil, err
		}
		k[2] = intRB.intVal
		if *Hybrid && k[2] < HOTREC_SMALL {
			k[3] = HOTBIT
		} else {
			k[3] = 0
		}
	} else {
		k[2] = payTrans.c_id
		k[3] = payTrans.c_hot_bit
	}
	rec, err = exec.GetRecord(CUSTOMER, k, remotePart, req, isHome)
	if err != nil {
		return nil, err
	}
	cTuple := rec.GetTuple().(*CustomerTuple)
	// Decrease C_BALANCE
	payTrans.wb_c_balance.floatVal = cTuple.c_balance - payTrans.h_amount
	err = exec.WriteValue(CUSTOMER, k, remotePart, &payTrans.wb_c_balance, C_BALANCE, req, false, isHome, rec)
	if err != nil {
		return nil, err
	}
	// Increase C_YTD_PAYMENT
	payTrans.wb_c_ytd_payment.floatVal = cTuple.c_ytd_payment + payTrans.h_amount
	err = exec.WriteValue(CUSTOMER, k, remotePart, &payTrans.wb_c_ytd_payment, C_YTD_PAYMENT, req, false, isHome, rec)
	if err != nil {
		return nil, err
	}

	// Increase C_PAYMENT_CNT
	payTrans.wb_c_payment_cnt.intVal = cTuple.c_payment_cnt + 1
	err = exec.WriteValue(CUSTOMER, k, remotePart, &payTrans.wb_c_payment_cnt, C_PAYMENT_CNT, req, false, isHome, rec)
	if err != nil {
		return nil, err
	}
	k[3] = 0

	// 10 %
	if payTrans.rnd.Intn(100) < 10 {
		cTuple.GetValue(&payTrans.wb_c_data, C_DATA)
		payTrans.wb_c_data.stringVal = payTrans.wb_c_data.stringVal[:CAP_C_DATA]
		c := 0
		for _, b := range []byte(strconv.FormatInt(int64(payTrans.c_id), 10)) {
			payTrans.wb_c_data.stringVal[c] = b
			c++
		}
		for _, b := range []byte(strconv.FormatInt(int64(payTrans.d_id), 10)) {
			payTrans.wb_c_data.stringVal[c] = b
			c++
		}
		for _, b := range []byte(strconv.FormatInt(int64(payTrans.c_id), 10)) {
			payTrans.wb_c_data.stringVal[c] = b
			c++
		}
		for _, b := range []byte(strconv.FormatInt(int64(payTrans.w_id), 10)) {
			payTrans.wb_c_data.stringVal[c] = b
			c++
		}
		for _, b := range []byte(strconv.FormatFloat(float64(payTrans.h_amount), 'E', -1, 64)) {
			payTrans.wb_c_data.stringVal[c] = b
			c++
		}
	}

	k[0] = payTrans.w_id
	k[1] = payTrans.d_id
	k[2] = payTrans.c_id

	hTuple := payTrans.hRec.GetTuple().(*HistoryTuple)
	hTuple.h_c_id = payTrans.c_id
	hTuple.h_c_d_id = payTrans.d_id
	hTuple.h_c_w_id = payTrans.c_w_id
	hTuple.h_d_id = payTrans.d_id
	hTuple.h_w_id = payTrans.w_id
	c := 0
	for _, b := range wTuple.w_name {
		hTuple.h_data[c] = b
		c++
	}
	for _, b := range dTuple.d_name {
		hTuple.h_data[c] = b
		c++
	}

	err = exec.InsertRecord(HISTORY, k, partNum, payTrans.hRec)
	if err != nil {
		return nil, err
	}

	if exec.Commit(req, isHome) == 0 {
		return nil, EABORT
	}

	return nil, nil
}

func OrderStatus(t Trans, exec ETransaction) (Value, error) {
	orderTrans := t.(*OrderStatusTrans)

	var k Key

	var rec Record
	var err error

	intRB := &orderTrans.intRB
	req := &orderTrans.req

	partNum := orderTrans.w_id

	isHome := t.isHome()

	// Select one row from Customer
	k[0] = orderTrans.w_id
	k[1] = orderTrans.d_id
	if orderTrans.isLast {
		k[2] = orderTrans.c_last
		//for i, b := range orderTrans.c_last {
		//	k[i+16] = b
		//}
		err = exec.GetKeysBySecIndex(CUSTOMER, k, partNum, intRB)
		if err != nil {
			return nil, err
		}
		k[2] = intRB.intVal
		if *Hybrid && k[2] < HOTREC_SMALL {
			k[3] = HOTBIT
		}
	} else {
		k[2] = orderTrans.c_id
		k[3] = orderTrans.c_hot_bit
	}
	_, err = exec.GetRecord(CUSTOMER, k, partNum, req, isHome)
	if err != nil {
		return nil, err
	}
	k[3] = 0

	// Select one row from orders
	err = exec.GetKeysBySecIndex(ORDER, k, partNum, intRB)
	if err != nil {
		if err == ENOORDER {
			exec.Abort(req)
			return nil, nil
		}
		return nil, err
	}
	o_id := intRB.intVal

	k[2] = o_id
	rec, err = exec.GetRecord(ORDER, k, partNum, req, isHome)
	if err != nil {
		return nil, err
	}

	ol_cnt := rec.GetTuple().(*OrderTuple).o_ol_cnt
	for i := 0; i < ol_cnt; i++ {
		k[3] = i
		_, err = exec.GetRecord(ORDERLINE, k, partNum, req, isHome)
		if err != nil {
			return nil, err
		}
	}

	if exec.Commit(req, isHome) == 0 {
		return nil, EABORT
	}

	return nil, nil
}

func Delivery(t Trans, exec ETransaction) (Value, error) {
	deliveryTrans := t.(*DeliveryTrans)

	var k Key

	var rec Record
	var err error
	var val Value

	intRB := &deliveryTrans.intRB
	req := &deliveryTrans.req

	partNum := deliveryTrans.w_id

	isHome := t.isHome()

	k[0] = deliveryTrans.w_id
	k[1] = deliveryTrans.d_id
	rec, err = exec.DeleteRecord(NEWORDER, k, partNum)
	if err != nil {
		if err == ENODEL {
			return nil, nil
		} else {
			return nil, err
		}
	}

	o_id := rec.GetTuple().(*NewOrderTuple).no_o_id

	// Update ORDER Table
	k[2] = o_id
	rec, err = exec.GetRecord(ORDER, k, partNum, req, isHome)
	if err != nil {
		return nil, err
	}
	rec.GetValue(intRB, O_C_ID)
	c_id := intRB.intVal

	rec.GetValue(intRB, O_OL_CNT)
	ol_cnt := intRB.intVal

	err = exec.WriteValue(ORDER, k, partNum, &deliveryTrans.wb_o_carrier, O_CARRIER_ID, req, false, isHome, rec)
	if err != nil {
		return nil, err
	}

	floatRB := &deliveryTrans.floatRB
	ol_amount := float32(0)
	for i := 0; i < ol_cnt; i++ {
		k[3] = i
		rec, val, _, err = exec.ReadValue(ORDERLINE, k, partNum, floatRB, OL_AMOUNT, req, isHome)
		if err != nil {
			return nil, err
		}
		ol_amount += val.(*FloatValue).floatVal
		err = exec.WriteValue(ORDERLINE, k, partNum, &deliveryTrans.wb_date_ar[i], OL_DELIVERY_D, req, false, isHome, rec)
		if err != nil {
			return nil, err
		}

	}

	k[2] = c_id
	if *Hybrid && k[2] < HOTREC_SMALL {
		k[3] = HOTBIT
	} else {
		k[3] = 0
	}
	wb_ol_amount := &deliveryTrans.wb_ol_amount
	wb_ol_amount.floatVal = ol_amount
	err = exec.WriteValue(CUSTOMER, k, partNum, wb_ol_amount, C_BALANCE, req, true, isHome, nil)
	if err != nil {
		return nil, err
	}

	wb_delivery_cnt := &deliveryTrans.wb_delivery_cnt
	wb_delivery_cnt.intVal = 1
	err = exec.WriteValue(CUSTOMER, k, partNum, wb_delivery_cnt, C_DELIVERY_CNT, req, true, isHome, nil)
	if err != nil {
		return nil, err
	}

	if exec.Commit(req, isHome) == 0 {
		return nil, EABORT
	}

	return nil, nil
}

func StockLevel(t Trans, exec ETransaction) (Value, error) {
	stockTrans := t.(*StockLevelTrans)

	var k Key
	var s_k Key

	var rec Record
	var err error

	partNum := stockTrans.w_id
	req := &stockTrans.req

	isHome := t.isHome()

	// Select one row from District Table
	k[0] = stockTrans.w_id
	k[1] = stockTrans.d_id
	k[3] = stockTrans.d_hot_bit
	rec, err = exec.GetRecord(DISTRICT, k, partNum, req, isHome)
	if err != nil {
		return nil, err
	}
	next_o_id := rec.GetTuple().(*DistrictTuple).d_next_o_id
	k[3] = 0

	threshold := stockTrans.threshold
	i_id_ar := stockTrans.i_id_ar
	ol_cnt := 0
	count := 0

	s_k[0] = stockTrans.w_id
	for i := next_o_id - 1; i >= next_o_id-20; i-- {
		// Read one row from ORDER Table
		k[2] = i
		k[3] = 0
		rec, err = exec.GetRecord(ORDER, k, partNum, req, isHome)
		if err != nil {
			return nil, err
		}

		ol_cnt = rec.GetTuple().(*OrderTuple).o_ol_cnt
		for j := 0; j < ol_cnt; j++ {
			// Read ol_cnt rows from OrderLine Table
			k[3] = j
			rec, err = exec.GetRecord(ORDERLINE, k, partNum, req, isHome)
			if err != nil {
				return nil, err
			}
			i_id := rec.GetTuple().(*OrderLineTuple).ol_i_id
			exist := false
			for _, tmpId := range i_id_ar {
				if tmpId == i_id {
					exist = true
					break
				}
			}
			if exist {
				continue
			}

			// Record it
			n := len(i_id_ar)
			i_id_ar = i_id_ar[:n+1]
			i_id_ar[n] = i_id

			// Check threshold; Read s_quantity from Stock Table
			s_k[1] = i_id
			if *Hybrid && i_id < HOTREC {
				s_k[3] = HOTBIT
			}
			rec, err = exec.GetRecord(STOCK, s_k, partNum, req, isHome)
			if err != nil {
				return nil, err
			}
			s_k[3] = 0
			if rec.GetTuple().(*StockTuple).s_quantity < threshold {
				count++
			}
		}
	}

	if exec.Commit(req, isHome) == 0 {
		return nil, EABORT
	}

	return nil, nil
}

/*
1. Get Records from ACCOUNTS to check the existence
2. Get SAVINGS Balance of AcctID0
3. Get CHECKING Balance of AcctID1
4. Calculate Sum of Balance from AcctID0 and AcctID1
5. Update CHECKING Balance of AcctID0 with Zero
6. Update SAVINGS Balance of AcctID1 with its Balance minus Sum
*/
func Amalgamate(t Trans, exec ETransaction) (Value, error) {
	sbTrnas := t.(*SBTrans)

	//intRB := &sbTrnas.intRB
	floatRB := &sbTrnas.floatRB
	req := &sbTrnas.req

	isHome := t.isHome()

	var part0, part1 int
	if len(sbTrnas.accessParts) == 1 {
		part0 = sbTrnas.accessParts[0]
		part1 = sbTrnas.accessParts[0]
	} else {
		part0 = sbTrnas.accessParts[0]
		part1 = sbTrnas.accessParts[1]
	}

	for i := 0; i < SBMAXSUBTRANS; i++ {
		fv0 := &sbTrnas.fv[i*SBMAXPARTS+0]
		fv1 := &sbTrnas.fv[i*SBMAXPARTS+1]

		acctId0 := sbTrnas.accoutID[i*SBMAXPARTS+0]
		acctId1 := sbTrnas.accoutID[i*SBMAXPARTS+1]

		var val Value
		var err error
		/*_, _, _, err = exec.ReadValue(ACCOUNTS, acctId0, part0, intRB, ACCT_ID, req, isHome)
		if err != nil {
			return nil, err
		}

		_, _, _, err = exec.ReadValue(ACCOUNTS, acctId1, part1, intRB, ACCT_ID, req, isHome)
		if err != nil {
			return nil, err
		}*/

		err = exec.MayWrite(SAVINGS, acctId0, part0, req)
		if err != nil {
			return nil, err
		}

		err = exec.MayWrite(CHECKING, acctId1, part1, req)
		if err != nil {
			return nil, err
		}

		_, val, _, err = exec.ReadValue(SAVINGS, acctId0, part0, floatRB, SAVING_BAL, req, isHome)
		if err != nil {
			return nil, err
		}
		//sum := val.(*FloatValue).floatVal
		fv0.floatVal = val.(*FloatValue).floatVal

		_, val, _, err = exec.ReadValue(CHECKING, acctId1, part1, floatRB, CHECK_BAL, req, isHome)
		if err != nil {
			return nil, err
		}
		//sum += val.(*FloatValue).floatVal
		fv1.floatVal = val.(*FloatValue).floatVal

		/*val, err = exec.ReadValue(SAVINGS, acctId1, part1, 1)
		if err != nil {
			return nil, err
		}
		sum = val.(*FloatValue).floatVal - sum*/

		//fv0.floatVal = float64(0)
		//fv1.floatVal = sum

		/*
			err = exec.WriteValue(CHECKING, acctId0, part0, fv0, 1)
			if err != nil {
				return nil, err
			}

			err = exec.WriteValue(SAVINGS, acctId1, part1, fv1, 1)
			if err != nil {
				return nil, err
			}
		*/

		err = exec.WriteValue(CHECKING, acctId1, part1, fv0, CHECK_BAL, req, false, isHome, nil)
		if err != nil {
			return nil, err
		}

		err = exec.WriteValue(SAVINGS, acctId0, part0, fv1, SAVING_BAL, req, false, isHome, nil)
		if err != nil {
			return nil, err
		}
	}

	if exec.Commit(req, isHome) == 0 {
		return nil, EABORT
	}

	return nil, nil
}

/*
1. Get Records from ACCOUNTS to check the existence
2. Get CHECKING Balance from SendAcct
3. If CHECKING Balance is smaller than Amount, Abort
4. Deduct Amount from CHECKING Balance of SendAcct
5. Add Amount to CHECKING Balance of DestAcct
*/
func SendPayment(t Trans, exec ETransaction) (Value, error) {
	sbTrnas := t.(*SBTrans)

	//intRB := &sbTrnas.intRB
	floatRB := &sbTrnas.floatRB
	req := &sbTrnas.req

	isHome := t.isHome()

	var part0, part1 int
	if len(sbTrnas.accessParts) == 1 {
		part0 = sbTrnas.accessParts[0]
		part1 = sbTrnas.accessParts[0]
	} else {
		part0 = sbTrnas.accessParts[0]
		part1 = sbTrnas.accessParts[1]
	}

	ammt := &sbTrnas.ammount

	for i := 0; i < SBMAXSUBTRANS; i++ {
		fv0 := &sbTrnas.fv[i*SBMAXPARTS+0]
		fv1 := &sbTrnas.fv[i*SBMAXPARTS+1]

		send := sbTrnas.accoutID[i*SBMAXPARTS+0]
		dest := sbTrnas.accoutID[i*SBMAXPARTS+1]

		var val Value
		var err error
		/*_, _, _, err = exec.ReadValue(ACCOUNTS, send, part0, intRB, ACCT_ID, req, isHome)
		if err != nil {
			return nil, err
		}

		_, _, _, err = exec.ReadValue(ACCOUNTS, dest, part1, intRB, ACCT_ID, req, isHome)
		if err != nil {
			return nil, err
		}*/

		err = exec.MayWrite(CHECKING, send, part0, req)
		if err != nil {
			return nil, err
		}

		err = exec.MayWrite(CHECKING, dest, part1, req)
		if err != nil {
			return nil, err
		}

		_, val, _, err = exec.ReadValue(CHECKING, send, part0, floatRB, CHECK_BAL, req, isHome)
		if err != nil {
			return nil, err
		}
		bal := val.(*FloatValue).floatVal

		if bal < ammt.floatVal {
			exec.Abort(req)
			return nil, ELACKBALANCE
		}

		fv0.floatVal = bal - ammt.floatVal

		_, val, _, err = exec.ReadValue(CHECKING, dest, part1, floatRB, CHECK_BAL, req, isHome)
		if err != nil {
			return nil, err
		}
		fv1.floatVal = val.(*FloatValue).floatVal + ammt.floatVal

		err = exec.WriteValue(CHECKING, send, part0, fv0, CHECK_BAL, req, false, isHome, nil)
		if err != nil {
			return nil, err
		}

		err = exec.WriteValue(CHECKING, dest, part1, fv1, CHECK_BAL, req, false, isHome, nil)
		if err != nil {
			return nil, err
		}
	}

	if exec.Commit(req, isHome) == 0 {
		return nil, EABORT
	}

	return nil, nil
}

/*
1. Get Records from ACCOUNTS to check the existence
2. Get CHECKING and SAVINGS Balance of AcctID
3. Return their Sum
*/
func Balance(t Trans, exec ETransaction) (Value, error) {
	sbTrnas := t.(*SBTrans)

	//intRB := &sbTrnas.intRB
	floatRB := &sbTrnas.floatRB
	req := &sbTrnas.req

	ret := &sbTrnas.ret
	part := sbTrnas.accessParts[0]

	isHome := t.isHome()

	for i := 0; i < SBMAXSUBTRANS; i++ {
		acct := sbTrnas.accoutID[i]

		var val Value
		var err error
		/*_, _, _, err = exec.ReadValue(ACCOUNTS, acct, part, intRB, ACCT_ID, req, isHome)
		if err != nil {
			return nil, err
		}*/

		_, val, _, err = exec.ReadValue(CHECKING, acct, part, floatRB, CHECK_BAL, req, isHome)
		if err != nil {
			return nil, err
		}
		ret.floatVal = val.(*FloatValue).floatVal

		_, val, _, err = exec.ReadValue(SAVINGS, acct, part, floatRB, SAVING_BAL, req, isHome)
		if err != nil {
			return nil, err
		}

		ret.floatVal += val.(*FloatValue).floatVal
	}

	if exec.Commit(req, isHome) == 0 {
		return nil, EABORT
	}

	return ret, nil
}

/*
1. Get Records from ACCOUNTS to check the existence
2. Get CHECKING and SAVINGS Balance of AcctID
3. If Sum of them is smaller than Amount, Update CHECKING Balance with its Balance minus Amount plus 1
4. Else Update CHECKING Balance with its Balance minus Amount
*/
func WriteCheck(t Trans, exec ETransaction) (Value, error) {
	sbTrnas := t.(*SBTrans)

	//intRB := &sbTrnas.intRB
	floatRB := &sbTrnas.floatRB
	req := &sbTrnas.req

	part := sbTrnas.accessParts[0]
	ammt := &sbTrnas.ammount

	isHome := t.isHome()

	for i := 0; i < SBMAXSUBTRANS; i++ {
		acct := sbTrnas.accoutID[i]
		fv0 := &sbTrnas.fv[i]

		var val Value
		var err error
		/*_, _, _, err = exec.ReadValue(ACCOUNTS, acct, part, intRB, ACCT_ID, req, isHome)
		if err != nil {
			return nil, err
		}*/

		_, val, _, err = exec.ReadValue(CHECKING, acct, part, floatRB, CHECK_BAL, req, isHome)
		if err != nil {
			return nil, err
		}
		checkBal := val.(*FloatValue).floatVal
		sum := checkBal

		_, val, _, err = exec.ReadValue(SAVINGS, acct, part, floatRB, SAVING_BAL, req, isHome)
		if err != nil {
			return nil, err
		}
		sum += val.(*FloatValue).floatVal

		if sum < ammt.floatVal {
			fv0.floatVal = checkBal - ammt.floatVal + float32(1)
			err = exec.WriteValue(CHECKING, acct, part, fv0, 1, req, false, isHome, nil)
			if err != nil {
				return nil, err
			}
		} else {
			fv0.floatVal = checkBal - ammt.floatVal
			err = exec.WriteValue(CHECKING, acct, part, fv0, 1, req, false, isHome, nil)
			if err != nil {
				return nil, err
			}
		}
	}

	if exec.Commit(req, isHome) == 0 {
		return nil, EABORT
	}

	return nil, nil
}

/*
1. Get Records from ACCOUNTS to check the existence
2. Update CHECKING Balance of AcctID with its Balance plus Amount
*/
func DepositChecking(t Trans, exec ETransaction) (Value, error) {
	sbTrnas := t.(*SBTrans)

	//intRB := &sbTrnas.intRB
	floatRB := &sbTrnas.floatRB
	req := &sbTrnas.req

	part := sbTrnas.accessParts[0]
	ammt := &sbTrnas.ammount

	isHome := t.isHome()

	for i := 0; i < SBMAXSUBTRANS; i++ {
		fv0 := &sbTrnas.fv[i]
		acct := sbTrnas.accoutID[i]

		var val Value
		var err error
		/*_, _, _, err = exec.ReadValue(ACCOUNTS, acct, part, intRB, ACCT_ID, req, isHome)
		if err != nil {
			return nil, err
		}*/

		_, val, _, err = exec.ReadValue(CHECKING, acct, part, floatRB, CHECK_BAL, req, isHome)
		if err != nil {
			return nil, err
		}
		fv0.floatVal = val.(*FloatValue).floatVal + ammt.floatVal

		err = exec.WriteValue(CHECKING, acct, part, fv0, CHECK_BAL, req, false, isHome, nil)
		if err != nil {
			return nil, err
		}

	}

	if exec.Commit(req, isHome) == 0 {
		return nil, EABORT
	}

	return nil, nil
}

/*
1. Get Records from ACCOUNTS to check the existence
2. Get SAVINGS Balance of AcctID
3. Calculate Sum of SAVING Balance and Amount
4. If Sum is negative, Abort
5. Else Update SAVINGS Balance of AcctID with Sum
*/
func TransactionSavings(t Trans, exec ETransaction) (Value, error) {
	sbTrnas := t.(*SBTrans)

	//intRB := &sbTrnas.intRB
	floatRB := &sbTrnas.floatRB
	req := &sbTrnas.req

	part := sbTrnas.accessParts[0]
	ammt := &sbTrnas.ammount

	isHome := t.isHome()

	for i := 0; i < SBMAXSUBTRANS; i++ {
		acct := sbTrnas.accoutID[i]
		fv0 := &sbTrnas.fv[i]

		var val Value
		var err error
		/*_, _, _, err = exec.ReadValue(ACCOUNTS, acct, part, intRB, ACCT_ID, req, isHome)
		if err != nil {
			return nil, err
		}*/

		_, val, _, err = exec.ReadValue(SAVINGS, acct, part, floatRB, SAVING_BAL, req, isHome)
		if err != nil {
			return nil, err
		}
		sum := val.(*FloatValue).floatVal + ammt.floatVal

		if sum < 0 {
			exec.Abort(req)
			return nil, ENEGSAVINGS
		} else {
			fv0.floatVal = sum
			err = exec.WriteValue(SAVINGS, acct, part, fv0, SAVING_BAL, req, false, isHome, nil)
			if err != nil {
				return nil, err
			}
		}
	}

	if exec.Commit(req, isHome) == 0 {
		return nil, EABORT
	}

	return nil, nil
}

func AddOne(t Trans, exec ETransaction) (Value, error) {
	singleTrans := t.(*SingleTrans)
	iv := singleTrans.iv

	isHome := t.isHome()

	req := &singleTrans.req
	intRB := &singleTrans.intRB
	var k Key
	var part int
	var val Value
	var err error
	//var fromStore bool
	for i := 0; i < len(singleTrans.keys); i++ {
		k = singleTrans.keys[i]
		part = singleTrans.parts[i]

		err = exec.MayWrite(SINGLE, k, part, req)
		if err != nil {
			return nil, err
		}

		//val, fromStore, err = exec.ReadValue(SINGLE, k, part, intRB, SINGLE_VAL, tid)
		_, val, _, err = exec.ReadValue(SINGLE, k, part, intRB, SINGLE_VAL, req, isHome)
		if err != nil {
			return nil, err
		}

		/*if fromStore {
			iv[i].intVal = *val.(*int64) + 1
		} else {
			iv[i].intVal = val.(*IntValue).intVal + 1
		}*/
		iv[i].intVal = val.(*IntValue).intVal + 1

		err = exec.WriteValue(SINGLE, k, part, &iv[i], SINGLE_VAL, req, false, isHome, nil)
		if err != nil {
			return nil, err
		}

	}

	if exec.Commit(req, isHome) == 0 {
		return nil, EABORT
	}

	return nil, nil
}

func UpdateInt(t Trans, exec ETransaction) (Value, error) {
	singleTrans := t.(*SingleTrans)
	sv := singleTrans.sv
	strRB := &singleTrans.strRB
	req := &singleTrans.req
	var k Key
	var part int
	var err error
	var val Value
	var col int
	col = singleTrans.rnd.Intn(25) + SINGLE_VAL + 1
	var isRead bool
	isHome := t.isHome()
	for i := 0; i < len(singleTrans.keys); i++ {
		k = singleTrans.keys[i]
		part = singleTrans.parts[i]

		if singleTrans.rnd.Intn(100) >= singleTrans.rr && part >= *r_part {
			isRead = false
		} else {
			isRead = true
		}

		_, val, _, err = exec.ReadValue(SINGLE, k, part, strRB, col, req, isHome)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, ENOKEY
		}

		if !isRead {
			sv[i].stringVal = sv[i].stringVal[:CAP_SINGLE_STR]
			err = exec.WriteValue(SINGLE, k, part, &sv[i], col, req, false, isHome, nil)
			if err != nil {
				return nil, err
			}
		}
	}

	if exec.Commit(req, isHome) == 0 {
		return nil, EABORT
	}

	return nil, nil
}
