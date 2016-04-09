package testbed

import (
	"strconv"
	"time"

	"github.com/totemtang/cc-testbed/clog"
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

	TPCCBASE
	NEWORDER
	PAYMENTID
	PAYMENTLAST
	ORDERSTATUSID
	ORDERSTATUSLAST
	DELIVERY
	STOCKLEVEL

	LAST_TXN
)

type Trans interface {
	GetTXN() int
	GetAccessParts() []int
	SetTID(tid TID)
	SetTrial(trials int)
	GetTrial() int
	DecTrial()
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

func NewOrder(t Trans, exec ETransaction) (Value, error) {
	noTrans := t.(*NewOrderTrans)

	var k Key
	var keyAr [KEYLENTH]int64

	floatRB := &noTrans.floatRB
	intRB := &noTrans.intRB

	var rec Record
	var val Value
	var err error
	var allLocal int64

	if len(noTrans.accessParts) > 1 {
		allLocal = 0
	} else {
		allLocal = 1
	}

	partNum := noTrans.w_id

	// Get W_TAX
	w_id := noTrans.w_id
	keyAr[0] = w_id
	UKey(keyAr, &k)
	rec, err = exec.GetRecord(WAREHOUSE, k, partNum, 0)
	if err != nil {
		return nil, err
	}
	rec.GetValue(floatRB, W_TAX)
	w_tax := floatRB.floatVal

	// Get D_TAX and D_NEXT_O_ID
	d_id := noTrans.d_id
	keyAr[1] = d_id
	UKey(keyAr, &k)
	rec, err = exec.GetRecord(DISTRICT, k, partNum, 0)
	if err != nil {
		return nil, err
	}

	distTuple := rec.GetTuple().(*DistrictTuple)
	d_tax := distTuple.d_tax
	d_next_o_id := distTuple.d_next_o_id

	// Increment D_NEXT_O_ID
	wb_next_o_id := &noTrans.wb_next_o_id
	wb_next_o_id.intVal = d_next_o_id + 1
	err = exec.WriteValue(DISTRICT, k, partNum, wb_next_o_id, D_NEXT_O_ID, 0)
	if err != nil {
		return nil, err
	}

	// Get One Record from Customer
	c_id := noTrans.c_id
	keyAr[2] = c_id
	UKey(keyAr, &k)
	rec, err = exec.GetRecord(CUSTOMER, k, partNum, 0)
	if err != nil {
		return nil, err
	}

	rec.GetValue(floatRB, C_DISCOUNT)
	c_discount := floatRB.floatVal

	rb_c_last := &noTrans.rb_c_last
	rec.GetValue(rb_c_last, C_LAST)

	rb_c_credit := &noTrans.rb_c_credit
	rec.GetValue(rb_c_last, C_CREDIT)

	// Insert into NewOrder Table
	noTuple := noTrans.noRec.GetTuple().(*NewOrderTuple)
	noTuple.no_w_id = w_id
	noTuple.no_d_id = d_id
	noTuple.no_o_id = d_next_o_id
	keyAr[2] = 0
	UKey(keyAr, &k)
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
	keyAr[2] = d_next_o_id
	UKey(keyAr, &k)
	err = exec.InsertRecord(NEWORDER, k, partNum, noTrans.oRec)
	if err != nil {
		return nil, err
	}

	// Insert Order-Line and Update Stock
	var sKey Key
	var sKeyAr [KEYLENTH]int64
	keyAr[1] = 0
	keyAr[2] = 0
	var totalAmount float64
	for i := 0; i < noTrans.ol_cnt; i++ {
		keyAr[0] = noTrans.ol_i_id[i]
		UKey(keyAr, *k)
		rec, err = exec.GetRecord(ITEM, k, 0, 0)
		if err != nil {
			return nil, err
		}
		iTuple := rec.GetTuple().(*ItemTuple)

		sKeyAr[0] = noTrans.ol_supply_w_id[i]
		sKeyAr[1] = noTrans.ol_i_id[i]
		UKey(sKeyAr, *sKey)
		rec, err = exec.GetRecord(STOCK, sKey, sKeyAr[0], 0)
		if err != nil {
			return nil, err
		}
		sTuple := rec.GetTuple().(*StockTuple)

		// Update s_quantity
		val, err = exec.ReadValue(STOCK, sKey, sKeyAr[0], intRB, S_QUANTITY, 0)
		if err != nil {
			return nil, err
		}
		s_quantity := val.(*intRB).intVal
		if s_quantity > noTrans.ol_quantity[i]+10 {
			s_quantity -= noTrans.ol_quantity[i]
		} else {
			s_quantity = s_quantity - noTrans.ol_quantity[i] + 91
		}
		noTrans.wb_s_quantity[i].intVal = s_quantity
		err = exec.WriteValue(STOCK, sKey, sKeyAr[0], &noTrans.wb_s_quantity[i], S_QUANTITY, 0)
		if err != nil {
			return nil, err
		}

		// Update S_YTD
		val, err = exec.ReadValue(STOCK, sKey, sKeyAr[0], intRB, S_YTD, 0)
		if err != nil {
			return nil, err
		}
		noTrans.wb_s_ytd[i].intVal = val.(*IntValue).intVal + 1
		err = exec.WriteValue(STOCK, sKey, sKeyAr[0], &noTrans.wb_s_ytd[i], S_YTD, 0)
		if err != nil {
			return nil, err
		}

		// Update S_ORDER_CNT
		val, err = exec.ReadValue(STOCK, sKey, sKeyAr[0], intRB, S_ORDER_CNT, 0)
		if err != nil {
			return nil, err
		}
		noTrans.wb_s_order_cnt[i].intVal = val.(*IntValue).intVal + 1
		err = exec.WriteValue(STOCK, sKey, sKeyAr[0], &noTrans.wb_s_order_cnt[i], S_ORDER_CNT, 0)
		if err != nil {
			return nil, err
		}

		// Update S_REMOTE_CNT
		if sKeyAr[0] != w_id { // remote
			val, err = exec.ReadValue(STOCK, sKey, sKeyAr[0], intRB, S_REMOTE_CNT, 0)
			if err != nil {
				return nil, err
			}
			noTrans.wb_s_remote_cnt[i].intVal = val.(*IntValue).intVal + 1
			err = exec.WriteValue(STOCK, sKey, sKeyAr[0], &noTrans.wb_s_remote_cnt[i], S_REMOTE_CNT, 0)
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
		olTuple.ol_amount = float64(noTrans.ol_quantity[i]) * iTuple.i_price
		olTuple.ol_dist_info = sTuple.s_dist_01
		sKeyAr[0] = w_id
		sKeyAr[1] = d_id
		sKeyAr[2] = d_next_o_id
		sKeyAr[2] = i
		UKey(sKeyAr, sKey)
		exec.InsertRecord(ORDERLINE, sKey, sKeyAr[0], noTrans.olRec[i])

		totalAmount += olTuple.ol_amount
	}

	noTrans.retVal.floatVal = totalAmount * (1 - c_discount) * (1 + w_tax + d_tax)

	if exec.Commit() == 0 {
		return nil, EABORT
	}

	return &noTrans.retVal, nil
}

func PaymentID(t Trans, exec ETransaction) (Value, error) {
	payTrans := t.(*PaymentIDTrans)

	var k Key
	var keyAr [KEYLENTH]int64

	floatRB := &payTrans.floatRB
	intRB := &payTrans.intRB

	var rec Record
	var val Value
	var err error

	partNum := payTrans.w_id
	ts := payTrans.tid

	// Increment w_ytd in warehouse
	keyAr[0] = payTrans.w_id
	UKey(keyAr, &k)
	rec, err = exec.GetRecord(WAREHOUSE, k, partNum, ts)
	if err != nil {
		return nil, err
	}
	wTuple := rec.(*WarehouseTuple)
	payTrans.wb_w_ytd.floatVal = wTuple.w_ytd + payTrans.h_amount
	err = exec.WriteValue(WAREHOUSE, k, partNum, &payTrans.wb_w_ytd.floatVal, W_YTD, ts)
	if err != nil {
		return nil, err
	}

	// Increment D_YTD in district
	keyAr[1] = payTrans.d_id
	UKey(keyAr, &k)
	rec, err = exec.GetRecord(DISTRICT, k, partNum, ts)
	if err != nil {
		return nil, err
	}
	dTuple := rec.(*DistrictTuple)
	payTrans.wb_d_ytd.floatVal = dTuple.d_ytd + payTrans.h_amount
	err = exec.WriteValue(DISTRICT, k, partNum, &payTrans.wb_d_ytd.floatVal, D_YTD, ts)
	if err != nil {
		return nil, err
	}

	keyAr[0] = payTrans.c_w_id
	keyAr[1] = payTrans.d_id
	keyAr[2] = payTrans.c_id
	UKey(keyAr, &k)
	rec, err = exec.GetRecord(CUSTOMER, k, partNum, ts)
	if err != nil {
		return nil, err
	}
	cTuple := rec.GetTuple().(*CustomerTuple)
	// Decrease C_BALANCE
	payTrans.wb_c_balance.floatVal = cTuple.c_balance - payTrans.h_amount
	err = exec.WriteValue(CUSTOMER, k, partNum, &payTrans.wb_c_balance, C_BALANCE, ts)
	if err != nil {
		return nil, err
	}
	// Increase C_YTD_PAYMENT
	payTrans.wb_c_ytd_payment.floatVal = cTuple.c_ytd_payment + payTrans.h_amount
	err = exec.WriteValue(CUSTOMER, k, partNum, &payTrans.wb_c_ytd_payment, C_YTD_PAYMENT, ts)
	if err != nil {
		return nil, err
	}

	// Increase C_PAYMENT_CNT
	payTrans.wb_c_payment_cnt.intVal = cTuple.c_payment_cnt + 1
	err = exec.WriteValue(CUSTOMER, k, partNum, &payTrans.wb_c_payment_cnt, C_PAYMENT_CNT, ts)
	if err != nil {
		return nil, err
	}

	// 10 %
	if payTrans.rnd.Intn(100) < 10 {
		cTuple.GetValue(&payTrans.wb_c_data, C_DATA)
		payTrans.wb_c_data.stringVal = payTrans.wb_c_data.stringVal[:CAP_C_DATA]
		c := 0
		for _, b := range []byte(strconv.FormatInt(payTrans.c_id, 10)) {
			payTrans.wb_c_data.stringVal[c] = b
			c++
		}
		for _, b := range []byte(strconv.FormatInt(payTrans.d_id, 10)) {
			payTrans.wb_c_data.stringVal[c] = b
			c++
		}
		for _, b := range []byte(strconv.FormatInt(payTrans.c_id, 10)) {
			payTrans.wb_c_data.stringVal[c] = b
			c++
		}
		for _, b := range []byte(strconv.FormatInt(payTrans.w_id, 10)) {
			payTrans.wb_c_data.stringVal[c] = b
			c++
		}
		for _, b := range []byte(strconv.FormatFloat(payTrans.h_amount, 'E', -1, 64)) {
			payTrans.wb_c_data.stringVal[c] = b
			c++
		}
	}

	hTuple := payTrans.hRec.GetTuple().(*HistoryTuple)
	hTuple.h_c_id = payTrans.c_id
	hTuple.h_c_d_id = payTrans.d_id
	hTuple.h_c_w_id = payTrans.c_w_id
	hTuple.h_d_id = payTrans.d_id
	hTuple.h_w_id = payTrans.w_id
	hTuple.h_data = hTuple.h_data[:wTuple.len_w_name+dTuple.len_d_name]
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

	if exec.Commit() == 0 {
		return nil, EABORT
	}
}

func PaymentLast(t Trans, exec ETransaction) (Value, error) {

}

func OrderStatusID(t Trans, exec ETransaction) (Value, error) {

}

func OrderStatusLast(t Trans, exec ETransaction) (Value, error) {

}

func Delivery(t Trans, exec ETransaction) (Value, error) {

}

func StockLevel(t Trans, exec ETransaction) (Value, error) {

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

	intRB := &sbTrnas.intRB
	floatRB := &sbTrnas.floatRB

	tid := sbTrnas.tid
	fv0 := &sbTrnas.fv[0]
	fv1 := &sbTrnas.fv[1]

	var part0, part1 int
	if len(sbTrnas.accessParts) == 1 {
		part0 = sbTrnas.accessParts[0]
		part1 = sbTrnas.accessParts[0]
	} else {
		part0 = sbTrnas.accessParts[0]
		part1 = sbTrnas.accessParts[1]
	}

	acctId0 := sbTrnas.accoutID[0]
	acctId1 := sbTrnas.accoutID[1]

	var val Value
	var err error
	_, _, err = exec.ReadValue(ACCOUNTS, acctId0, part0, intRB, ACCT_ID, tid)
	if err != nil {
		return nil, err
	}

	_, _, err = exec.ReadValue(ACCOUNTS, acctId1, part1, intRB, ACCT_ID, tid)
	if err != nil {
		return nil, err
	}

	err = exec.MayWrite(SAVINGS, acctId0, part0, tid)
	if err != nil {
		return nil, err
	}

	err = exec.MayWrite(CHECKING, acctId1, part1, tid)
	if err != nil {
		return nil, err
	}

	val, _, err = exec.ReadValue(SAVINGS, acctId0, part0, floatRB, SAVING_BAL, tid)
	if err != nil {
		return nil, err
	}
	//sum := val.(*FloatValue).floatVal
	fv0.floatVal = val.(*FloatValue).floatVal

	val, _, err = exec.ReadValue(CHECKING, acctId1, part1, floatRB, CHECK_BAL, tid)
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

	err = exec.WriteValue(CHECKING, acctId1, part1, fv0, CHECK_BAL, tid)
	if err != nil {
		return nil, err
	}

	err = exec.WriteValue(SAVINGS, acctId0, part0, fv1, SAVING_BAL, tid)
	if err != nil {
		return nil, err
	}

	if exec.Commit() == 0 {
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

	intRB := &sbTrnas.intRB
	floatRB := &sbTrnas.floatRB

	tid := sbTrnas.tid
	fv0 := &sbTrnas.fv[0]
	fv1 := &sbTrnas.fv[1]
	ammt := &sbTrnas.ammount

	var part0, part1 int
	if len(sbTrnas.accessParts) == 1 {
		part0 = sbTrnas.accessParts[0]
		part1 = sbTrnas.accessParts[0]
	} else {
		part0 = sbTrnas.accessParts[0]
		part1 = sbTrnas.accessParts[1]
	}

	send := sbTrnas.accoutID[0]
	dest := sbTrnas.accoutID[1]

	var val Value
	var err error
	_, _, err = exec.ReadValue(ACCOUNTS, send, part0, intRB, ACCT_ID, tid)
	if err != nil {
		return nil, err
	}

	_, _, err = exec.ReadValue(ACCOUNTS, dest, part1, intRB, ACCT_ID, tid)
	if err != nil {
		return nil, err
	}

	err = exec.MayWrite(CHECKING, send, part0, tid)
	if err != nil {
		return nil, err
	}

	err = exec.MayWrite(CHECKING, dest, part1, tid)
	if err != nil {
		return nil, err
	}

	val, _, err = exec.ReadValue(CHECKING, send, part0, floatRB, CHECK_BAL, tid)
	if err != nil {
		return nil, err
	}
	bal := val.(*FloatValue).floatVal

	if bal < ammt.floatVal {
		exec.Abort()
		return nil, ELACKBALANCE
	}

	fv0.floatVal = bal - ammt.floatVal

	val, _, err = exec.ReadValue(CHECKING, dest, part1, floatRB, CHECK_BAL, tid)
	if err != nil {
		return nil, err
	}
	fv1.floatVal = val.(*FloatValue).floatVal + ammt.floatVal

	err = exec.WriteValue(CHECKING, send, part0, fv0, CHECK_BAL, tid)
	if err != nil {
		return nil, err
	}

	err = exec.WriteValue(CHECKING, dest, part1, fv1, CHECK_BAL, tid)
	if err != nil {
		return nil, err
	}

	if exec.Commit() == 0 {
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

	intRB := &sbTrnas.intRB
	floatRB := &sbTrnas.floatRB

	tid := sbTrnas.tid
	ret := &sbTrnas.ret
	part := sbTrnas.accessParts[0]
	acct := sbTrnas.accoutID[0]

	var val Value
	var err error
	_, _, err = exec.ReadValue(ACCOUNTS, acct, part, intRB, ACCT_ID, tid)
	if err != nil {
		return nil, err
	}

	val, _, err = exec.ReadValue(CHECKING, acct, part, floatRB, CHECK_BAL, tid)
	if err != nil {
		return nil, err
	}
	ret.floatVal = val.(*FloatValue).floatVal

	val, _, err = exec.ReadValue(SAVINGS, acct, part, floatRB, SAVING_BAL, tid)
	if err != nil {
		return nil, err
	}

	ret.floatVal += val.(*FloatValue).floatVal

	if exec.Commit() == 0 {
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

	intRB := &sbTrnas.intRB
	floatRB := &sbTrnas.floatRB

	tid := sbTrnas.tid
	part := sbTrnas.accessParts[0]
	acct := sbTrnas.accoutID[0]
	ammt := &sbTrnas.ammount
	fv0 := &sbTrnas.fv[0]

	var val Value
	var err error
	_, _, err = exec.ReadValue(ACCOUNTS, acct, part, intRB, ACCT_ID, tid)
	if err != nil {
		return nil, err
	}

	val, _, err = exec.ReadValue(CHECKING, acct, part, floatRB, CHECK_BAL, tid)
	if err != nil {
		return nil, err
	}
	checkBal := val.(*FloatValue).floatVal
	sum := checkBal

	val, _, err = exec.ReadValue(SAVINGS, acct, part, floatRB, SAVING_BAL, tid)
	if err != nil {
		return nil, err
	}
	sum += val.(*FloatValue).floatVal

	if sum < ammt.floatVal {
		fv0.floatVal = checkBal - ammt.floatVal + float64(1)
		err = exec.WriteValue(CHECKING, acct, part, fv0, 1, tid)
		if err != nil {
			return nil, err
		}
	} else {
		fv0.floatVal = checkBal - ammt.floatVal
		err = exec.WriteValue(CHECKING, acct, part, fv0, 1, tid)
		if err != nil {
			return nil, err
		}
	}

	if exec.Commit() == 0 {
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

	intRB := &sbTrnas.intRB
	floatRB := &sbTrnas.floatRB

	tid := sbTrnas.tid
	part := sbTrnas.accessParts[0]
	acct := sbTrnas.accoutID[0]
	ammt := &sbTrnas.ammount
	fv0 := &sbTrnas.fv[0]

	var val Value
	var err error
	_, _, err = exec.ReadValue(ACCOUNTS, acct, part, intRB, ACCT_ID, tid)
	if err != nil {
		return nil, err
	}

	val, _, err = exec.ReadValue(CHECKING, acct, part, floatRB, CHECK_BAL, tid)
	if err != nil {
		return nil, err
	}
	fv0.floatVal = val.(*FloatValue).floatVal + ammt.floatVal

	err = exec.WriteValue(CHECKING, acct, part, fv0, CHECK_BAL, tid)
	if err != nil {
		return nil, err
	}

	if exec.Commit() == 0 {
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

	intRB := &sbTrnas.intRB
	floatRB := &sbTrnas.floatRB

	tid := sbTrnas.tid
	part := sbTrnas.accessParts[0]
	acct := sbTrnas.accoutID[0]
	ammt := &sbTrnas.ammount
	fv0 := &sbTrnas.fv[0]

	var val Value
	var err error
	_, _, err = exec.ReadValue(ACCOUNTS, acct, part, intRB, ACCT_ID, tid)
	if err != nil {
		return nil, err
	}

	val, _, err = exec.ReadValue(SAVINGS, acct, part, floatRB, SAVING_BAL, tid)
	if err != nil {
		return nil, err
	}
	sum := val.(*FloatValue).floatVal + ammt.floatVal

	if sum < 0 {
		return nil, ENEGSAVINGS
	} else {
		fv0.floatVal = sum
		err = exec.WriteValue(SAVINGS, acct, part, fv0, SAVING_BAL, tid)
		if err != nil {
			return nil, err
		}
	}

	if exec.Commit() == 0 {
		return nil, EABORT
	}

	return nil, nil
}

func AddOne(t Trans, exec ETransaction) (Value, error) {
	singleTrans := t.(*SingleTrans)
	iv := singleTrans.iv

	tid := singleTrans.tid
	intRB := &singleTrans.intRB
	var k Key
	var part int
	var val Value
	var err error
	//var fromStore bool
	for i := 0; i < len(singleTrans.keys); i++ {
		k = singleTrans.keys[i]
		part = singleTrans.parts[i]

		err = exec.MayWrite(SINGLE, k, part, tid)
		if err != nil {
			return nil, err
		}

		//val, fromStore, err = exec.ReadValue(SINGLE, k, part, intRB, SINGLE_VAL, tid)
		val, _, err = exec.ReadValue(SINGLE, k, part, intRB, SINGLE_VAL, tid)
		if err != nil {
			return nil, err
		}

		/*if fromStore {
			iv[i].intVal = *val.(*int64) + 1
		} else {
			iv[i].intVal = val.(*IntValue).intVal + 1
		}*/
		iv[i].intVal = val.(*IntValue).intVal + 1

		err = exec.WriteValue(SINGLE, k, part, &iv[i], SINGLE_VAL, tid)
		if err != nil {
			return nil, err
		}

	}

	if exec.Commit() == 0 {
		return nil, EABORT
	}

	return nil, nil
}

func UpdateInt(t Trans, exec ETransaction) (Value, error) {
	singleTrans := t.(*SingleTrans)
	sv := singleTrans.sv
	strRB := &singleTrans.strRB
	tid := singleTrans.tid
	var k Key
	var part int
	var err error
	var val Value
	var col int
	for i := 0; i < len(singleTrans.keys); i++ {
		k = singleTrans.keys[i]
		part = singleTrans.parts[i]
		col = singleTrans.rnd.Intn(20) + SINGLE_VAL + 1

		if singleTrans.rnd.Intn(100) < singleTrans.rr {
			val, _, err = exec.ReadValue(SINGLE, k, part, strRB, col, tid)
			if err != nil {
				return nil, err
			}
			if val == nil {
				return nil, ENOKEY
			}
		} else {
			sv[i].stringVal = sv[i].stringVal[:CAP_SINGLE_STR]
			//for p, b := range CONST_STR_SINGLE {
			//	sv[i].stringVal[p] = byte(b)
			//}
			err = exec.WriteValue(SINGLE, k, part, &sv[i], col, tid)
			if err != nil {
				return nil, err
			}
		}
	}

	if exec.Commit() == 0 {
		return nil, EABORT
	}

	return nil, nil
}
