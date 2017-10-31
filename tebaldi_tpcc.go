package testbed

import (
	"strconv"
	//"time"

	//"github.com/totemtang/cc-testbed/clog"

	//"runtime/debug"
)

var GROUPA = 0
var GROUPB = 1
var LOCK = true
var NOTLOCK = false

var NO_Dist_Locks []SpinLockPad

func Init_Tebaldi(warehouse int) {
	NO_Dist_Locks = make([]SpinLockPad, warehouse*10)
}

func NewOrder_Tebaldi(t Trans, exec ETransaction) (Value, error) {
	noTrans := t.(*NewOrderTrans)

	tx := exec.(*TTransaction)
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
	rec, err = tx.GetRecord_Ex(WAREHOUSE, k, partNum, req, NOTLOCK, GROUPA)
	if err != nil {
		return nil, err
	}
	rec.GetValue(floatRB, W_TAX)
	w_tax := floatRB.floatVal

	// Get D_TAX and D_NEXT_O_ID
	d_id := noTrans.d_id
	k[1] = d_id
	k[3] = noTrans.d_hot_bit
	rec, err = tx.GetRecord_Ex(DISTRICT, k, partNum, req, NOTLOCK, GROUPA)
	if err != nil {
		return nil, err
	}

	distTuple := rec.GetTuple().(*DistrictTuple)
	d_tax := distTuple.d_tax
	d_next_o_id := distTuple.d_next_o_id

	// Lock district
	d_num := w_id*10 + d_id
	NO_Dist_Locks[d_num].Lock()

	// Increment D_NEXT_O_ID
	wb_next_o_id := &noTrans.wb_next_o_id
	wb_next_o_id.intVal = d_next_o_id + 1
	err = tx.WriteValue_Ex(DISTRICT, k, partNum, wb_next_o_id, D_NEXT_O_ID, req, false, NOTLOCK, GROUPA, rec)
	if err != nil {
		return nil, err
	}

	// Release the lock early
	NO_Dist_Locks[d_num].Unlock()

	// Get One Record from Customer
	c_id := noTrans.c_id
	k[2] = c_id
	k[3] = noTrans.c_hot_bit
	rec, err = tx.GetRecord_Ex(CUSTOMER, k, partNum, req, NOTLOCK, GROUPA)
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
	err = tx.InsertRecord(NEWORDER, k, partNum, noTrans.noRec)
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
	err = tx.InsertRecord(ORDER, k, partNum, noTrans.oRec)
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

	// Sort
	j := 0
	for i := 1; i < int(noTrans.ol_cnt); i++ {
		temp_w_id := noTrans.ol_supply_w_id[i]
		temp_i_id := noTrans.ol_i_id[i]
		for j = i - 1; j >= 0; j-- {
			cur_w_id := noTrans.ol_supply_w_id[j]
			cur_i_id := noTrans.ol_i_id[j]
			if temp_w_id < cur_w_id || (temp_w_id == cur_w_id && temp_i_id < cur_i_id) {
				noTrans.ol_supply_w_id[j+1] = cur_w_id
				noTrans.ol_i_id[j+1] = cur_i_id
			} else {
				break
			}
		}
		noTrans.ol_supply_w_id[j+1] = temp_w_id
		noTrans.ol_i_id[j+1] = temp_i_id
	}

	for i := 0; i < int(noTrans.ol_cnt); i++ {
		k[0] = noTrans.ol_i_id[i]
		rec, _ = table.data[0].ht.Get(k)
		iTuple = rec.GetTuple().(*ItemTuple)

		sKey[0] = noTrans.ol_supply_w_id[i]
		sKey[1] = noTrans.ol_i_id[i]
		sKey[2] = 0
		sKey[3] = noTrans.ol_hot_bit[i]

		if !distRead {
			_, _, _, err = tx.ReadValue_Ex(STOCK, sKey, sKey[0], rb_o_dist, S_DIST_01+d_id, req, LOCK, GROUPA)
			if err != nil {
				return nil, err
			}
			distRead = true
		}

		// Update s_quantity
		rec, val, _, err = tx.ReadValue_Ex(STOCK, sKey, sKey[0], intRB, S_QUANTITY, req, LOCK, GROUPA)
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
		err = tx.WriteValue_Ex(STOCK, sKey, sKey[0], &noTrans.wb_s_quantity[i], S_QUANTITY, req, false, LOCK, GROUPA, rec)
		if err != nil {
			return nil, err
		}

		// Update S_YTD
		//val, _, err = exec.ReadValue(STOCK, sKey, int(sKeyAr[0]), intRB, S_YTD, req)
		//if err != nil {
		//	return nil, err
		//}
		noTrans.wb_s_ytd[i].intVal = 1
		err = tx.WriteValue_Ex(STOCK, sKey, sKey[0], &noTrans.wb_s_ytd[i], S_YTD, req, true, LOCK, GROUPA, rec)
		if err != nil {
			return nil, err
		}

		// Update S_ORDER_CNT
		//val, _, err = exec.ReadValue(STOCK, sKey, int(sKeyAr[0]), intRB, S_ORDER_CNT, req)
		//if err != nil {
		//	return nil, err
		//}
		noTrans.wb_s_order_cnt[i].intVal = 1
		err = tx.WriteValue_Ex(STOCK, sKey, sKey[0], &noTrans.wb_s_order_cnt[i], S_ORDER_CNT, req, true, LOCK, GROUPA, rec)
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
			err = tx.WriteValue_Ex(STOCK, sKey, sKey[0], &noTrans.wb_s_remote_cnt[i], S_REMOTE_CNT, req, true, LOCK, GROUPA, rec)
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
		tx.InsertRecord(ORDERLINE, sKey, sKey[0], noTrans.olRec[i])

		totalAmount += olTuple.ol_amount
	}

	noTrans.retVal.floatVal = totalAmount * (1 - c_discount) * (1 + w_tax + d_tax)

	if tx.Commit_Ex(req, isHome, GROUPA) == 0 {
		return nil, EABORT
	}

	return &noTrans.retVal, nil
}

func Payment_Tebaldi(t Trans, exec ETransaction) (Value, error) {
	payTrans := t.(*PaymentTrans)
	tx := exec.(*TTransaction)

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
	rec, err = tx.GetRecord_Ex(WAREHOUSE, k, partNum, req, LOCK, GROUPA)
	if err != nil {
		return nil, err
	}
	wTuple := rec.GetTuple().(*WarehouseTuple)
	payTrans.wb_w_ytd.floatVal = wTuple.w_ytd + payTrans.h_amount
	err = tx.WriteValue_Ex(WAREHOUSE, k, partNum, &payTrans.wb_w_ytd, W_YTD, req, false, LOCK, GROUPA, rec)
	if err != nil {
		return nil, err
	}

	// Increment D_YTD in district
	k[1] = payTrans.d_id
	k[3] = payTrans.d_hot_bit
	rec, err = tx.GetRecord_Ex(DISTRICT, k, partNum, req, LOCK, GROUPA)
	if err != nil {
		return nil, err
	}
	dTuple := rec.GetTuple().(*DistrictTuple)
	payTrans.wb_d_ytd.floatVal = dTuple.d_ytd + payTrans.h_amount
	err = tx.WriteValue_Ex(DISTRICT, k, partNum, &payTrans.wb_d_ytd, D_YTD, req, false, LOCK, GROUPA, rec)
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
		err = tx.GetKeysBySecIndex(CUSTOMER, k, remotePart, intRB)
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
	rec, err = tx.GetRecord_Ex(CUSTOMER, k, remotePart, req, LOCK, GROUPA)
	if err != nil {
		return nil, err
	}
	cTuple := rec.GetTuple().(*CustomerTuple)
	// Decrease C_BALANCE
	payTrans.wb_c_balance.floatVal = cTuple.c_balance - payTrans.h_amount
	err = tx.WriteValue_Ex(CUSTOMER, k, remotePart, &payTrans.wb_c_balance, C_BALANCE, req, false, LOCK, GROUPA, rec)
	if err != nil {
		return nil, err
	}
	// Increase C_YTD_PAYMENT
	payTrans.wb_c_ytd_payment.floatVal = cTuple.c_ytd_payment + payTrans.h_amount
	err = tx.WriteValue_Ex(CUSTOMER, k, remotePart, &payTrans.wb_c_ytd_payment, C_YTD_PAYMENT, req, false, LOCK, GROUPA, rec)
	if err != nil {
		return nil, err
	}

	// Increase C_PAYMENT_CNT
	payTrans.wb_c_payment_cnt.intVal = cTuple.c_payment_cnt + 1
	err = tx.WriteValue_Ex(CUSTOMER, k, remotePart, &payTrans.wb_c_payment_cnt, C_PAYMENT_CNT, req, false, LOCK, GROUPA, rec)
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

	err = tx.InsertRecord(HISTORY, k, partNum, payTrans.hRec)
	if err != nil {
		return nil, err
	}

	if tx.Commit_Ex(req, isHome, GROUPA) == 0 {
		return nil, EABORT
	}

	return nil, nil
}

func OrderStatus_Tebaldi(t Trans, exec ETransaction) (Value, error) {
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

func Delivery_Tebaldi(t Trans, exec ETransaction) (Value, error) {
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

	return nil, nil
}

func StockLevel_Tebaldi(t Trans, exec ETransaction) (Value, error) {
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
