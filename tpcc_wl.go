package testbed

import (
	"math/rand"
	"time"
)

const (
	MAXOLCNT = 15
)

type BaseTrans struct {
	accessParts []int
	trail       int
	tid         TID
	intRB       IntValue
	floatRB     FloatValue
	dateRB      DateValue
	rnd         rand.Rand
}

func (bt *BaseTrans) GetTXN() int {
	return bt.TXN
}

func (bt *BaseTrans) GetAccessParts() []int {
	return bt.accessParts
}

func (bt *BaseTrans) SetTID(tid TID) {
	bt.tid = tid
}

func (bt *BaseTrans) SetTrial(trial int) {
	bt.trial = trial
}

func (bt *BaseTrans) GetTrial() int {
	return bt.trial
}

func (bt *BaseTrans) DecTrial() {
	bt.trial--
}

type NewOrderTrans struct {
	padding1 [PADDING]byte
	BaseTrans
	w_id            int64
	d_id            int64
	c_id            int64
	o_entry_d       time.Time
	ol_cnt          int64
	ol_i_id         [MAXOLCNT]int64
	ol_quantity     [MAXOLCNT]int64
	ol_supply_w_id  [MAXOLCNT]int64
	wb_next_o_id    IntValue
	wb_s_quantity   [MAXOLCNT]IntValue
	wb_s_ytd        [MAXOLCNT]IntValue
	wb_s_order_cnt  [MAXOLCNT]IntValue
	wb_s_remote_cnt [MAXOLCNT]IntValue
	rb_c_last       StringValue
	rb_c_credit     StringValue
	rb_i_name       StringValue
	rb_o_dist       StringValue
	noRec           Record
	oRec            Record
	olRec           [MAXOLCNT]Record
	retVal          FloatValue
	padding2        [PADDING]byte
}

type PaymentIDTrans struct {
	padding1 [PADDING]byte
	BaseTrans
	w_id             int64
	d_id             int64
	c_id             int64
	c_w_id           int64
	h_amount         float64
	wb_w_ytd         FloatValue
	wb_d_ytd         FloatValue
	wb_c_balance     FloatValue
	wb_c_ytd_payment FloatValue
	wb_c_payment_cnt IntValue
	wb_c_data        StringValue
	hRec             Record
	padding2         [PADDING]byte
}

type PaymentLastTrans struct {
	padding1 [PADDING]byte
	BaseTrans
	w_id     int64
	d_id     int64
	c_last   [CAP_C_LAST]byte
	c_w_id   int64
	h_amount float64
	padding2 [PADDING]byte
}

type OrderStatusIDTrans struct {
	padding1 [PADDING]byte
	BaseTrans
	w_id     int64
	d_id     int64
	c_id     int64
	padding2 [PADDING]byte
}

type OrderStatusLastTrans struct {
	padding1 [PADDING]byte
	BaseTrans
	w_id     int64
	d_id     int64
	c_last   [CAP_C_LAST]byte
	padding2 [PADDING]byte
}

type DeliveryTrans struct {
	padding1 [PADDING]byte
	BaseTrans
	w_id         int64
	o_carrier_id int64
	padding2     [PADDING]byte
}

type StockLevelTrans struct {
	padding1 [PADDING]byte
	BaseTrans
	w_id      int64
	d_id      int64
	threshold int64
	padding2  [PADDING]byte
}
