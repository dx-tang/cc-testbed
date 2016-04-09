package testbed

import (
	"github.com/totemtang/cc-testbed/clog"
	"time"
)

// Table Reference Constants
const (
	WAREHOUSE = iota
	DISTRICT
	CUSTOMER
	HISTORY
	NEWORDER
	ORDER
	ORDERLINE
	ITEM
	STOCK
)

// Column Reference Constants
// WAREHOUSE
const (
	W_ID = iota
	W_NAME
	W_STREET_1
	W_STREET_2
	W_CITY
	W_STATE
	W_ZIP
	W_TAX
	W_YTD
)

const (
	CAP_STREET_1 = 20
	CAP_STREET_2 = 20
	CAP_CITY     = 20
	CAP_STATE    = 2
	CAP_ZIP      = 9
)

const (
	CAP_W_NAME = 10
)

// 249 bytes
type WarehouseTuple struct {
	padding1       [PADDING]byte
	w_id           int64
	w_name         [CAP_W_NAME]byte
	len_w_name     int
	w_street_1     [CAP_STREET_1]byte
	len_w_street_1 int
	w_street_2     [CAP_STREET_2]byte
	len_w_street_2 int
	w_city         [CAP_CITY]byte
	len_w_city     int
	w_state        [CAP_STATE]byte
	w_zip          [CAP_ZIP]byte
	w_tax          float64
	w_ytd          float64
	padding2       [PADDING]byte
}

func (w *WarehouseTuple) GetValue(val Value, col int) {
	switch col {
	case W_ID:
		val.(*IntValue).intVal = w.w_id
	case W_NAME:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:w.len_w_name]
		src := w.w_name[:w.len_w_name]
		copy(sv.stringVal, src)
	case W_STREET_1:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:w.len_w_street_1]
		src := w.w_street_1[:w.len_w_street_1]
		copy(sv.stringVal, src)
	case W_STREET_2:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:w.len_w_street_2]
		src := w.w_street_2[:w.len_w_street_2]
		copy(sv.stringVal, src)
	case W_CITY:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:w.len_w_city]
		src := w.w_city[:w.len_w_city]
		copy(sv.stringVal, src)
	case W_STATE:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:CAP_STATE]
		src := w.w_state[:CAP_STATE]
		copy(sv.stringVal, src)
	case W_ZIP:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:CAP_ZIP]
		src := w.w_zip[:CAP_ZIP]
		copy(sv.stringVal, src)
	case W_TAX:
		val.(*FloatValue).floatVal = w.w_tax
	case W_YTD:
		val.(*FloatValue).floatVal = w.w_ytd
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

func (w *WarehouseTuple) SetValue(val Value, col int) {
	switch col {
	case W_ID:
		w.w_id = val.(*IntValue).intVal
	case W_NAME:
		sv := val.(*StringValue)
		w.len_w_name = len(sv.stringVal)
		dest := w.w_name[:w.len_w_name]
		copy(dest, sv.stringVal)
	case W_STREET_1:
		sv := val.(*StringValue)
		w.len_w_street_1 = len(sv.stringVal)
		dest := w.w_street_1[:w.len_w_street_1]
		copy(dest, sv.stringVal)
	case W_STREET_2:
		sv := val.(*StringValue)
		w.len_w_street_2 = len(sv.stringVal)
		dest := w.w_street_2[:w.len_w_street_2]
		copy(dest, sv.stringVal)
	case W_CITY:
		sv := val.(*StringValue)
		w.len_w_city = len(sv.stringVal)
		dest := w.w_city[:w.len_w_city]
		copy(dest, sv.stringVal)
	case W_STATE:
		sv := val.(*StringValue)
		dest := w.w_state[:CAP_STATE]
		copy(dest, sv.stringVal)
	case W_ZIP:
		sv := val.(*StringValue)
		dest := w.w_zip[:CAP_ZIP]
		copy(dest, sv.stringVal)
	case W_TAX:
		w.w_tax = val.(*FloatValue).floatVal
	case W_YTD:
		w.w_ytd = val.(*FloatValue).floatVal
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

// DISTRICT
const (
	D_ID = iota
	D_W_ID
	D_NAME
	D_STREET_1
	D_STREET_2
	D_CITY
	D_STATE
	D_ZIP
	D_TAX
	D_YTD
	D_NEXT_O_ID
)

const (
	CAP_D_NAME = 10
)

// 265 bytes
type DistrictTuple struct {
	padding1       [PADDING]byte
	d_id           int64
	d_w_id         int64
	d_name         [CAP_D_NAME]byte
	len_d_name     int
	d_street_1     [CAP_STREET_1]byte
	len_d_street_1 int
	d_street_2     [CAP_STREET_2]byte
	len_d_street_2 int
	d_city         [CAP_CITY]byte
	len_d_city     int
	d_state        [CAP_STATE]byte
	d_zip          [CAP_ZIP]byte
	d_tax          float64
	d_ytd          float64
	d_next_o_id    int64
	padding2       [PADDING]byte
}

func (d *DistrictTuple) GetValue(val Value, col int) {
	switch col {
	case D_ID:
		val.(*IntValue).intVal = d.d_id
	case D_W_ID:
		val.(*IntValue).intVal = d.d_w_id
	case D_NAME:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:d.len_d_name]
		src := d.d_name[:d.len_d_name]
		copy(sv.stringVal, src)
	case D_STREET_1:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:d.len_d_street_1]
		src := d.d_street_1[:d.len_d_street_1]
		copy(sv.stringVal, src)
	case D_STREET_2:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:d.len_d_street_2]
		src := d.d_street_2[:d.len_d_street_2]
		copy(sv.stringVal, src)
	case D_CITY:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:d.len_d_city]
		src := d.d_city[:d.len_d_city]
		copy(sv.stringVal, src)
	case D_STATE:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:CAP_STATE]
		src := d.d_state[:CAP_STATE]
		copy(sv.stringVal, src)
	case D_ZIP:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:CAP_ZIP]
		src := d.d_zip[:CAP_ZIP]
		copy(sv.stringVal, src)
	case D_TAX:
		val.(*FloatValue).floatVal = d.d_tax
	case D_YTD:
		val.(*FloatValue).floatVal = d.d_ytd
	case D_NEXT_O_ID:
		val.(*IntValue).intVal = d.d_next_o_id
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

func (d *DistrictTuple) SetValue(val Value, col int) {
	switch col {
	case D_ID:
		d.d_id = val.(*IntValue).intVal
	case D_W_ID:
		d.d_w_id = val.(*IntValue).intVal
	case D_NAME:
		sv := val.(*StringValue)
		d.len_d_name = len(sv.stringVal)
		dest := d.d_name[:d.len_d_name]
		copy(dest, sv.stringVal)
	case D_STREET_1:
		sv := val.(*StringValue)
		d.len_d_street_1 = len(sv.stringVal)
		dest := d.d_street_1[:d.len_d_street_1]
		copy(dest, sv.stringVal)
	case D_STREET_2:
		sv := val.(*StringValue)
		d.len_d_street_2 = len(sv.stringVal)
		dest := d.d_street_2[:d.len_d_street_2]
		copy(dest, sv.stringVal)
	case D_CITY:
		sv := val.(*StringValue)
		d.len_d_city = len(sv.stringVal)
		dest := d.d_city[:d.len_d_city]
		copy(dest, sv.stringVal)
	case D_STATE:
		sv := val.(*StringValue)
		dest := d.d_state[:CAP_STATE]
		copy(dest, sv.stringVal)
	case D_ZIP:
		sv := val.(*StringValue)
		dest := d.d_zip[:CAP_ZIP]
		copy(dest, sv.stringVal)
	case D_TAX:
		d.d_tax = val.(*FloatValue).floatVal
	case D_YTD:
		d.d_ytd = val.(*FloatValue).floatVal
	case D_NEXT_O_ID:
		d.d_next_o_id = val.(*IntValue).intVal
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

// CUSTOMER
const (
	C_ID = iota
	C_D_ID
	C_W_ID
	C_FIRST
	C_MIDDLE
	C_LAST
	C_STREET_1
	C_STREET_2
	C_CITY
	C_STATE
	C_ZIP
	C_PHONE
	C_SINCE
	C_CREDIT
	C_CREDIT_LIM
	C_DISCOUNT
	C_BALANCE
	C_YTD_PAYMENT
	C_PAYMENT_CNT
	C_DELIVERY_CNT
	C_DATA
)

const (
	CAP_C_FIRST  = 16
	CAP_C_MIDDLE = 2
	CAP_C_LAST   = 16
	CAP_C_PHONE  = 16
	CAP_C_CREDIT = 2
	CAP_C_DATA   = 500
)

type CustomerTuple struct {
	padding1       [PADDING]byte
	c_id           int64
	c_d_id         int64
	c_w_id         int64
	c_first        [CAP_C_FIRST]byte
	len_c_first    int
	c_middle       [CAP_C_MIDDLE]byte
	c_last         [CAP_C_LAST]byte
	len_c_last     int
	c_street_1     [CAP_STREET_1]byte
	len_c_street_1 int
	c_street_2     [CAP_STREET_2]byte
	len_c_street_2 int
	c_city         [CAP_CITY]byte
	len_c_city     int
	c_state        [CAP_STATE]byte
	c_zip          [CAP_ZIP]byte
	c_phone        [CAP_C_PHONE]byte
	c_since        time.Time
	c_credit       [CAP_C_CREDIT]byte
	c_credit_lim   float64
	c_discount     float64
	c_balance      float64
	c_ytd_payment  float64
	c_payment_cnt  int64
	c_delivery_cnt int64
	c_data         [CAP_C_DATA]byte
	len_c_data     int
	padding2       [PADDING]byte
}

func (c *CustomerTuple) GetValue(val Value, col int) {
	switch col {
	case C_ID:
		val.(*IntValue).intVal = c.c_id
	case C_D_ID:
		val.(*IntValue).intVal = c.c_d_id
	case C_W_ID:
		val.(*IntValue).intVal = c.c_w_id
	case C_FIRST:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:c.len_c_first]
		src := c.c_first[:c.len_c_first]
		copy(sv.stringVal, src)
	case C_MIDDLE:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:CAP_C_MIDDLE]
		src := c.c_middle[:CAP_C_MIDDLE]
		copy(sv.stringVal, src)
	case C_LAST:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:c.len_c_last]
		src := c.c_last[:c.len_c_last]
		copy(sv.stringVal, src)
	case C_STREET_1:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:c.len_c_street_1]
		src := c.c_street_1[:c.len_c_street_1]
		copy(sv.stringVal, src)
	case C_STREET_2:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:c.len_c_street_2]
		src := c.c_street_2[:c.len_c_street_2]
		copy(sv.stringVal, src)
	case C_CITY:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:c.len_c_city]
		src := c.c_city[:c.len_c_city]
		copy(sv.stringVal, src)
	case C_STATE:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:CAP_STATE]
		src := c.c_state[:CAP_STATE]
		copy(sv.stringVal, src)
	case C_ZIP:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:CAP_ZIP]
		src := c.c_zip[:CAP_ZIP]
		copy(sv.stringVal, src)
	case C_PHONE:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:CAP_C_PHONE]
		src := c.c_phone[:CAP_C_PHONE]
		copy(sv.stringVal, src)
	case C_SINCE:
		val.(*DateValue).dateVal = c.c_since
	case C_CREDIT:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:CAP_C_CREDIT]
		src := c.c_credit[:CAP_C_CREDIT]
		copy(sv.stringVal, src)
	case C_CREDIT_LIM:
		val.(*FloatValue).floatVal = c.c_credit_lim
	case C_DISCOUNT:
		val.(*FloatValue).floatVal = c.c_discount
	case C_BALANCE:
		val.(*FloatValue).floatVal = c.c_balance
	case C_YTD_PAYMENT:
		val.(*FloatValue).floatVal = c.c_ytd_payment
	case C_PAYMENT_CNT:
		val.(*IntValue).intVal = c.c_payment_cnt
	case C_DELIVERY_CNT:
		val.(*IntValue).intVal = c.c_delivery_cnt
	case C_DATA:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:c.len_c_data]
		src := c.c_data[:c.len_c_data]
		copy(sv.stringVal, src)
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

func (c *CustomerTuple) SetValue(val Value, col int) {
	switch col {
	case C_ID:
		c.c_id = val.(*IntValue).intVal
	case C_D_ID:
		c.c_d_id = val.(*IntValue).intVal
	case C_W_ID:
		c.c_w_id = val.(*IntValue).intVal
	case C_FIRST:
		sv := val.(*StringValue)
		c.len_c_first = len(sv.stringVal)
		dest := c.c_first[:c.len_c_first]
		copy(dest, sv.stringVal)
	case C_MIDDLE:
		sv := val.(*StringValue)
		dest := c.c_middle[:CAP_C_MIDDLE]
		copy(dest, sv.stringVal)
	case C_LAST:
		sv := val.(*StringValue)
		c.len_c_last = len(sv.stringVal)
		dest := c.c_last[:c.len_c_last]
		copy(dest, sv.stringVal)
	case C_STREET_1:
		sv := val.(*StringValue)
		c.len_c_street_1 = len(sv.stringVal)
		dest := c.c_street_1[:c.len_c_street_1]
		copy(dest, sv.stringVal)
	case C_STREET_2:
		sv := val.(*StringValue)
		c.len_c_street_2 = len(sv.stringVal)
		dest := c.c_street_2[:c.len_c_street_2]
		copy(dest, sv.stringVal)
	case C_CITY:
		sv := val.(*StringValue)
		c.len_c_city = len(sv.stringVal)
		dest := c.c_city[:c.len_c_city]
		copy(dest, sv.stringVal)
	case C_STATE:
		sv := val.(*StringValue)
		dest := c.c_state[:CAP_STATE]
		copy(dest, sv.stringVal)
	case C_ZIP:
		sv := val.(*StringValue)
		dest := c.c_zip[:CAP_ZIP]
		copy(dest, sv.stringVal)
	case C_PHONE:
		sv := val.(*StringValue)
		dest := c.c_phone[:CAP_C_PHONE]
		copy(dest, sv.stringVal)
	case C_SINCE:
		c.c_since = val.(*DateValue).dateVal
	case C_CREDIT:
		sv := val.(*StringValue)
		dest := c.c_credit[:CAP_C_CREDIT]
		copy(dest, sv.stringVal)
	case C_CREDIT_LIM:
		c.c_credit_lim = val.(*FloatValue).floatVal
	case C_DISCOUNT:
		c.c_discount = val.(*FloatValue).floatVal
	case C_BALANCE:
		c.c_balance = val.(*FloatValue).floatVal
	case C_YTD_PAYMENT:
		c.c_ytd_payment = val.(*FloatValue).floatVal
	case C_PAYMENT_CNT:
		c.c_payment_cnt = val.(*IntValue).intVal
	case C_DELIVERY_CNT:
		c.c_delivery_cnt = val.(*IntValue).intVal
	case C_DATA:
		sv := val.(*StringValue)
		c.len_c_data = len(sv.stringVal)
		dest := c.c_data[:c.len_c_data]
		copy(dest, sv.stringVal)
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

// HISTORY
const (
	H_C_ID = iota
	H_C_D_ID
	H_C_W_ID
	H_D_ID
	H_W_ID
	H_DATE
	H_AMOUNT
	H_DATA
)

const (
	CAP_H_DATA = 24
)

type HistoryTuple struct {
	padding1   [PADDING]byte
	h_c_id     int64
	h_c_d_id   int64
	h_c_w_id   int64
	h_d_id     int64
	h_w_id     int64
	h_date     time.Time
	h_amount   float64
	h_data     [CAP_H_DATA]byte
	len_h_data int
	padding2   [PADDING]byte
}

func (h *HistoryTuple) GetValue(val Value, col int) {
	switch col {
	case H_C_ID:
		val.(*IntValue).intVal = h.h_c_id
	case H_C_D_ID:
		val.(*IntValue).intVal = h.h_c_d_id
	case H_C_W_ID:
		val.(*IntValue).intVal = h.h_c_w_id
	case H_D_ID:
		val.(*IntValue).intVal = h.h_d_id
	case H_W_ID:
		val.(*IntValue).intVal = h.h_w_id
	case H_DATE:
		val.(*DateValue).dateVal = h.h_date
	case H_AMOUNT:
		val.(*FloatValue).floatVal = h.h_amount
	case H_DATA:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:h.len_h_data]
		src := h.h_data[:h.len_h_data]
		copy(sv.stringVal, src)
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

func (h *HistoryTuple) SetValue(val Value, col int) {
	switch col {
	case H_C_ID:
		h.h_c_id = val.(*IntValue).intVal
	case H_C_D_ID:
		h.h_c_d_id = val.(*IntValue).intVal
	case H_C_W_ID:
		h.h_c_w_id = val.(*IntValue).intVal
	case H_D_ID:
		h.h_d_id = val.(*IntValue).intVal
	case H_W_ID:
		h.h_w_id = val.(*IntValue).intVal
	case H_DATE:
		h.h_date = val.(*DateValue).dateVal
	case H_AMOUNT:
		h.h_amount = val.(*FloatValue).floatVal
	case H_DATA:
		sv := val.(*StringValue)
		h.len_h_data = len(sv.stringVal)
		dest := h.h_data[:h.len_h_data]
		copy(dest, sv.stringVal)
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

// NEW-ORDER
const (
	NO_O_ID = iota
	NO_D_ID
	NO_W_ID
)

type NewOrderTuple struct {
	padding1 [PADDING]byte
	no_o_id  int64
	no_d_id  int64
	no_w_id  int64
	padding2 [PADDING]byte
}

func (no *NewOrderTuple) GetValue(val Value, col int) {
	switch col {
	case NO_O_ID:
		val.(*IntValue).intVal = no.no_o_id
	case NO_D_ID:
		val.(*IntValue).intVal = no.no_d_id
	case NO_W_ID:
		val.(*IntValue).intVal = no.no_w_id
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

func (no *NewOrderTuple) SetValue(val Value, col int) {
	switch col {
	case NO_O_ID:
		no.no_o_id = val.(*IntValue).intVal
	case NO_D_ID:
		no.no_d_id = val.(*IntValue).intVal
	case NO_W_ID:
		no.no_w_id = val.(*IntValue).intVal
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

// ORDER
const (
	O_ID = iota
	O_D_ID
	O_W_ID
	O_C_ID
	O_ENTRY_D
	O_CARRIER_ID
	O_OL_CNT
	O_ALL_LOCAL
)

type OrderTuple struct {
	padding1     [PADDING]byte
	o_id         int64
	o_d_id       int64
	o_w_id       int64
	o_c_id       int64
	o_entry_d    time.Time
	o_carrier_id int64
	o_ol_cnt     int64
	o_all_local  int64
	padding2     [PADDING]byte
}

func (o *OrderTuple) GetValue(val Value, col int) {
	switch col {
	case O_ID:
		val.(*IntValue).intVal = o.o_id
	case O_D_ID:
		val.(*IntValue).intVal = o.o_d_id
	case O_W_ID:
		val.(*IntValue).intVal = o.o_w_id
	case O_C_ID:
		val.(*IntValue).intVal = o.o_c_id
	case O_ENTRY_D:
		val.(*DateValue).dateVal = o.o_entry_d
	case O_CARRIER_ID:
		val.(*IntValue).intVal = o.o_carrier_id
	case O_OL_CNT:
		val.(*IntValue).intVal = o.o_ol_cnt
	case O_ALL_LOCAL:
		val.(*IntValue).intVal = o.o_all_local
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

func (o *OrderTuple) SetValue(val Value, col int) {
	switch col {
	case O_ID:
		o.o_id = val.(*IntValue).intVal
	case O_D_ID:
		o.o_d_id = val.(*IntValue).intVal
	case O_W_ID:
		o.o_w_id = val.(*IntValue).intVal
	case O_C_ID:
		o.o_c_id = val.(*IntValue).intVal
	case O_ENTRY_D:
		o.o_entry_d = val.(*DateValue).dateVal
	case O_CARRIER_ID:
		o.o_carrier_id = val.(*IntValue).intVal
	case O_OL_CNT:
		o.o_ol_cnt = val.(*IntValue).intVal
	case O_ALL_LOCAL:
		o.o_all_local = val.(*IntValue).intVal
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

// ORDER-LINE
const (
	OL_O_ID = iota
	OL_D_ID
	OL_W_ID
	OL_NUMBER
	OL_I_ID
	OL_SUPPLY_W_ID
	OL_DELIVERY_D
	OL_QUANTITY
	OL_AMOUNT
	OL_DIST_INFO
)

const (
	CAP_DIST = 24
)

type OrderLineTuple struct {
	padding1       [PADDING]byte
	ol_o_id        int64
	ol_d_id        int64
	ol_w_id        int64
	ol_number      int64
	ol_i_id        int64
	ol_supply_w_id int64
	ol_delivery_d  time.Time
	ol_quantity    int64
	ol_amount      float64
	ol_dist_info   [CAP_DIST]byte
	padding2       [PADDING]byte
}

func (ol *OrderLineTuple) GetValue(val Value, col int) {
	switch col {
	case OL_O_ID:
		val.(*IntValue).intVal = ol.ol_o_id
	case OL_D_ID:
		val.(*IntValue).intVal = ol.ol_d_id
	case OL_W_ID:
		val.(*IntValue).intVal = ol.ol_w_id
	case OL_NUMBER:
		val.(*IntValue).intVal = ol.ol_number
	case OL_I_ID:
		val.(*IntValue).intVal = ol.ol_i_id
	case OL_SUPPLY_W_ID:
		val.(*IntValue).intVal = ol.ol_supply_w_id
	case OL_DELIVERY_D:
		val.(*DateValue).dateVal = ol.ol_delivery_d
	case OL_QUANTITY:
		val.(*IntValue).intVal = ol.ol_quantity
	case OL_AMOUNT:
		val.(*FloatValue).floatVal = ol.ol_amount
	case OL_DIST_INFO:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:CAP_DIST]
		src := ol.ol_dist_info[:CAP_DIST]
		copy(sv.stringVal, src)
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

func (ol *OrderLineTuple) SetValue(val Value, col int) {
	switch col {
	case OL_O_ID:
		ol.ol_o_id = val.(*IntValue).intVal
	case OL_D_ID:
		ol.ol_d_id = val.(*IntValue).intVal
	case OL_W_ID:
		ol.ol_w_id = val.(*IntValue).intVal
	case OL_NUMBER:
		ol.ol_number = val.(*IntValue).intVal
	case OL_I_ID:
		ol.ol_i_id = val.(*IntValue).intVal
	case OL_SUPPLY_W_ID:
		ol.ol_supply_w_id = val.(*IntValue).intVal
	case OL_DELIVERY_D:
		ol.ol_delivery_d = val.(*DateValue).dateVal
	case OL_QUANTITY:
		ol.ol_quantity = val.(*IntValue).intVal
	case OL_AMOUNT:
		ol.ol_amount = val.(*FloatValue).floatVal
	case OL_DIST_INFO:
		sv := val.(*StringValue)
		dest := ol.ol_dist_info[:CAP_DIST]
		copy(dest, sv.stringVal)
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

// ITEM
const (
	I_ID = iota
	I_IM_ID
	I_NAME
	I_PRICE
	I_DATA
)

const (
	CAP_I_NAME = 24
	CAP_I_DATA = 50
)

type ItemTuple struct {
	padding1   [PADDING]byte
	i_id       int64
	i_im_id    int64
	i_name     [CAP_I_NAME]byte
	len_i_name int
	i_price    float64
	i_data     [CAP_I_DATA]byte
	len_i_data int
	padding2   [PADDING]byte
}

func (i *ItemTuple) GetValue(val Value, col int) {
	switch col {
	case I_ID:
		val.(*IntValue).intVal = i.i_id
	case I_IM_ID:
		val.(*IntValue).intVal = i.i_im_id
	case I_NAME:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:i.len_i_name]
		src := i.i_name[:i.len_i_name]
		copy(sv.stringVal, src)
	case I_PRICE:
		val.(*FloatValue).floatVal = i.i_price
	case I_DATA:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:i.len_i_data]
		src := i.i_data[:i.len_i_data]
		copy(sv.stringVal, src)
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

func (i *ItemTuple) SetValue(val Value, col int) {
	switch col {
	case I_ID:
		i.i_id = val.(*IntValue).intVal
	case I_IM_ID:
		i.i_im_id = val.(*IntValue).intVal
	case I_NAME:
		sv := val.(*StringValue)
		i.len_i_name = len(sv.stringVal)
		dest := i.i_name[:i.len_i_name]
		copy(dest, sv.stringVal)
	case I_PRICE:
		i.i_price = val.(*FloatValue).floatVal
	case I_DATA:
		sv := val.(*StringValue)
		i.len_i_data = len(sv.stringVal)
		dest := i.i_data[:i.len_i_data]
		copy(dest, sv.stringVal)
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

// STOCK
const (
	S_I_ID = iota
	S_W_ID
	S_QUANTITY
	S_DIST_01
	S_DIST_02
	S_DIST_03
	S_DIST_04
	S_DIST_05
	S_DIST_06
	S_DIST_07
	S_DIST_08
	S_DIST_09
	S_DIST_10
	S_YTD
	S_ORDER_CNT
	S_REMOTE_CNT
	S_DATA
)

const (
	CAP_S_DATA = 50
)

type StockTuple struct {
	padding1     [PADDING]byte
	s_i_id       int64
	s_w_id       int64
	s_quantity   int64
	s_dist_01    [CAP_DIST]byte
	s_dist_02    [CAP_DIST]byte
	s_dist_03    [CAP_DIST]byte
	s_dist_04    [CAP_DIST]byte
	s_dist_05    [CAP_DIST]byte
	s_dist_06    [CAP_DIST]byte
	s_dist_07    [CAP_DIST]byte
	s_dist_08    [CAP_DIST]byte
	s_dist_09    [CAP_DIST]byte
	s_dist_10    [CAP_DIST]byte
	s_ytd        int64
	s_order_cnt  int64
	s_remote_cnt int64
	s_data       [CAP_S_DATA]byte
	len_s_data   int
	padding2     [PADDING]byte
}

func (s *StockTuple) GetValue(val Value, col int) {
	var str *[CAP_DIST]byte
	switch col {
	case S_I_ID:
		val.(*IntValue).intVal = s.s_i_id
		return
	case S_W_ID:
		val.(*IntValue).intVal = s.s_w_id
		return
	case S_QUANTITY:
		val.(*IntValue).intVal = s.s_quantity
		return
	case S_DIST_01:
		str = &s.s_dist_01
	case S_DIST_02:
		str = &s.s_dist_02
	case S_DIST_03:
		str = &s.s_dist_03
	case S_DIST_04:
		str = &s.s_dist_04
	case S_DIST_05:
		str = &s.s_dist_05
	case S_DIST_06:
		str = &s.s_dist_06
	case S_DIST_07:
		str = &s.s_dist_07
	case S_DIST_08:
		str = &s.s_dist_08
	case S_DIST_09:
		str = &s.s_dist_09
	case S_DIST_10:
		str = &s.s_dist_10
	case S_YTD:
		val.(*IntValue).intVal = s.s_ytd
		return
	case S_ORDER_CNT:
		val.(*IntValue).intVal = s.s_order_cnt
		return
	case S_REMOTE_CNT:
		val.(*IntValue).intVal = s.s_remote_cnt
		return
	case S_DATA:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:s.len_s_data]
		src := s.s_data[:s.len_s_data]
		copy(sv.stringVal, src)
		return
	}

	sv := val.(*StringValue)
	sv.stringVal = sv.stringVal[:CAP_DIST]
	src := (*str)[:CAP_DIST]
	copy(sv.stringVal, src)
}

func (s *StockTuple) SetValue(val Value, col int) {
	var str *[CAP_DIST]byte
	switch col {
	case S_I_ID:
		s.s_i_id = val.(*IntValue).intVal
		return
	case S_W_ID:
		s.s_w_id = val.(*IntValue).intVal
		return
	case S_QUANTITY:
		s.s_quantity = val.(*IntValue).intVal
		return
	case S_DIST_01:
		str = &s.s_dist_01
	case S_DIST_02:
		str = &s.s_dist_02
	case S_DIST_03:
		str = &s.s_dist_03
	case S_DIST_04:
		str = &s.s_dist_04
	case S_DIST_05:
		str = &s.s_dist_05
	case S_DIST_06:
		str = &s.s_dist_06
	case S_DIST_07:
		str = &s.s_dist_07
	case S_DIST_08:
		str = &s.s_dist_08
	case S_DIST_09:
		str = &s.s_dist_09
	case S_DIST_10:
		str = &s.s_dist_10
	case S_YTD:
		s.s_ytd = val.(*IntValue).intVal
		return
	case S_ORDER_CNT:
		s.s_order_cnt = val.(*IntValue).intVal
		return
	case S_REMOTE_CNT:
		s.s_remote_cnt = val.(*IntValue).intVal
		return
	case S_DATA:
		sv := val.(*StringValue)
		s.len_s_data = len(sv.stringVal)
		dest := s.s_data[:s.len_s_data]
		copy(dest, sv.stringVal)
		return
	}

	sv := val.(*StringValue)
	dest := (*str)[:CAP_DIST]
	copy(dest, sv.stringVal)
}
