package testbed

import (
	"bufio"
	"io"
	"math/rand"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/totemtang/cc-testbed/clog"
)

const (
	WAREHOUSE_FILE = "warehouse.txt"
	DISTRICT_FILE  = "district.txt"
	CUSTOMER_FILE  = "customer.txt"
	HISTORY_FILE   = "history.txt"
	NEWORDER_FILE  = "new_orders.txt"
	ORDER_FILE     = "orders.txt"
	ORDERLINE_FILE = "order_line.txt"
	ITEM_FILE      = "item.txt"
	STOCK_FILE     = "stock.txt"
)

var LAST_STRING = [10]string{"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"}

var lastToIndex map[string]int

const (
	MAXOLCNT             = 15
	MINOLCNT             = 5
	TPCCTRANSNUM         = 6
	TPCCTABLENUM         = 9
	C_ID_PER_DIST        = 3000
	C_LAST_PER_DIST      = 1000
	STOCKLEVEL_DISTITEMS = 300
	TPCC_MAXPART         = 2
)

type ParseFunc func(string) (Key, int, Tuple)

type BaseTrans struct {
	accessParts []int
	trial       int
	TXN         int
	req         LockReq
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
	bt.req.tid = tid
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

// Vary Distribution of i_id
type NewOrderTrans struct {
	padding1 [PADDING]byte
	BaseTrans
	w_id            int
	d_id            int
	c_id            int
	o_entry_d       time.Time
	ol_cnt          int
	ol_i_id         [MAXOLCNT]int
	ol_quantity     [MAXOLCNT]int
	ol_supply_w_id  [MAXOLCNT]int
	wb_next_o_id    IntValue
	wb_s_quantity   [MAXOLCNT]IntValue
	wb_s_ytd        [MAXOLCNT]IntValue
	wb_s_order_cnt  [MAXOLCNT]IntValue
	wb_s_remote_cnt [MAXOLCNT]IntValue
	rb_c_credit     StringValue
	rb_i_name       StringValue
	rb_o_dist       StringValue
	noRec           Record
	oRec            Record
	olRec           [MAXOLCNT]Record
	retVal          FloatValue
	padding2        [PADDING]byte
}

// Vary C_ID and C_LAST
type PaymentTrans struct {
	padding1 [PADDING]byte
	BaseTrans
	isLast           bool
	w_id             int
	d_id             int
	c_id             int
	c_last           int
	c_w_id           int
	h_amount         float32
	wb_w_ytd         FloatValue
	wb_d_ytd         FloatValue
	wb_c_balance     FloatValue
	wb_c_ytd_payment FloatValue
	wb_c_payment_cnt IntValue
	wb_c_data        StringValue
	hRec             Record
	padding2         [PADDING]byte
}

// Vary C_ID or C_LAST
type OrderStatusTrans struct {
	padding1 [PADDING]byte
	BaseTrans
	w_id     int
	d_id     int
	isLast   bool
	c_id     int
	c_last   int
	padding2 [PADDING]byte
}

type DeliveryTrans struct {
	padding1 [PADDING]byte
	BaseTrans
	w_id         int
	o_carrier_id int
	padding2     [PADDING]byte
}

// Even Distribution D_ID
type StockLevelTrans struct {
	padding1 [PADDING]byte
	BaseTrans
	w_id      int
	d_id      int
	threshold int
	i_id_ar   []int
	padding2  [PADDING]byte
}

type TPCCTransGen struct {
	padding1        [PADDING]byte
	i_id_gen        KeyGen
	w_id_gen        KeyGen
	c_id_gen        KeyGen
	c_last_gen      KeyGen
	rnd             rand.Rand
	transPercentage [TPCCTRANSNUM]int
	transBuf        [TPCCTRANSNUM][QUEUESIZE]Trans
	head            [TPCCTRANSNUM]int
	tail            [TPCCTRANSNUM]int
	cr              float64
	partIndex       int
	nParts          int
	isPartition     bool
	oa              OrderAllocator
	ola             OrderLineAllocator
	ha              HistoryAllocator
	padding2        [PADDING]byte
}

func (tg *TPCCTransGen) GenOneTrans(mode int) Trans {
	var t Trans
	rnd := &tg.rnd

	txn := rnd.Intn(100)
	for i, v := range tg.transPercentage {
		if txn < v {
			txn = i
			break
		}
	}

	txn = txn + TPCC_BASE + 1

	switch txn {
	case TPCC_NEWORDER:
		t = genNewOrderTrans(tg, txn)
	case TPCC_PAYMENT_ID:
		t = genPaymentTrans(tg, txn, false)
	case TPCC_PAYMENT_LAST:
		t = genPaymentTrans(tg, txn, true)
	case TPCC_ORDERSTATUS_ID:
		t = genOrderStatusTrans(tg, txn, false)
	case TPCC_ORDERSTATUS_LAST:
		t = genOrderStatusTrans(tg, txn, true)
	case TPCC_STOCKLEVEL:
		t = genStockLevelTrans(tg, txn)
	default:
		clog.Error("TPCC does not support transaction %v\n", txn)
	}

	return t

}

func (tg *TPCCTransGen) ReleaseOneTrans(t Trans) {
	txnIndex := t.GetTXN() - TPCC_BASE - 1
	tg.tail[txnIndex] = (tg.tail[txnIndex] + 1) % QUEUESIZE
	tg.transBuf[txnIndex][tg.tail[txnIndex]] = t
}

func genNewOrderTrans(tg *TPCCTransGen, txn int) Trans {
	txnIndex := txn - TPCC_BASE - 1
	t := tg.transBuf[txnIndex][tg.head[txnIndex]].(*NewOrderTrans)
	tg.head[txnIndex] = (tg.head[txnIndex] + 1) % QUEUESIZE

	rnd := &tg.rnd
	i_id_gen := tg.i_id_gen
	w_id_gen := tg.w_id_gen

	pi := tg.partIndex
	isPart := tg.isPartition
	cr := int(tg.cr)

	oa := &tg.oa
	ola := &tg.ola

	t.w_id = pi
	var tmpPi int
	if isPart {
		if rnd.Intn(100) < cr {
			t.accessParts = t.accessParts[:2]
			for {
				tmpPi = int(w_id_gen.GetWholeRank())
				if tmpPi != pi {
					break
				}
			}
			if tmpPi > pi {
				t.accessParts[0] = pi
				t.accessParts[1] = tmpPi
			} else {
				t.accessParts[0] = tmpPi
				t.accessParts[1] = pi
			}
		} else {
			t.accessParts = t.accessParts[:1]
			t.accessParts[0] = pi
		}

	} else {
		t.accessParts = t.accessParts[:1]
		t.accessParts[0] = w_id_gen.GetWholeRank()
		t.w_id = t.accessParts[0]
	}

	t.TXN = txn
	t.d_id = rnd.Intn(DIST_COUNT)
	t.c_id = rnd.Intn(C_ID_PER_DIST)
	t.o_entry_d = time.Now()
	t.ol_cnt = MINOLCNT + rnd.Intn(MAXOLCNT-MINOLCNT)
	t.oRec = oa.genOrderRec()

	j := 0
	for i := 0; i < t.ol_cnt; i++ {
		t.ol_supply_w_id[i] = t.accessParts[j]
		t.ol_i_id[i] = i_id_gen.GetPartRank(t.accessParts[j])
		j = (j + 1) % len(t.accessParts)

		t.ol_quantity[i] = 5
		t.olRec[i] = ola.genOrderLineRec()
	}

	return t

}

func genPaymentTrans(tg *TPCCTransGen, txn int, isLast bool) Trans {
	txnIndex := txn - TPCC_BASE - 1
	t := tg.transBuf[txnIndex][tg.head[txnIndex]].(*PaymentTrans)
	tg.head[txnIndex] = (tg.head[txnIndex] + 1) % QUEUESIZE

	rnd := &tg.rnd
	c_id_gen := tg.c_id_gen
	c_last_gen := tg.c_last_gen
	w_id_gen := tg.w_id_gen

	pi := tg.partIndex
	isPart := tg.isPartition
	cr := int(tg.cr)

	var tmpPi int = pi
	if isPart {
		if rnd.Intn(100) < cr {
			t.accessParts = t.accessParts[:2]
			for {
				tmpPi = w_id_gen.GetWholeRank()
				if tmpPi != pi {
					break
				}
			}
			if tmpPi > pi {
				t.accessParts[0] = pi
				t.accessParts[1] = tmpPi
			} else {
				t.accessParts[0] = tmpPi
				t.accessParts[1] = pi
			}
		} else {
			t.accessParts = t.accessParts[:1]
			t.accessParts[0] = pi
		}
		t.w_id = pi
		t.c_w_id = tmpPi
	} else {
		t.w_id = w_id_gen.GetWholeRank()
		t.c_w_id = w_id_gen.GetWholeRank()
	}

	t.TXN = txn
	t.isLast = isLast

	if isLast {
		dist_cust := c_last_gen.GetWholeRank()
		t.d_id = dist_cust / C_LAST_PER_DIST
		t.c_last = dist_cust % C_LAST_PER_DIST
		//last_id := dist_cust % C_LAST_PER_DIST

		/*
			index := 0
			d := last_id / 100
			last_id = last_id % 100
			t.c_last = t.c_last[:len(LAST_STRING[d])]
			for i := 0; i < len(LAST_STRING[d]); i++ {
				t.c_last[index] = LAST_STRING[d][i]
				index++
			}

			d = last_id / 10
			last_id = last_id % 10
			t.c_last = t.c_last[:index+len(LAST_STRING[d])]
			for i := 0; i < len(LAST_STRING[d]); i++ {
				t.c_last[index] = LAST_STRING[d][i]
				index++
			}

			d = last_id
			t.c_last = t.c_last[:index+len(LAST_STRING[d])]
			for i := 0; i < len(LAST_STRING[d]); i++ {
				t.c_last[index] = LAST_STRING[d][i]
				index++
			}*/
	} else {
		dist_cust := c_id_gen.GetWholeRank()
		t.d_id = dist_cust / C_ID_PER_DIST
		t.c_id = dist_cust % C_ID_PER_DIST
	}

	t.h_amount = float32(rnd.Intn(5000) + 1)
	t.hRec = tg.ha.genHistoryRec()

	return t
}

func genOrderStatusTrans(tg *TPCCTransGen, txn int, isLast bool) Trans {
	txnIndex := txn - TPCC_BASE - 1
	t := tg.transBuf[txnIndex][tg.head[txnIndex]].(*OrderStatusTrans)
	tg.head[txnIndex] = (tg.head[txnIndex] + 1) % QUEUESIZE

	w_id_gen := tg.w_id_gen
	c_id_gen := tg.c_id_gen
	c_last_gen := tg.c_last_gen

	pi := tg.partIndex

	t.accessParts = t.accessParts[:1]
	t.accessParts[0] = pi
	t.TXN = txn
	t.isLast = isLast

	if tg.isPartition {
		t.w_id = pi
	} else {
		t.w_id = w_id_gen.GetWholeRank()
	}

	if isLast {
		dist_cust := c_last_gen.GetWholeRank()
		t.d_id = dist_cust / C_LAST_PER_DIST
		t.c_last = dist_cust % C_LAST_PER_DIST
		//last_id := dist_cust % C_LAST_PER_DIST

		/*
			index := 0
			d := last_id / 100
			last_id = last_id % 100
			t.c_last = t.c_last[:len(LAST_STRING[d])]
			for i := 0; i < len(LAST_STRING[d]); i++ {
				t.c_last[index] = LAST_STRING[d][i]
				index++
			}

			d = last_id / 10
			last_id = last_id % 10
			t.c_last = t.c_last[:index+len(LAST_STRING[d])]
			for i := 0; i < len(LAST_STRING[d]); i++ {
				t.c_last[index] = LAST_STRING[d][i]
				index++
			}

			d = last_id
			t.c_last = t.c_last[:index+len(LAST_STRING[d])]
			for i := 0; i < len(LAST_STRING[d]); i++ {
				t.c_last[index] = LAST_STRING[d][i]
				index++
			}
		*/
	} else {
		dist_cust := c_id_gen.GetWholeRank()
		t.d_id = dist_cust / C_ID_PER_DIST
		t.c_id = dist_cust % C_ID_PER_DIST
	}

	return t
}

func genStockLevelTrans(tg *TPCCTransGen, txn int) Trans {
	txnIndex := txn - TPCC_BASE - 1
	t := tg.transBuf[txnIndex][tg.head[txnIndex]].(*StockLevelTrans)
	tg.head[txnIndex] = (tg.head[txnIndex] + 1) % QUEUESIZE

	w_id_gen := tg.w_id_gen

	rnd := &tg.rnd
	pi := tg.partIndex
	t.accessParts = t.accessParts[:1]
	t.accessParts[0] = pi
	t.TXN = txn
	if tg.isPartition {
		t.w_id = tg.partIndex
	} else {
		t.w_id = w_id_gen.GetWholeRank()
	}
	t.d_id = rnd.Intn(DIST_COUNT)
	t.threshold = 10 + rnd.Intn(11)
	t.i_id_ar = t.i_id_ar[:0]

	return t
}

type TPCCWorkload struct {
	padding1        [PADDING]byte
	transPercentage [TPCCTRANSNUM]int
	transGen        []TPCCTransGen
	w_id_range      int
	kr_w_id         []int
	c_id_range      int
	kr_c_id         []int
	c_last_range    int
	kr_c_last       []int
	i_id_range      int
	kr_i_id         []int
	store           *Store
	nWorkers        int
	nParts          int
	isPartition     bool
	padding2        [PADDING]byte
}

func NewTPCCWL(workload string, nParts int, isPartition bool, nWorkers int, s float64, transPercentage string, cr float64, ps float64, dataDir string) *TPCCWorkload {
	tpccWL := &TPCCWorkload{}

	tp := strings.Split(transPercentage, ":")
	if len(tp) != TPCCTRANSNUM {
		clog.Error("Wrong format of transaction percentage string %s\n", transPercentage)
	}

	for i, str := range tp {
		per, err := strconv.Atoi(str)
		if err != nil {
			clog.Error("TransPercentage Format Error %s\n", str)
		}
		if i != 0 {
			tpccWL.transPercentage[i] = tpccWL.transPercentage[i-1] + per
		} else {
			tpccWL.transPercentage[i] = per
		}
	}

	if tpccWL.transPercentage[TPCCTRANSNUM-1] != 100 {
		clog.Error("Wrong format of transaction percentage string %s; Sum should be 100\n", transPercentage)
	}

	start := time.Now()
	tpccWL.store = NewStore(workload, nParts, isPartition)
	clog.Info("Building Store %.2fs \n", time.Since(start).Seconds())

	tpccWL.nWorkers = nWorkers
	tpccWL.nParts = nParts
	tpccWL.isPartition = isPartition

	tpccWL.w_id_range = *NumPart

	parseFuncAR := [TPCCTABLENUM]ParseFunc{parseWarehouse, parseDistrict, parseCustomer, parseHistory, parseNewOrder, parseOrder, parseOrderLine, parseItem, parseStock}

	dataFiles := [TPCCTABLENUM]string{WAREHOUSE_FILE, DISTRICT_FILE, CUSTOMER_FILE, HISTORY_FILE, NEWORDER_FILE, ORDER_FILE, ORDERLINE_FILE, ITEM_FILE, STOCK_FILE}

	start = time.Now()

	var rowBytes []byte
	var k Key
	var partNum int
	var tuple Tuple
	store := tpccWL.store
	// First Load Item in a single thread
	df, err := os.OpenFile(dataDir+"/"+ITEM_FILE, os.O_RDONLY, 0600)
	reader := bufio.NewReader(df)
	for {
		rowBytes, _, err = reader.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			clog.Error("Reading File %s Error", ITEM_FILE)
			df.Close()
			return nil
		}
		k, partNum, tuple = parseItem(string(rowBytes))
		store.CreateRecByID(ITEM, k, partNum, tuple)

		tpccWL.i_id_range++
	}
	df.Close()

	// Load other data using Parallel threading
	var wg sync.WaitGroup
	for j := 0; j < *NumPart; j++ {
		wg.Add(1)
		go func(n int) {
			var rowBytes []byte
			var k Key
			var partNum int
			var tuple Tuple
			//iRecs := make([]InsertRec, 1)
			//noTuple := &NewOrderTuple{}
			//noRec := MakeRecord(store.tables[NEWORDER], k, noTuple)
			for i := 0; i < TPCCTABLENUM; i++ {
				if i == ITEM {
					continue
				}
				parseFunc := parseFuncAR[i]
				file := "shard-" + strconv.Itoa(n) + "/" + dataFiles[i]
				df, err := os.OpenFile(dataDir+"/"+file, os.O_RDONLY, 0600)
				reader := bufio.NewReader(df)
				for {
					rowBytes, _, err = reader.ReadLine()
					if err == io.EOF {
						break
					} else if err != nil {
						clog.Error("Reading File %s Error", file)
						df.Close()
						break
					}
					k, partNum, tuple = parseFunc(string(rowBytes))
					//iRecs[0].k = k
					//iRecs[0].partNum = partNum
					/*if i == NEWORDER {
						noRec.SetTuple(tuple)
						iRecs[0].rec = noRec
						store.tables[i].InsertRecord(iRecs)
					} else if i == CUSTOMER {
						store.tables[i].CreateRecByID(k, partNum, tuple)
					} else {
						rec := MakeRecord(store.tables[i], k, tuple)
						iRecs[0].rec = rec
						store.tables[i].InsertRecord(iRecs)
					}*/
					store.tables[i].CreateRecByID(k, partNum, tuple)
				}
				df.Close()
			}
			wg.Done()
		}(j)
	}

	wg.Wait()

	clog.Info("Loading Data %.2fs \n", time.Since(start).Seconds())

	/*var tmpKey Key
	tmpKey[4] = 31
	tmpKey[5] = 51
	_, err = store.tables[STOCK].GetRecByID(tmpKey, 0)
	if err != nil {
		clog.Info("No Key")
	}*/

	lastToIndex = make(map[string]int)
	for i, str := range LAST_STRING {
		lastToIndex[str] = i
	}

	tpccWL.c_id_range = DIST_COUNT * C_ID_PER_DIST
	tpccWL.c_last_range = DIST_COUNT * C_LAST_PER_DIST

	kr_i_id := make([]int, nParts+PADDINGINT*2)
	kr_i_id = kr_i_id[PADDINGINT : PADDINGINT+nParts]
	kr_w_id := make([]int, nParts+PADDINGINT*2)
	kr_w_id = kr_w_id[PADDINGINT : PADDINGINT+nParts]
	kr_c_id := make([]int, nParts+PADDINGINT*2)
	kr_c_id = kr_c_id[PADDINGINT : PADDINGINT+nParts]
	kr_c_last := make([]int, nParts+PADDINGINT*2)
	kr_c_last = kr_c_last[PADDINGINT : PADDINGINT+nParts]
	for i := 0; i < nParts; i++ {
		kr_i_id[i] = tpccWL.i_id_range
		kr_w_id[i] = tpccWL.w_id_range
		kr_c_id[i] = tpccWL.c_id_range
		kr_c_last[i] = tpccWL.c_last_range
	}

	tpccWL.kr_w_id = kr_w_id
	tpccWL.kr_c_id = kr_c_id
	tpccWL.kr_c_last = kr_c_last
	tpccWL.kr_i_id = kr_i_id

	start = time.Now()

	tpccWL.transGen = make([]TPCCTransGen, nWorkers)
	for i := 0; i < nWorkers; i++ {
		tg := &tpccWL.transGen[i]
		tg.rnd = *rand.New(rand.NewSource(time.Now().UnixNano() % int64(i+1)))
		tg.transPercentage = tpccWL.transPercentage
		for j := 0; j < QUEUESIZE; j++ {
			// PreAllocate NewOrder for each Transaction
			var noRec Record
			noTuple := &NewOrderTuple{}
			if *SysType == LOCKING {
				tmpRec := &LRecord{}
				tmpRec.wdLock.Initialize()
				noRec = tmpRec
			} else if *SysType == OCC {
				noRec = &ORecord{}
			} else if *SysType == PARTITION {
				noRec = &PRecord{}
			} else if *SysType == ADAPTIVE {
				tmpRec := &ARecord{}
				tmpRec.wdLock.Initialize()
				noRec = tmpRec
			} else {
				clog.Info("System Type %v Not Support", *SysType)
			}
			noRec.SetTuple(noTuple)
			tg.transBuf[TPCC_NEWORDER-TPCC_NEWORDER][j] = makeNewOrderTrans(tg.rnd, i, noRec)
			tg.transBuf[TPCC_PAYMENT_ID-TPCC_NEWORDER][j] = makePaymentTrans(tg.rnd, i)
			tg.transBuf[TPCC_PAYMENT_LAST-TPCC_NEWORDER][j] = makePaymentTrans(tg.rnd, i)
			tg.transBuf[TPCC_ORDERSTATUS_ID-TPCC_NEWORDER][j] = makeOrderStatusTrans(tg.rnd, i)
			tg.transBuf[TPCC_ORDERSTATUS_LAST-TPCC_NEWORDER][j] = makeOrderStatusTrans(tg.rnd, i)
			tg.transBuf[TPCC_STOCKLEVEL-TPCC_NEWORDER][j] = makeStockLevelTrans(tg.rnd, i)
		}

		tg.tail[TPCC_NEWORDER-TPCC_NEWORDER] = -1
		tg.tail[TPCC_PAYMENT_ID-TPCC_NEWORDER] = -1
		tg.tail[TPCC_PAYMENT_LAST-TPCC_NEWORDER] = -1
		tg.tail[TPCC_ORDERSTATUS_ID-TPCC_NEWORDER] = -1
		tg.tail[TPCC_ORDERSTATUS_LAST-TPCC_NEWORDER] = -1
		tg.tail[TPCC_STOCKLEVEL-TPCC_NEWORDER] = -1

		tg.cr = cr
		tg.partIndex = i
		tg.nParts = nParts
		tg.isPartition = isPartition

		tg.i_id_gen = tpcc_NewKeyGen(s, i, tpccWL.i_id_range, nParts, kr_i_id, isPartition)
		tg.c_id_gen = tpcc_NewKeyGen(s, i, tpccWL.c_id_range, nParts, kr_c_id, isPartition)
		tg.c_last_gen = tpcc_NewKeyGen(s, i, tpccWL.c_last_range, nParts, kr_c_last, isPartition)
		tg.w_id_gen = tpcc_NewKeyGen(ps, i, tpccWL.w_id_range, nParts, kr_w_id, isPartition)

		tg.oa.OneAllocate()
		tg.ola.OneAllocate()
		tg.ha.OneAllocate()

	}

	clog.Info("Generating Trans Pool %.2fs", time.Since(start).Seconds())

	// Loading data into another store
	/*start = time.Now()
	tmpStore := NewStore(workload, nParts, isPartition)

	for i, table := range store.tables {
		table.BulkLoad(tmpStore.tables[i])
	}
	clog.Info("Bulkloading Another Store %.2fs", time.Since(start).Seconds())*/

	return tpccWL

}

func (tpccWL *TPCCWorkload) GetStore() *Store {
	return tpccWL.store
}

func (tpccWL *TPCCWorkload) GetTableCount() int {
	return len(tpccWL.store.tables)
}

func (tpccWL *TPCCWorkload) GetTransGen(partIndex int) TransGen {
	if partIndex >= len(tpccWL.transGen) {
		clog.Error("Part Index %v Out of Range %v for TransGen\n", partIndex, len(tpccWL.transGen))
	}
	return &tpccWL.transGen[partIndex]
}

func (tpccWL *TPCCWorkload) GetKeyGens() [][]KeyGen {
	keygens := make([][]KeyGen, tpccWL.nWorkers)
	for i := 0; i < tpccWL.nWorkers; i++ {
		keygens[i] = make([]KeyGen, 3)
		keygens[i][0] = tpccWL.transGen[i].c_id_gen
		keygens[i][1] = tpccWL.transGen[i].c_last_gen
		keygens[i][2] = tpccWL.transGen[i].i_id_gen
	}
	return keygens
}

func (tpccWL *TPCCWorkload) SetKeyGens(keygens [][]KeyGen) {
	for i := 0; i < tpccWL.nWorkers; i++ {
		tpccWL.transGen[i].c_id_gen = keygens[i][0]
		tpccWL.transGen[i].c_last_gen = keygens[i][1]
		tpccWL.transGen[i].i_id_gen = keygens[i][2]
	}
}

func (tpccWL *TPCCWorkload) NewKeyGen(s float64) [][]KeyGen {
	keygens := make([][]KeyGen, tpccWL.nWorkers)
	for i := 0; i < tpccWL.nWorkers; i++ {
		keygens[i] = make([]KeyGen, 3)
		keygens[i][0] = tpcc_NewKeyGen(s, i, tpccWL.c_id_range, tpccWL.nParts, tpccWL.kr_c_id, tpccWL.isPartition)
		keygens[i][1] = tpcc_NewKeyGen(s, i, tpccWL.c_last_range, tpccWL.nParts, tpccWL.kr_c_last, tpccWL.isPartition)
		keygens[i][2] = tpcc_NewKeyGen(s, i, tpccWL.i_id_range, tpccWL.nParts, tpccWL.kr_i_id, tpccWL.isPartition)
	}

	return keygens
}

func (tpccWL *TPCCWorkload) GetPartGens() []KeyGen {
	keygens := make([]KeyGen, tpccWL.nWorkers)
	for i := 0; i < tpccWL.nWorkers; i++ {
		keygens[i] = tpccWL.transGen[i].w_id_gen
	}
	return keygens
}

func (tpccWL *TPCCWorkload) SetPartGens(keygens []KeyGen) {
	for i := 0; i < tpccWL.nWorkers; i++ {
		tpccWL.transGen[i].w_id_gen = keygens[i]
	}
}

func (tpccWL *TPCCWorkload) NewPartGen(ps float64) []KeyGen {
	keygens := make([]KeyGen, tpccWL.nWorkers)
	for i := 0; i < tpccWL.nWorkers; i++ {
		keygens[i] = tpcc_NewKeyGen(ps, i, tpccWL.w_id_range, tpccWL.nParts, tpccWL.kr_w_id, tpccWL.isPartition)
	}
	return keygens
}

func (tpccWL *TPCCWorkload) ResetConf(transPercentage string, cr float64, coord *Coordinator) {
	tp := strings.Split(transPercentage, ":")
	if len(tp) != TPCCTRANSNUM {
		clog.Error("Wrong format of transaction percentage string %s\n", transPercentage)
	}
	for i, str := range tp {
		per, err := strconv.Atoi(str)
		if err != nil {
			clog.Error("TransPercentage Format Error %s\n", str)
		}
		if i != 0 {
			tpccWL.transPercentage[i] = tpccWL.transPercentage[i-1] + per
		} else {
			tpccWL.transPercentage[i] = per
		}
	}

	if tpccWL.transPercentage[TPCCTRANSNUM-1] != 100 {
		clog.Error("Wrong format of transaction percentage string %s; Sum should be 100\n", transPercentage)
	}

	for i := 0; i < len(tpccWL.transGen); i++ {
		tg := &tpccWL.transGen[i]
		tg.transPercentage = tpccWL.transPercentage
		tg.cr = cr
		for j := 0; j < len(tg.head); j++ {
			tg.head[j] = 0
			tg.tail[j] = -1
		}
		tg.oa.Reset()
		tg.ola.Reset()
		tg.ha.Reset()
	}

	for _, table := range tpccWL.store.tables {
		table.Reset()
	}

	for _, w := range coord.Workers {
		w.iaAR[ORDER].Reset()
		w.iaAR[ORDERLINE].Reset()
		w.iaAR[HISTORY].Reset()
		w.iaAR[NEWORDER].Reset()
	}

	debug.SetGCPercent(1)
	debug.FreeOSMemory()
	debug.SetGCPercent(-1)

}

func makeNewOrderTrans(rnd rand.Rand, id int, rec Record) *NewOrderTrans {
	trans := &NewOrderTrans{
		BaseTrans: BaseTrans{
			rnd: rnd,
		},
		noRec: rec,
	}
	trans.accessParts = make([]int, TPCC_MAXPART+PADDINGINT*2)
	trans.accessParts = trans.accessParts[PADDINGINT : PADDINGINT+TPCC_MAXPART]
	trans.req.id = id

	//trans.rb_c_last.stringVal = make([]byte, CAP_C_LAST+2*PADDINGBYTE)
	//trans.rb_c_last.stringVal = trans.rb_c_last.stringVal[PADDINGBYTE : PADDINGBYTE+CAP_C_LAST]
	trans.rb_c_credit.stringVal = make([]byte, CAP_C_CREDIT+2*PADDINGBYTE)
	trans.rb_c_credit.stringVal = trans.rb_c_credit.stringVal[PADDINGBYTE : PADDINGBYTE+CAP_C_CREDIT]
	trans.rb_i_name.stringVal = make([]byte, CAP_I_NAME+2*PADDINGBYTE)
	trans.rb_i_name.stringVal = trans.rb_i_name.stringVal[PADDINGBYTE : PADDINGBYTE+CAP_I_NAME]
	trans.rb_o_dist.stringVal = make([]byte, CAP_DIST+2*PADDINGBYTE)
	trans.rb_o_dist.stringVal = trans.rb_o_dist.stringVal[PADDINGBYTE : PADDINGBYTE+CAP_DIST]

	return trans
}

func makePaymentTrans(rnd rand.Rand, id int) *PaymentTrans {
	trans := &PaymentTrans{
		BaseTrans: BaseTrans{
			rnd: rnd,
		},
	}
	trans.accessParts = make([]int, TPCC_MAXPART+PADDINGINT*2)
	trans.accessParts = trans.accessParts[PADDINGINT : PADDINGINT+TPCC_MAXPART]
	trans.req.id = id

	//trans.c_last = make([]byte, CAP_C_LAST+2*PADDINGBYTE)
	//trans.c_last = trans.c_last[PADDINGBYTE : PADDINGBYTE+CAP_C_LAST]
	trans.wb_c_data.stringVal = make([]byte, CAP_C_DATA+2*PADDINGBYTE)
	trans.wb_c_data.stringVal = trans.wb_c_data.stringVal[PADDINGBYTE : PADDINGBYTE+CAP_C_DATA]

	return trans
}

func makeOrderStatusTrans(rnd rand.Rand, id int) *OrderStatusTrans {
	trans := &OrderStatusTrans{
		BaseTrans: BaseTrans{
			rnd: rnd,
		},
	}
	trans.accessParts = make([]int, TPCC_MAXPART+PADDINGINT*2)
	trans.accessParts = trans.accessParts[PADDINGINT : PADDINGINT+TPCC_MAXPART]
	trans.req.id = id
	//trans.c_last = make([]byte, CAP_C_LAST+2*PADDINGBYTE)
	//trans.c_last = trans.c_last[PADDINGBYTE : PADDINGBYTE+CAP_C_LAST]
	return trans
}

func makeStockLevelTrans(rnd rand.Rand, id int) *StockLevelTrans {
	trans := &StockLevelTrans{
		BaseTrans: BaseTrans{
			rnd: rnd,
		},
	}
	trans.accessParts = make([]int, TPCC_MAXPART+PADDINGINT*2)
	trans.accessParts = trans.accessParts[PADDINGINT : PADDINGINT+TPCC_MAXPART]
	trans.i_id_ar = make([]int, STOCKLEVEL_DISTITEMS+2*PADDINGINT)
	trans.i_id_ar = trans.i_id_ar[PADDINGINT : PADDINGINT+STOCKLEVEL_DISTITEMS]
	trans.req.id = id
	return trans
}

func tpcc_NewKeyGen(s float64, partIndex int, keyRange int, nParts int, keyArray []int, isPartition bool) KeyGen {
	var kg KeyGen
	if s == 1 {
		kg = NewUniformRand(partIndex, keyRange, nParts, keyArray, isPartition)
	} else if s > 1 {
		kg = NewZipfRandLarge(partIndex, keyRange, nParts, keyArray, s, isPartition)
	} else if s < 0 {
		kg = NewHotColdRand(partIndex, keyRange, nParts, keyArray, -s, isPartition)
	} else { // s >=0 && s < 1
		kg = NewZipfRandSmall(partIndex, keyRange, nParts, keyArray, s, isPartition)
	}
	return kg
}

func parseInt(column string) int {
	ret, _ := strconv.ParseInt(column, 10, 32)
	return int(ret)
}

func parseFloat(column string) float32 {
	ret, _ := strconv.ParseFloat(column, 32)
	return float32(ret)
}

func parseDate(column string) time.Time {
	splits := strings.Split(column, " ")

	date := strings.Split(splits[0], "-")
	year, _ := strconv.Atoi(date[0])
	month, _ := strconv.Atoi(date[1])
	day, _ := strconv.Atoi(date[2])

	dayTime := strings.Split(splits[1], ":")
	hour, _ := strconv.Atoi(dayTime[0])
	minute, _ := strconv.Atoi(dayTime[1])
	second, _ := strconv.Atoi(dayTime[2])

	return time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
}

func parseWarehouse(row string) (Key, int, Tuple) {
	var k Key
	var partNum int
	tuple := &WarehouseTuple{}
	columns := strings.Split(row, "\t")

	tuple.w_id = parseInt(columns[W_ID])

	tuple.len_w_name = len(columns[W_NAME])
	w_name := tuple.w_name[:tuple.len_w_name]
	copy(w_name, []byte(columns[W_NAME]))

	tuple.len_w_street_1 = len(columns[W_STREET_1])
	w_street_1 := tuple.w_street_1[:tuple.len_w_street_1]
	copy(w_street_1, []byte(columns[W_STREET_1]))

	tuple.len_w_street_2 = len(columns[W_STREET_2])
	w_street_2 := tuple.w_street_2[:tuple.len_w_street_2]
	copy(w_street_2, []byte(columns[W_STREET_2]))

	tuple.len_w_city = len(columns[W_CITY])
	w_city := tuple.w_city[:tuple.len_w_city]
	copy(w_city, []byte(columns[W_CITY]))

	w_state := tuple.w_state[:CAP_STATE]
	copy(w_state, []byte(columns[W_STATE]))

	w_zip := tuple.w_zip[:CAP_ZIP]
	copy(w_zip, []byte(columns[W_ZIP]))

	tuple.w_tax = parseFloat(columns[W_TAX])
	tuple.w_ytd = parseFloat(columns[W_YTD])

	// Build Key
	k[0] = tuple.w_id

	partNum = int(tuple.w_id)

	return k, partNum, tuple

}

func parseDistrict(row string) (Key, int, Tuple) {
	var k Key
	var partNum int
	tuple := &DistrictTuple{}
	columns := strings.Split(row, "\t")

	tuple.d_id = parseInt(columns[D_ID])
	tuple.d_w_id = parseInt(columns[D_W_ID])

	tuple.len_d_name = len(columns[D_NAME])
	d_name := tuple.d_name[:tuple.len_d_name]
	copy(d_name, []byte(columns[D_NAME]))

	tuple.len_d_street_1 = len(columns[D_STREET_1])
	d_street_1 := tuple.d_street_1[:tuple.len_d_street_1]
	copy(d_street_1, []byte(columns[D_STREET_1]))

	tuple.len_d_street_2 = len(columns[D_STREET_2])
	d_street_2 := tuple.d_street_2[:tuple.len_d_street_2]
	copy(d_street_2, []byte(columns[D_STREET_2]))

	tuple.len_d_city = len(columns[D_CITY])
	d_city := tuple.d_city[:tuple.len_d_city]
	copy(d_city, []byte(columns[D_CITY]))

	d_state := tuple.d_state[:CAP_STATE]
	copy(d_state, []byte(columns[D_STATE]))

	d_zip := tuple.d_zip[:CAP_ZIP]
	copy(d_zip, []byte(columns[D_ZIP]))

	tuple.d_tax = parseFloat(columns[D_TAX])
	tuple.d_ytd = parseFloat(columns[D_YTD])
	tuple.d_next_o_id = parseInt(columns[D_NEXT_O_ID])

	k[0] = tuple.d_w_id
	k[1] = tuple.d_id

	partNum = int(tuple.d_w_id)

	return k, partNum, tuple
}

func parseCustomer(row string) (Key, int, Tuple) {
	var k Key
	var partNum int
	tuple := &CustomerTuple{}
	columns := strings.Split(row, "\t")

	tuple.c_id = parseInt(columns[C_ID])
	tuple.c_d_id = parseInt(columns[C_D_ID])
	tuple.c_w_id = parseInt(columns[C_W_ID])

	tuple.len_c_first = len(columns[C_FIRST])
	c_first := tuple.c_first[:tuple.len_c_first]
	copy(c_first, []byte(columns[C_FIRST]))

	c_middle := tuple.c_middle[:CAP_C_MIDDLE]
	copy(c_middle, []byte(columns[C_MIDDLE]))

	//tuple.len_c_last = len(columns[C_LAST])
	//c_last := tuple.c_last[:tuple.len_c_last]
	//copy(c_last, []byte(columns[C_LAST]))
	var c_last int = 0
	lastStr := columns[C_LAST]
	for p := 0; p < 3; p++ {
		var i int
		for i = 0; i < len(LAST_STRING); i++ {
			if strings.HasPrefix(lastStr, LAST_STRING[i]) {
				c_last = c_last*10 + i
				break
			}
		}
		lastStr = strings.TrimPrefix(lastStr, LAST_STRING[i])
	}
	tuple.c_last = c_last

	tuple.len_c_street_1 = len(columns[C_STREET_1])
	c_street_1 := tuple.c_street_1[:tuple.len_c_street_1]
	copy(c_street_1, []byte(columns[C_STREET_1]))

	tuple.len_c_street_2 = len(columns[C_STREET_2])
	c_street_2 := tuple.c_street_2[:tuple.len_c_street_2]
	copy(c_street_2, []byte(columns[C_STREET_2]))

	tuple.len_c_city = len(columns[C_CITY])
	c_city := tuple.c_city[:tuple.len_c_city]
	copy(c_city, []byte(columns[C_CITY]))

	c_state := tuple.c_state[:CAP_STATE]
	copy(c_state, []byte(columns[C_STATE]))

	c_zip := tuple.c_zip[:CAP_ZIP]
	copy(c_zip, []byte(columns[C_ZIP]))

	c_phone := tuple.c_phone[:CAP_C_PHONE]
	copy(c_phone, []byte(columns[C_PHONE]))

	tuple.c_since = parseDate(columns[C_SINCE])

	c_credit := tuple.c_credit[:CAP_C_CREDIT]
	copy(c_credit, []byte(columns[C_CREDIT]))

	tuple.c_credit_lim = parseFloat(columns[C_CREDIT_LIM])
	tuple.c_discount = parseFloat(columns[C_DISCOUNT])
	tuple.c_balance = parseFloat(columns[C_BALANCE])
	tuple.c_ytd_payment = parseFloat(columns[C_YTD_PAYMENT])
	tuple.c_payment_cnt = parseInt(columns[C_PAYMENT_CNT])
	tuple.c_delivery_cnt = parseInt(columns[C_DELIVERY_CNT])

	tuple.len_c_data = len(columns[C_DATA])
	c_data := tuple.c_data[:tuple.len_c_data]
	copy(c_data, []byte(columns[C_DATA]))

	k[0] = tuple.c_w_id
	k[1] = tuple.c_d_id
	k[2] = tuple.c_id

	partNum = int(tuple.c_w_id)

	return k, partNum, tuple
}

func parseHistory(row string) (Key, int, Tuple) {
	var k Key
	var partNum int = 0
	tuple := &HistoryTuple{}
	columns := strings.Split(row, "\t")

	tuple.h_c_id = parseInt(columns[H_C_ID])
	tuple.h_c_d_id = parseInt(columns[H_C_D_ID])
	tuple.h_c_w_id = parseInt(columns[H_C_W_ID])
	tuple.h_d_id = parseInt(columns[H_D_ID])
	tuple.h_w_id = parseInt(columns[H_W_ID])
	tuple.h_date = parseDate(columns[H_DATE])
	tuple.h_amount = parseFloat(columns[H_AMOUNT])

	tuple.len_h_data = len(columns[H_DATA])
	h_data := tuple.h_data[:tuple.len_h_data]
	copy(h_data, []byte(columns[H_DATA]))

	k[0] = tuple.h_w_id
	k[1] = tuple.h_d_id
	k[2] = tuple.h_c_id

	return k, partNum, tuple
}

func parseNewOrder(row string) (Key, int, Tuple) {
	var k Key
	var partNum int = 0
	tuple := &NewOrderTuple{}
	columns := strings.Split(row, "\t")

	tuple.no_o_id = parseInt(columns[NO_O_ID])
	tuple.no_d_id = parseInt(columns[NO_D_ID])
	tuple.no_w_id = parseInt(columns[NO_W_ID])

	k[0] = tuple.no_w_id
	k[1] = tuple.no_d_id

	partNum = int(tuple.no_w_id)

	return k, partNum, tuple
}

func parseOrder(row string) (Key, int, Tuple) {
	var k Key
	var partNum int = 0
	tuple := &OrderTuple{}
	columns := strings.Split(row, "\t")

	tuple.o_id = parseInt(columns[O_ID])
	tuple.o_d_id = parseInt(columns[O_D_ID])
	tuple.o_w_id = parseInt(columns[O_W_ID])
	tuple.o_c_id = parseInt(columns[O_C_ID])
	tuple.o_entry_d = parseDate(columns[O_ENTRY_D])
	if strings.Compare(columns[O_CARRIER_ID], "\\N") == 0 {
		tuple.o_carrier_id = -1
	} else {
		tuple.o_carrier_id = parseInt(columns[O_CARRIER_ID])
	}
	tuple.o_ol_cnt = parseInt(columns[O_OL_CNT])
	tuple.o_all_local = parseInt(columns[O_ALL_LOCAL])

	k[0] = tuple.o_w_id
	k[1] = tuple.o_d_id
	k[2] = tuple.o_id

	partNum = int(tuple.o_w_id)

	return k, partNum, tuple

}

func parseOrderLine(row string) (Key, int, Tuple) {
	var k Key
	var partNum int = 0
	tuple := &OrderLineTuple{}
	columns := strings.Split(row, "\t")

	tuple.ol_o_id = parseInt(columns[OL_O_ID])
	tuple.ol_d_id = parseInt(columns[OL_D_ID])
	tuple.ol_w_id = parseInt(columns[OL_W_ID])
	tuple.ol_number = parseInt(columns[OL_NUMBER])
	tuple.ol_i_id = parseInt(columns[OL_I_ID])
	tuple.ol_supply_w_id = parseInt(columns[OL_SUPPLY_W_ID])
	if strings.Compare(columns[OL_DELIVERY_D], "\\N") != 0 {
		tuple.ol_delivery_d = parseDate(columns[OL_DELIVERY_D])
	}
	tuple.ol_quantity = parseInt(columns[OL_QUANTITY])
	tuple.ol_amount = parseFloat(columns[OL_AMOUNT])

	ol_dist_info := tuple.ol_dist_info[:CAP_DIST]
	copy(ol_dist_info, []byte(columns[OL_DIST_INFO]))

	k[0] = tuple.ol_w_id
	k[1] = tuple.ol_d_id
	k[2] = tuple.ol_o_id
	k[3] = tuple.ol_number

	partNum = int(tuple.ol_w_id)

	return k, partNum, tuple
}

func parseItem(row string) (Key, int, Tuple) {
	var k Key
	var partNum int = 0
	tuple := &ItemTuple{}
	columns := strings.Split(row, "\t")

	tuple.i_id = parseInt(columns[I_ID])
	tuple.i_im_id = parseInt(columns[I_IM_ID])

	tuple.len_i_name = len(columns[I_NAME])
	i_name := tuple.i_name[:tuple.len_i_name]
	copy(i_name, []byte(columns[I_NAME]))

	tuple.i_price = parseFloat(columns[I_PRICE])

	tuple.len_i_data = len(columns[I_DATA])
	i_data := tuple.i_data[:tuple.len_i_data]
	copy(i_data, []byte(columns[I_DATA]))

	k[0] = tuple.i_id

	return k, partNum, tuple

}

func parseStock(row string) (Key, int, Tuple) {
	var k Key
	var partNum int = 0
	tuple := &StockTuple{}
	columns := strings.Split(row, "\t")

	tuple.s_i_id = parseInt(columns[S_I_ID])
	tuple.s_w_id = parseInt(columns[S_W_ID])
	tuple.s_quantity = parseInt(columns[S_QUANTITY])

	s_dist_01 := tuple.s_dist_01[:CAP_DIST]
	copy(s_dist_01, []byte(columns[S_DIST_01]))

	s_dist_02 := tuple.s_dist_02[:CAP_DIST]
	copy(s_dist_02, []byte(columns[S_DIST_02]))

	s_dist_03 := tuple.s_dist_03[:CAP_DIST]
	copy(s_dist_03, []byte(columns[S_DIST_03]))

	s_dist_04 := tuple.s_dist_04[:CAP_DIST]
	copy(s_dist_04, []byte(columns[S_DIST_04]))

	s_dist_05 := tuple.s_dist_05[:CAP_DIST]
	copy(s_dist_05, []byte(columns[S_DIST_05]))

	s_dist_06 := tuple.s_dist_06[:CAP_DIST]
	copy(s_dist_06, []byte(columns[S_DIST_06]))

	s_dist_07 := tuple.s_dist_07[:CAP_DIST]
	copy(s_dist_07, []byte(columns[S_DIST_07]))

	s_dist_08 := tuple.s_dist_08[:CAP_DIST]
	copy(s_dist_08, []byte(columns[S_DIST_08]))

	s_dist_09 := tuple.s_dist_09[:CAP_DIST]
	copy(s_dist_09, []byte(columns[S_DIST_09]))

	s_dist_10 := tuple.s_dist_10[:CAP_DIST]
	copy(s_dist_10, []byte(columns[S_DIST_10]))

	tuple.s_ytd = parseInt(columns[S_YTD])
	tuple.s_order_cnt = parseInt(columns[S_ORDER_CNT])
	tuple.s_remote_cnt = parseInt(columns[S_REMOTE_CNT])

	tuple.len_s_data = len(columns[S_DATA])
	s_data := tuple.s_data[:tuple.len_s_data]
	copy(s_data, []byte(columns[S_DATA]))

	k[0] = tuple.s_w_id
	k[1] = tuple.s_i_id

	partNum = int(tuple.s_w_id)

	return k, partNum, tuple

}
