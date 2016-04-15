package testbed

import (
	//"time"

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

	SINGLEBASE = iota
	ADDONE
	UPDATEINT

	LAST_TXN
)

// Request Type
const (
	LOCK_NONE = iota
	LOCK_SH
	LOCK_EX
)

// State
const (
	LOCK_OK = iota
	LOCK_WAIT
	LOCK_ABORT
)

type LockReq struct {
	padding1 [PADDING]byte
	id       int
	tid      TID
	reqType  int
	state    chan int
	padding2 [PADDING]byte
}

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

	req := sbTrnas.req
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
	_, _, err = exec.ReadValue(ACCOUNTS, acctId0, part0, intRB, ACCT_ID, req)
	if err != nil {
		return nil, err
	}

	_, _, err = exec.ReadValue(ACCOUNTS, acctId1, part1, intRB, ACCT_ID, req)
	if err != nil {
		return nil, err
	}

	err = exec.MayWrite(SAVINGS, acctId0, part0, req)
	if err != nil {
		return nil, err
	}

	err = exec.MayWrite(CHECKING, acctId1, part1, req)
	if err != nil {
		return nil, err
	}

	val, _, err = exec.ReadValue(SAVINGS, acctId0, part0, floatRB, SAVING_BAL, req)
	if err != nil {
		return nil, err
	}
	//sum := val.(*FloatValue).floatVal
	fv0.floatVal = val.(*FloatValue).floatVal

	val, _, err = exec.ReadValue(CHECKING, acctId1, part1, floatRB, CHECK_BAL, req)
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

	err = exec.WriteValue(CHECKING, acctId1, part1, fv0, CHECK_BAL, req)
	if err != nil {
		return nil, err
	}

	err = exec.WriteValue(SAVINGS, acctId0, part0, fv1, SAVING_BAL, req)
	if err != nil {
		return nil, err
	}

	if exec.Commit(req) == 0 {
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

	req := sbTrnas.req
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
	_, _, err = exec.ReadValue(ACCOUNTS, send, part0, intRB, ACCT_ID, req)
	if err != nil {
		return nil, err
	}

	_, _, err = exec.ReadValue(ACCOUNTS, dest, part1, intRB, ACCT_ID, req)
	if err != nil {
		return nil, err
	}

	err = exec.MayWrite(CHECKING, send, part0, req)
	if err != nil {
		return nil, err
	}

	err = exec.MayWrite(CHECKING, dest, part1, req)
	if err != nil {
		return nil, err
	}

	val, _, err = exec.ReadValue(CHECKING, send, part0, floatRB, CHECK_BAL, req)
	if err != nil {
		return nil, err
	}
	bal := val.(*FloatValue).floatVal

	if bal < ammt.floatVal {
		exec.Abort(req)
		return nil, ELACKBALANCE
	}

	fv0.floatVal = bal - ammt.floatVal

	val, _, err = exec.ReadValue(CHECKING, dest, part1, floatRB, CHECK_BAL, req)
	if err != nil {
		return nil, err
	}
	fv1.floatVal = val.(*FloatValue).floatVal + ammt.floatVal

	err = exec.WriteValue(CHECKING, send, part0, fv0, CHECK_BAL, req)
	if err != nil {
		return nil, err
	}

	err = exec.WriteValue(CHECKING, dest, part1, fv1, CHECK_BAL, req)
	if err != nil {
		return nil, err
	}

	if exec.Commit(req) == 0 {
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

	req := sbTrnas.req
	ret := &sbTrnas.ret
	part := sbTrnas.accessParts[0]
	acct := sbTrnas.accoutID[0]

	var val Value
	var err error
	_, _, err = exec.ReadValue(ACCOUNTS, acct, part, intRB, ACCT_ID, req)
	if err != nil {
		return nil, err
	}

	val, _, err = exec.ReadValue(CHECKING, acct, part, floatRB, CHECK_BAL, req)
	if err != nil {
		return nil, err
	}
	ret.floatVal = val.(*FloatValue).floatVal

	val, _, err = exec.ReadValue(SAVINGS, acct, part, floatRB, SAVING_BAL, req)
	if err != nil {
		return nil, err
	}

	ret.floatVal += val.(*FloatValue).floatVal

	if exec.Commit(req) == 0 {
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

	req := sbTrnas.req
	part := sbTrnas.accessParts[0]
	acct := sbTrnas.accoutID[0]
	ammt := &sbTrnas.ammount
	fv0 := &sbTrnas.fv[0]

	var val Value
	var err error
	_, _, err = exec.ReadValue(ACCOUNTS, acct, part, intRB, ACCT_ID, req)
	if err != nil {
		return nil, err
	}

	val, _, err = exec.ReadValue(CHECKING, acct, part, floatRB, CHECK_BAL, req)
	if err != nil {
		return nil, err
	}
	checkBal := val.(*FloatValue).floatVal
	sum := checkBal

	val, _, err = exec.ReadValue(SAVINGS, acct, part, floatRB, SAVING_BAL, req)
	if err != nil {
		return nil, err
	}
	sum += val.(*FloatValue).floatVal

	if sum < ammt.floatVal {
		fv0.floatVal = checkBal - ammt.floatVal + float64(1)
		err = exec.WriteValue(CHECKING, acct, part, fv0, 1, req)
		if err != nil {
			return nil, err
		}
	} else {
		fv0.floatVal = checkBal - ammt.floatVal
		err = exec.WriteValue(CHECKING, acct, part, fv0, 1, req)
		if err != nil {
			return nil, err
		}
	}

	if exec.Commit(req) == 0 {
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

	req := sbTrnas.req
	part := sbTrnas.accessParts[0]
	acct := sbTrnas.accoutID[0]
	ammt := &sbTrnas.ammount
	fv0 := &sbTrnas.fv[0]

	var val Value
	var err error
	_, _, err = exec.ReadValue(ACCOUNTS, acct, part, intRB, ACCT_ID, req)
	if err != nil {
		return nil, err
	}

	val, _, err = exec.ReadValue(CHECKING, acct, part, floatRB, CHECK_BAL, req)
	if err != nil {
		return nil, err
	}
	fv0.floatVal = val.(*FloatValue).floatVal + ammt.floatVal

	err = exec.WriteValue(CHECKING, acct, part, fv0, CHECK_BAL, req)
	if err != nil {
		return nil, err
	}

	if exec.Commit(req) == 0 {
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

	req := sbTrnas.req
	part := sbTrnas.accessParts[0]
	acct := sbTrnas.accoutID[0]
	ammt := &sbTrnas.ammount
	fv0 := &sbTrnas.fv[0]

	var val Value
	var err error
	_, _, err = exec.ReadValue(ACCOUNTS, acct, part, intRB, ACCT_ID, req)
	if err != nil {
		exec.Abort(req)
		return nil, err
	}

	val, _, err = exec.ReadValue(SAVINGS, acct, part, floatRB, SAVING_BAL, req)
	if err != nil {
		exec.Abort(req)
		return nil, err
	}
	sum := val.(*FloatValue).floatVal + ammt.floatVal

	if sum < 0 {
		exec.Abort(req)
		return nil, ENEGSAVINGS
	} else {
		fv0.floatVal = sum
		err = exec.WriteValue(SAVINGS, acct, part, fv0, SAVING_BAL, req)
		if err != nil {
			exec.Abort(req)
			return nil, err
		}
	}

	if exec.Commit(req) == 0 {
		return nil, EABORT
	}

	return nil, nil
}

func AddOne(t Trans, exec ETransaction) (Value, error) {
	singleTrans := t.(*SingleTrans)
	iv := singleTrans.iv

	req := singleTrans.req
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
			exec.Abort(req)
			return nil, err
		}

		//val, fromStore, err = exec.ReadValue(SINGLE, k, part, intRB, SINGLE_VAL, req)
		val, _, err = exec.ReadValue(SINGLE, k, part, intRB, SINGLE_VAL, req)
		if err != nil {
			exec.Abort(req)
			return nil, err
		}

		/*if fromStore {
			iv[i].intVal = *val.(*int64) + 1
		} else {
			iv[i].intVal = val.(*IntValue).intVal + 1
		}*/
		iv[i].intVal = val.(*IntValue).intVal + 1

		err = exec.WriteValue(SINGLE, k, part, &iv[i], SINGLE_VAL, req)
		if err != nil {
			exec.Abort(req)
			return nil, err
		}

	}

	if exec.Commit(req) == 0 {
		return nil, EABORT
	}

	return nil, nil
}

func UpdateInt(t Trans, exec ETransaction) (Value, error) {
	singleTrans := t.(*SingleTrans)
	sv := singleTrans.sv
	strRB := &singleTrans.strRB
	req := singleTrans.req
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
			val, _, err = exec.ReadValue(SINGLE, k, part, strRB, col, req)
			if err != nil {
				exec.Abort(req)
				return nil, err
			}
			if val == nil {
				exec.Abort(req)
				return nil, ENOKEY
			}
		} else {
			sv[i].stringVal = sv[i].stringVal[:CAP_SINGLE_STR]
			//for p, b := range CONST_STR_SINGLE {
			//	sv[i].stringVal[p] = byte(b)
			//}
			err = exec.WriteValue(SINGLE, k, part, &sv[i], col, req)
			if err != nil {
				exec.Abort(req)
				return nil, err
			}
		}
	}

	if exec.Commit(req) == 0 {
		return nil, EABORT
	}

	return nil, nil
}
