package testbed

import (
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

	LAST_TXN
)

type Trans interface {
	GetTXN() int
	GetAccessParts() []int
	DoNothing()
}

type TransGen interface {
	GenOneTrans() Trans
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
	_, err = exec.ReadValue(ACCOUNTS, acctId0, part0, A_ID)
	if err != nil {
		return nil, err
	}

	_, err = exec.ReadValue(ACCOUNTS, acctId1, part1, A_ID)
	if err != nil {
		return nil, err
	}

	val, err = exec.ReadValue(SAVINGS, acctId0, part0, S_BAL)
	if err != nil {
		return nil, err
	}
	//sum := val.(*FloatValue).floatVal
	fv0.floatVal = *val.(*float64)

	val, err = exec.ReadValue(CHECKING, acctId1, part1, C_BAL)
	if err != nil {
		return nil, err
	}
	//sum += val.(*FloatValue).floatVal
	fv1.floatVal = *val.(*float64)

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

	err = exec.WriteValue(CHECKING, acctId1, part1, fv0, C_BAL)
	if err != nil {
		return nil, err
	}

	err = exec.WriteValue(SAVINGS, acctId0, part0, fv1, S_BAL)
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
	_, err = exec.ReadValue(ACCOUNTS, send, part0, A_ID)
	if err != nil {
		return nil, err
	}

	_, err = exec.ReadValue(ACCOUNTS, dest, part1, A_ID)
	if err != nil {
		return nil, err
	}

	val, err = exec.ReadValue(CHECKING, send, part0, C_BAL)
	if err != nil {
		return nil, err
	}
	bal := *val.(*float64)

	if bal < ammt.floatVal {
		exec.Abort()
		return nil, ELACKBALANCE
	}

	fv0.floatVal = bal - ammt.floatVal

	val, err = exec.ReadValue(CHECKING, dest, part1, C_BAL)
	if err != nil {
		return nil, err
	}
	fv1.floatVal = *val.(*float64) + ammt.floatVal

	err = exec.WriteValue(CHECKING, send, part0, fv0, C_BAL)
	if err != nil {
		return nil, err
	}

	err = exec.WriteValue(CHECKING, dest, part1, fv1, C_BAL)
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

	ret := &sbTrnas.ret
	part := sbTrnas.accessParts[0]
	acct := sbTrnas.accoutID[0]

	var val Value
	var err error
	_, err = exec.ReadValue(ACCOUNTS, acct, part, A_ID)
	if err != nil {
		return nil, err
	}

	val, err = exec.ReadValue(CHECKING, acct, part, C_BAL)
	if err != nil {
		return nil, err
	}
	ret.floatVal = *val.(*float64)

	val, err = exec.ReadValue(SAVINGS, acct, part, S_BAL)
	if err != nil {
		return nil, err
	}

	ret.floatVal += *val.(*float64)

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

	part := sbTrnas.accessParts[0]
	acct := sbTrnas.accoutID[0]
	ammt := &sbTrnas.ammount
	fv0 := &sbTrnas.fv[0]

	var val Value
	var err error
	_, err = exec.ReadValue(ACCOUNTS, acct, part, A_ID)
	if err != nil {
		return nil, err
	}

	val, err = exec.ReadValue(CHECKING, acct, part, C_BAL)
	if err != nil {
		return nil, err
	}
	checkBal := *val.(*float64)
	sum := checkBal

	val, err = exec.ReadValue(SAVINGS, acct, part, S_BAL)
	if err != nil {
		return nil, err
	}
	sum += *val.(*float64)

	if sum < ammt.floatVal {
		fv0.floatVal = checkBal - ammt.floatVal + float64(1)
		err = exec.WriteValue(CHECKING, acct, part, fv0, 1)
		if err != nil {
			return nil, err
		}
	} else {
		fv0.floatVal = checkBal - ammt.floatVal
		err = exec.WriteValue(CHECKING, acct, part, fv0, 1)
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

	part := sbTrnas.accessParts[0]
	acct := sbTrnas.accoutID[0]
	ammt := &sbTrnas.ammount
	fv0 := &sbTrnas.fv[0]

	var val Value
	var err error
	_, err = exec.ReadValue(ACCOUNTS, acct, part, A_ID)
	if err != nil {
		return nil, err
	}

	val, err = exec.ReadValue(CHECKING, acct, part, C_BAL)
	if err != nil {
		return nil, err
	}
	fv0.floatVal = *val.(*float64) + ammt.floatVal

	err = exec.WriteValue(CHECKING, acct, part, fv0, C_BAL)
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

	part := sbTrnas.accessParts[0]
	acct := sbTrnas.accoutID[0]
	ammt := &sbTrnas.ammount
	fv0 := &sbTrnas.fv[0]

	var val Value
	var err error
	_, err = exec.ReadValue(ACCOUNTS, acct, part, A_ID)
	if err != nil {
		return nil, err
	}

	val, err = exec.ReadValue(SAVINGS, acct, part, S_BAL)
	if err != nil {
		return nil, err
	}
	sum := *val.(*float64) + ammt.floatVal

	if sum < 0 {
		return nil, ENEGSAVINGS
	} else {
		fv0.floatVal = sum
		err = exec.WriteValue(SAVINGS, acct, part, fv0, S_BAL)
		if err != nil {
			return nil, err
		}
	}

	if exec.Commit() == 0 {
		return nil, EABORT
	}

	return nil, nil
}
