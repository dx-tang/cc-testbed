package testbed

import (
	"math/rand"
	"strconv"
	"strings"

	"github.com/totemtang/cc-testbed/clog"
)

// Transaction Parameters
const (
	BAL  = 1000000
	AMMT = 1
)

// Table Reference Constants
const (
	ACCOUNTS = iota
	SAVINGS
	CHECKING
)

// Column Reference Constants
const (
	ACCT_ID = iota
	ACCT_NAME
)

const (
	CHECK_ID = iota
	CHECK_BAL
)

const (
	SAVING_ID = iota
	SAVING_BAL
)

// Define CAPACITY For String Fields
const (
	CAP_ACCT_NAME = 4
)

const (
	SBTRANSNUM  = 6
	SBMAXPARTS  = 2
	SBSTRMAXLEN = 100
)

type AccoutsTuple struct {
	padding1 [PADDING]byte
	accoutID int64
	name     [CAP_ACCT_NAME]byte
	padding2 [PADDING]byte
}

func (at *AccoutsTuple) GetValue(val Value, col int) {
	switch col {
	case ACCT_ID:
		iv := val.(*IntValue)
		iv.intVal = at.accoutID
	case ACCT_NAME:
		sv := val.(*StringValue)
		sv.stringVal = sv.stringVal[:CAP_ACCT_NAME]
		for i := 0; i < CAP_ACCT_NAME; i++ {
			sv.stringVal[i] = at.name[i]
		}
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

func (at *AccoutsTuple) SetValue(val Value, col int) {
	switch col {
	case ACCT_ID:
		at.accoutID = val.(*IntValue).intVal
	case ACCT_NAME:
		newOne := val.(*StringValue).stringVal
		for i, b := range newOne {
			at.name[i] = b
		}
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

type CheckingTuple struct {
	padding1 [PADDING]byte
	accoutID int64
	balance  float64
	padding2 [PADDING]byte
}

func (ct *CheckingTuple) GetValue(val Value, col int) {
	switch col {
	case CHECK_ID:
		iv := val.(*IntValue)
		iv.intVal = ct.accoutID
	case CHECK_BAL:
		fv := val.(*FloatValue)
		fv.floatVal = ct.balance
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

func (ct *CheckingTuple) SetValue(val Value, col int) {
	switch col {
	case CHECK_ID:
		ct.accoutID = val.(*IntValue).intVal
	case CHECK_BAL:
		ct.balance = val.(*FloatValue).floatVal
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

type SavingsTuple struct {
	padding1 [PADDING]byte
	accoutID int64
	balance  float64
	padding2 [PADDING]byte
}

func (st *SavingsTuple) GetValue(val Value, col int) {
	switch col {
	case SAVING_ID:
		iv := val.(*IntValue)
		iv.intVal = st.accoutID
	case SAVING_BAL:
		fv := val.(*FloatValue)
		fv.floatVal = st.balance
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

func (st *SavingsTuple) SetValue(val Value, col int) {
	switch col {
	case SAVING_ID:
		st.accoutID = val.(*IntValue).intVal
	case SAVING_BAL:
		st.balance = val.(*FloatValue).floatVal
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

type SBTrans struct {
	padding1    [PADDING]byte
	TXN         int
	accessParts []int
	accoutID    []Key
	intRB       IntValue
	floatRB     FloatValue
	ammount     FloatValue
	fv          []FloatValue
	ret         FloatValue
	trial       int
	tid         TID
	padding2    [PADDING]byte
}

func (t *SBTrans) GetTXN() int {
	return t.TXN
}

func (s *SBTrans) GetAccessParts() []int {
	return s.accessParts
}

func (s *SBTrans) SetTID(tid TID) {
	s.tid = tid
}

func (s *SBTrans) SetTrial(trial int) {
	s.trial = trial
}

func (s *SBTrans) GetTrial() int {
	return s.trial
}

func (s *SBTrans) DecTrial() {
	s.trial--
}

type SBTransGen struct {
	rnd             *rand.Rand
	transPercentage [SBTRANSNUM]int
	gen             *Generator
	transBuf        []*SBTrans
	head            int
	tail            int
	cr              float64
	partIndex       int
	nParts          int
	isPartition     bool
}

func (s *SBTransGen) GenOneTrans(mode int) Trans {
	t := s.transBuf[s.head]
	s.head = (s.head + 1) % QUEUESIZE
	rnd := s.rnd
	gen := s.gen
	cr := int(s.cr)
	var pi int
	if mode == PARTITION {
		pi = s.partIndex
	} else {
		pi = rnd.Intn(s.nParts)
	}
	isPart := s.isPartition

	txn := rnd.Intn(100)
	for i, v := range s.transPercentage {
		if txn < v {
			txn = i + 1
			break
		}
	}

	t.TXN = txn + SMALLBANKBASE

	var tmpPi int
	switch t.TXN {
	case BALANCE: // Get Balance from One Account
		t.accessParts = t.accessParts[:1]
		t.accessParts[0] = pi
		t.accoutID = t.accoutID[:1]
		t.accoutID[0] = gen.GetKey(CHECKING, pi)
	case WRITECHECK:
		t.accessParts = t.accessParts[:1]
		t.accessParts[0] = pi
		t.accoutID = t.accoutID[:1]
		t.accoutID[0] = gen.GetKey(CHECKING, pi)
		t.ammount.floatVal = float64(AMMT)
	case AMALGAMATE:
		if isPart && rnd.Intn(100) < cr { // cross-partition transaction
			t.accessParts = t.accessParts[:2]
			t.accoutID = t.accoutID[:2]
			//tmpPi = (pi + rnd.Intn(nParts-1) + 1) % nParts
			for {
				tmpPi = gen.GenOnePart()
				if tmpPi != pi {
					break
				}
			}
			if tmpPi > pi {
				t.accessParts[0] = pi
				t.accessParts[1] = tmpPi
				t.accoutID[0] = gen.GetKey(CHECKING, pi)
				t.accoutID[1] = gen.GetKey(CHECKING, tmpPi)
			} else {
				t.accessParts[0] = tmpPi
				t.accessParts[1] = pi
				t.accoutID[0] = gen.GetKey(CHECKING, tmpPi)
				t.accoutID[1] = gen.GetKey(CHECKING, pi)
			}
		} else {
			t.accessParts = t.accessParts[:1]
			t.accessParts[0] = pi
			t.accoutID = t.accoutID[:2]
			t.accoutID[0] = gen.GetKey(CHECKING, pi)
			for {
				tmpKey := gen.GetKey(CHECKING, pi)
				if tmpKey != t.accoutID[0] {
					t.accoutID[1] = tmpKey
					break
				}
			}
		}
	case SENDPAYMENT:
		if isPart && rnd.Intn(100) < cr { // cross-partition transaction
			t.accessParts = t.accessParts[:2]
			t.accoutID = t.accoutID[:2]
			//tmpPi = (pi + rnd.Intn(nParts-1) + 1) % nParts
			for {
				tmpPi = gen.GenOnePart()
				if tmpPi != pi {
					break
				}
			}
			if tmpPi > pi {
				t.accessParts[0] = pi
				t.accessParts[1] = tmpPi
				t.accoutID[0] = gen.GetKey(CHECKING, pi)
				t.accoutID[1] = gen.GetKey(CHECKING, tmpPi)
			} else {
				t.accessParts[0] = tmpPi
				t.accessParts[1] = pi
				t.accoutID[0] = gen.GetKey(CHECKING, tmpPi)
				t.accoutID[1] = gen.GetKey(CHECKING, pi)
			}
		} else {
			t.accessParts = t.accessParts[:1]
			t.accessParts[0] = pi
			t.accoutID = t.accoutID[:2]
			t.accoutID[0] = gen.GetKey(CHECKING, pi)
			for {
				tmpKey := gen.GetKey(CHECKING, pi)
				if tmpKey != t.accoutID[0] {
					t.accoutID[1] = tmpKey
					break
				}
			}
		}
		t.ammount.floatVal = float64(AMMT)
	case DEPOSITCHECKING:
		t.accessParts = t.accessParts[:1]
		t.accessParts[0] = pi
		t.accoutID = t.accoutID[:1]
		t.accoutID[0] = gen.GetKey(CHECKING, pi)
		t.ammount.floatVal = float64(AMMT)
	case TRANSACTIONSAVINGS:
		t.accessParts = t.accessParts[:1]
		t.accessParts[0] = pi
		t.accoutID = t.accoutID[:1]
		t.accoutID[0] = gen.GetKey(SAVINGS, pi)
		t.ammount.floatVal = float64(AMMT)
	default:
		clog.Error("SmallBank does not support transaction %v\n", t.TXN)
	}

	if WDTRIAL > 0 {
		t.trial = rnd.Intn(WDTRIAL)
	}

	return t
}

func (s *SBTransGen) ReleaseOneTrans(t Trans) {
	s.tail = (s.tail + 1) % QUEUESIZE
	s.transBuf[s.tail] = t.(*SBTrans)
}

type SBWorkload struct {
	transPercentage [SBTRANSNUM]int
	basic           *BasicWorkload
	transGen        []*SBTransGen
}

func NewSmallBankWL(workload string, nParts int, isPartition bool, isPhysical bool, nWorkers int, s float64, transPercentage string, cr float64, ps float64) *SBWorkload {
	sbWorkload := &SBWorkload{}

	tp := strings.Split(transPercentage, ":")
	if len(tp) != SBTRANSNUM {
		clog.Error("Wrong format of transaction percentage string %s\n", transPercentage)
	}

	for i, str := range tp {
		per, err := strconv.Atoi(str)
		if err != nil {
			clog.Error("TransPercentage Format Error %s\n", str)
		}
		if i != 0 {
			sbWorkload.transPercentage[i] = sbWorkload.transPercentage[i-1] + per
		} else {
			sbWorkload.transPercentage[i] = per
		}
	}

	if sbWorkload.transPercentage[SBTRANSNUM-1] != 100 {
		clog.Error("Wrong format of transaction percentage string %s; Sum should be 100\n", transPercentage)
	}

	sbWorkload.basic = NewBasicWorkload(workload, nParts, isPartition, isPhysical, nWorkers, s, ps)

	// Populating the Store
	hp := sbWorkload.basic.generators[0]
	for i := 0; i < sbWorkload.basic.tableCount; i++ {
		keyRange := sbWorkload.basic.IDToKeyRange[i]
		nKeys := sbWorkload.basic.nKeys[i]
		store := sbWorkload.basic.store
		keyLen := len(keyRange)
		compKey := make([]int64, keyLen)

		var k int = 0
		for j := int64(0); j < nKeys; j++ {

			key := CKey(compKey)
			partNum := hp.GetPart(i, key)

			// Generate One Value
			if i == ACCOUNTS {
				at := &AccoutsTuple{
					accoutID: compKey[0],
					//name:     make([]byte, 0, SBSTRMAXLEN+2*PADDINGBYTE),
				}
				//at.name = at.name[PADDINGBYTE:PADDINGBYTE]
				//at.name = at.name[:4]
				for p := 0; p < CAP_ACCT_NAME; p++ {
					at.name[p] = "name"[p]
				}
				store.CreateRecByID(i, key, partNum, at)
			} else if i == SAVINGS {
				st := &SavingsTuple{
					accoutID: compKey[0],
					balance:  float64(BAL),
				}
				store.CreateRecByID(i, key, partNum, st)
			} else { // CHECKING
				ct := &CheckingTuple{
					accoutID: compKey[0],
					balance:  float64(BAL),
				}
				store.CreateRecByID(i, key, partNum, ct)
			}

			for int64(compKey[k]+1) >= keyRange[k] {
				compKey[k] = 0
				k++
				if k >= keyLen {
					break
				}
			}
			if k < keyLen {
				compKey[k]++
				k = 0
			}
		}
	}

	// Prepare for generating transactions
	sbWorkload.transGen = make([]*SBTransGen, nWorkers)
	for i := 0; i < nWorkers; i++ {
		tg := &SBTransGen{
			gen:             sbWorkload.basic.generators[i],
			rnd:             rand.New(rand.NewSource(int64(i*13 + 17))),
			transPercentage: sbWorkload.transPercentage,
			cr:              cr,
			nParts:          nParts,
			isPartition:     isPartition,
		}
		if isPartition {
			tg.partIndex = i
		} else {
			tg.partIndex = 0
		}
		tg.transBuf = make([]*SBTrans, QUEUESIZE+2*PADDINGINT64)
		tg.transBuf = tg.transBuf[PADDINGINT64 : QUEUESIZE+PADDINGINT64]
		for p := 0; p < QUEUESIZE; p++ {
			trans := &SBTrans{
				accessParts: make([]int, 0, SBMAXPARTS+2*PADDINGINT),
				accoutID:    make([]Key, 0, SBMAXPARTS+2*PADDINGKEY),
				//ammount:     FloatValue{},
				fv: make([]FloatValue, SBMAXPARTS),
				//ret:         FloatValue{},
			}
			trans.accessParts = trans.accessParts[PADDINGINT:PADDINGINT]
			trans.accoutID = trans.accoutID[PADDINGKEY:PADDINGKEY]
			tg.transBuf[p] = trans
		}
		tg.head = 0
		tg.tail = -1
		sbWorkload.transGen[i] = tg
	}

	return sbWorkload
}

func (s *SBWorkload) GetTransGen(partIndex int) TransGen {
	if partIndex >= len(s.transGen) {
		clog.Error("Part Index %v Out of Range %v for TransGen\n", partIndex, len(s.transGen))
	}
	return s.transGen[partIndex]
}

func (s *SBWorkload) GetStore() *Store {
	return s.basic.store
}

func (s *SBWorkload) ResetConf(transPercentage string, cr float64) {
	tp := strings.Split(transPercentage, ":")
	if len(tp) != SBTRANSNUM {
		clog.Error("Wrong format of transaction percentage string %s\n", transPercentage)
	}
	for i, str := range tp {
		per, err := strconv.Atoi(str)
		if err != nil {
			clog.Error("TransPercentage Format Error %s\n", str)
		}
		if i != 0 {
			s.transPercentage[i] = s.transPercentage[i-1] + per
		} else {
			s.transPercentage[i] = per
		}
	}

	if s.transPercentage[SBTRANSNUM-1] != 100 {
		clog.Error("Wrong format of transaction percentage string %s; Sum should be 100\n", transPercentage)
	}

	for i := 0; i < len(s.transGen); i++ {
		tg := s.transGen[i]
		tg.gen = s.basic.generators[i]
		tg.transPercentage = s.transPercentage
		tg.cr = cr
		tg.head = 0
		tg.tail = -1
	}

}

func (sb *SBWorkload) ResetPart(nParts int, isPartition bool) {
	sb.basic.ResetPart(nParts, isPartition)
	for i, tranGen := range sb.transGen {
		if isPartition {
			tranGen.partIndex = i
		} else {
			tranGen.partIndex = 0
		}
		tranGen.isPartition = isPartition
		tranGen.nParts = nParts
	}
}

func (s *SBWorkload) GetBasicWL() *BasicWorkload {
	return s.basic
}

func (s *SBWorkload) GetIDToKeyRange() [][]int64 {
	return s.basic.IDToKeyRange
}

func (s *SBWorkload) GetTableCount() int {
	return s.basic.tableCount
}

func (s *SBWorkload) PrintChecking() {
	var total float64
	nKeys := s.basic.nKeys[CHECKING]
	gen := s.basic.generators[0]
	keyRange := s.basic.IDToKeyRange[CHECKING]
	keyLen := len(keyRange)
	compKey := make([]int64, keyLen)
	store := s.basic.store
	floatRB := &FloatValue{}

	var val Value
	var k int = 0
	for i := int64(0); i < nKeys; i++ {
		key := CKey(compKey)
		partNum := gen.GetPart(CHECKING, key)
		val = store.GetValueByID(CHECKING, key, partNum, floatRB, CHECK_BAL)
		total += val.(*FloatValue).floatVal

		for int64(compKey[k]+1) >= keyRange[k] {
			compKey[k] = 0
			k++
			if k >= keyLen {
				break
			}
		}
		if k < keyLen {
			compKey[k]++
			k = 0
		}
	}

	clog.Info("Total Checking Balance %v\n", total)
}

func (s *SBWorkload) ResetData() {
	nKeys := s.basic.nKeys[CHECKING]
	gen := s.basic.generators[0]
	keyRange := s.basic.IDToKeyRange[CHECKING]
	keyLen := len(keyRange)
	compKey := make([]int64, keyLen)
	store := s.basic.store
	floatRB := &FloatValue{}

	var k int = 0
	for i := int64(0); i < nKeys; i++ {
		key := CKey(compKey)
		partNum := gen.GetPart(CHECKING, key)

		floatRB.floatVal = BAL
		store.SetValueByID(CHECKING, key, partNum, floatRB, CHECK_BAL)
		store.SetValueByID(SAVINGS, key, partNum, floatRB, SAVING_BAL)

		for int64(compKey[k]+1) >= keyRange[k] {
			compKey[k] = 0
			k++
			if k >= keyLen {
				break
			}
		}
		if k < keyLen {
			compKey[k]++
			k = 0
		}
	}
}
