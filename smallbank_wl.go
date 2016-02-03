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
	A_ID = iota
	A_NAME
)

const (
	C_ID = iota
	C_BAL
)

const (
	S_ID = iota
	S_BAL
)

const (
	SBTRANSNUM  = 6
	SBMAXPARTS  = 2
	SBSTRMAXLEN = 100
)

type AccoutsTuple struct {
	padding1 [64]byte
	accoutID int64
	name     []byte
	padding2 [64]byte
}

func (at *AccoutsTuple) GetValue(col int) Value {
	switch col {
	case 0:
		return &at.accoutID
	case 1:
		return &at.name
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
	return nil
}

func (at *AccoutsTuple) SetValue(val Value, col int) {
	switch col {
	case 0:
		at.accoutID = val.(*IntValue).intVal
	case 1:
		newOne := val.(*StringValue).stringVal
		at.name = at.name[0:len(newOne)]
		for i, b := range newOne {
			at.name[i] = b
		}
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

type CheckingTuple struct {
	padding1 [64]byte
	accoutID int64
	balance  float64
	padding2 [64]byte
}

func (ct *CheckingTuple) GetValue(col int) Value {
	switch col {
	case 0:
		return &ct.accoutID
	case 1:
		return &ct.balance
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
	return nil
}

func (ct *CheckingTuple) SetValue(val Value, col int) {
	switch col {
	case 0:
		ct.accoutID = val.(*IntValue).intVal
	case 1:
		ct.balance = val.(*FloatValue).floatVal
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

type SavingsTuple struct {
	padding1 [64]byte
	accoutID int64
	balance  float64
	padding2 [64]byte
}

func (st *SavingsTuple) GetValue(col int) Value {
	switch col {
	case 0:
		return &st.accoutID
	case 1:
		return &st.balance
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
	return nil
}

func (st *SavingsTuple) SetValue(val Value, col int) {
	switch col {
	case 0:
		st.accoutID = val.(*IntValue).intVal
	case 1:
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
	ammount     FloatValue
	fv          []FloatValue
	ret         FloatValue
	trial       int
	padding2    [PADDING]byte
}

func (t *SBTrans) GetTXN() int {
	return t.TXN
}

func (s *SBTrans) GetAccessParts() []int {
	return s.accessParts
}

func (s *SBTrans) DoNothing() {

}

type SBTransGen struct {
	rnd             *rand.Rand
	transPercentage [SBTRANSNUM]int
	gen             *Generator
	trans           *SBTrans
	cr              float64
	partIndex       int
	nParts          int
	isPartition     bool
}

func (s *SBTransGen) GenOneTrans() Trans {
	t := s.trans
	rnd := s.rnd
	gen := s.gen
	cr := int(s.cr)
	pi := s.partIndex
	nParts := s.nParts
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
			tmpPi = (pi + rnd.Intn(nParts-1) + 1) % nParts
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
			tmpPi = (pi + rnd.Intn(nParts-1) + 1) % nParts
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

type SBWorkload struct {
	transPercentage [SBTRANSNUM]int
	basic           *BasicWorkload
	transGen        []*SBTransGen
}

func NewSmallBankWL(workload string, nParts int, isPartition bool, nWorkers int, s float64, transPercentage string, cr float64) *SBWorkload {
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

	sbWorkload.basic = NewBasicWorkload(workload, nParts, isPartition, nWorkers, s)

	// Populating the Store
	hp := sbWorkload.basic.generators[0]
	for i := 0; i < sbWorkload.basic.tableCount; i++ {
		keyRange := sbWorkload.basic.IDToKeyRange[i]
		nKeys := sbWorkload.basic.nKeys[i]
		store := sbWorkload.basic.store
		keyLen := len(keyRange)
		compKey := make([]OneKey, keyLen)

		var k int = 0
		for j := int64(0); j < nKeys; j++ {

			key := CKey(compKey)
			partNum := hp.GetPart(i, key)

			// Generate One Value
			if i == ACCOUNTS {
				at := &AccoutsTuple{
					accoutID: int64(compKey[0]),
					name:     make([]byte, 0, SBSTRMAXLEN+2*PADDINGBYTE),
				}
				at.name = at.name[PADDINGBYTE:PADDINGBYTE]
				at.name = at.name[:4]
				for p := 0; p < 4; p++ {
					at.name[p] = "name"[p]
				}
				store.CreateRecByID(i, key, partNum, at)
			} else if i == SAVINGS {
				st := &SavingsTuple{
					accoutID: int64(compKey[0]),
					balance:  float64(BAL),
				}
				store.CreateRecByID(i, key, partNum, st)
			} else { // CHECKING
				ct := &CheckingTuple{
					accoutID: int64(compKey[0]),
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
		trans := &SBTrans{
			accessParts: make([]int, 0, SBMAXPARTS+2*PADDINGINT),
			accoutID:    make([]Key, 0, SBMAXPARTS+2*PADDINGKEY),
			//ammount:     FloatValue{},
			fv: make([]FloatValue, SBMAXPARTS),
			//ret:         FloatValue{},
		}
		trans.accessParts = trans.accessParts[PADDINGINT:PADDINGINT]
		trans.accoutID = trans.accoutID[PADDINGKEY:PADDINGKEY]
		tg.trans = trans
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

func (s *SBWorkload) GetTableCount() int {
	return s.basic.tableCount
}

func (s *SBWorkload) PrintChecking() {
	var total float64
	nKeys := s.basic.nKeys[CHECKING]
	gen := s.basic.generators[0]
	keyRange := s.basic.IDToKeyRange[CHECKING]
	keyLen := len(keyRange)
	compKey := make([]OneKey, keyLen)
	store := s.basic.store

	var val Value
	var k int = 0
	for i := int64(0); i < nKeys; i++ {
		key := CKey(compKey)
		partNum := gen.GetPart(CHECKING, key)
		val = store.GetValueByID(CHECKING, key, partNum, C_BAL)
		total += *val.(*float64)

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