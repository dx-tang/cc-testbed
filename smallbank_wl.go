package testbed

import (
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/spinlock"
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
	SBTRANSNUM    = 6
	SBMAXPARTS    = 2
	SBMAXSUBTRANS = 5
	SBMAXKEYS     = SBMAXPARTS * SBMAXSUBTRANS
	SBSTRMAXLEN   = 100
)

type AccoutsTuple struct {
	padding1 [PADDING]byte
	accoutID int
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

func (at *AccoutsTuple) DeltaValue(val Value, col int) {
	clog.Error("Account Table does not support DeltaValue\n")
}

type CheckingTuple struct {
	padding1 [PADDING]byte
	accoutID int
	balance  float32
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

func (ct *CheckingTuple) DeltaValue(val Value, col int) {
	switch col {
	case CHECK_BAL:
		ct.balance = ct.balance + val.(*FloatValue).floatVal
	default:
		clog.Error("Column Index %v does not support DeltaValue\n", col)
	}
}

type SavingsTuple struct {
	padding1 [PADDING]byte
	accoutID int
	balance  float32
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

func (st *SavingsTuple) DeltaValue(val Value, col int) {
	switch col {
	case CHECK_BAL:
		st.balance = st.balance + val.(*FloatValue).floatVal
	default:
		clog.Error("Column Index %v does not support DeltaValue\n", col)
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
	req         LockReq
	penalty     time.Time
	home        bool
	homePart    int
	padding2    [PADDING]byte
}

func (t *SBTrans) GetTXN() int {
	return t.TXN
}

func (s *SBTrans) GetAccessParts() []int {
	return s.accessParts
}

func (s *SBTrans) SetTID(tid TID) {
	s.req.tid = tid
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

func (s *SBTrans) SetPenalty(penalty time.Time) {
	s.penalty = penalty
}

func (s *SBTrans) GetPenalty() time.Time {
	return s.penalty
}

func (s *SBTrans) isHome() bool {
	return s.home
}

func (s *SBTrans) getHome() int {
	return s.homePart
}

type SBTransGen struct {
	padding1 [PADDING]byte
	spinlock.Spinlock
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
	validProb       float64
	validTime       time.Time
	endTime         time.Time
	timeInit        bool
	dt              DummyTrans
	w               *Worker
	padding2        [PADDING]byte
}

func (s *SBTransGen) GenOneTrans(mode int) Trans {
	s.w.Lock()
	defer s.w.Unlock()

	start := time.Now()

	if !s.timeInit {
		s.endTime = start.Add(time.Duration(TIMESLICE) * time.Millisecond)
		s.validTime = start.Add(time.Duration(TIMESLICE*s.validProb) * time.Millisecond)
		s.timeInit = true
	}

	if start.After(s.validTime) {
		if start.Before(s.endTime) { // Issue Dummy Trans
			return &s.dt
		} else {
			s.endTime = start.Add(time.Duration(TIMESLICE) * time.Millisecond)
			s.validTime = start.Add(time.Duration(TIMESLICE*s.validProb) * time.Millisecond)
		}
	}

	t := s.transBuf[s.head]
	s.head = (s.head + 1) % QUEUESIZE

	rnd := s.rnd
	gen := s.gen
	cr := int(s.cr)
	var pi int

	isPart := s.isPartition

	if isPart {
		pi = s.partIndex
	} else {
		pi = gen.GenOnePart()
	}

	txn := rnd.Intn(100)
	for i, v := range s.transPercentage {
		if txn < v {
			txn = i + 1
			break
		}
	}

	t.TXN = txn + SMALLBANKBASE

	if isPart {
		pi = s.partIndex
	} else {
		pi = gen.GenOnePart()
	}

	t.homePart = pi

	if pi == s.partIndex {
		t.home = true
	} else {
		t.home = false
	}

	var tmpPi int
	switch t.TXN {
	case BALANCE: // Get Balance from One Account
		t.accessParts = t.accessParts[:1]
		t.accessParts[0] = pi
		t.accoutID = t.accoutID[:SBMAXSUBTRANS]
		for i := 0; i < SBMAXSUBTRANS; i++ {
			t.accoutID[i] = gen.GetKey(CHECKING, pi)
		}
	case WRITECHECK:
		t.accessParts = t.accessParts[:1]
		t.accessParts[0] = pi
		t.accoutID = t.accoutID[:SBMAXSUBTRANS]
		for i := 0; i < SBMAXSUBTRANS; i++ {
			t.accoutID[i] = gen.GetKey(CHECKING, pi)
		}
		t.ammount.floatVal = AMMT
	case AMALGAMATE:
		if rnd.Intn(100) < cr { // cross-partition transaction
			t.accessParts = t.accessParts[:2]
			for {
				tmpPi = gen.GenOnePart()
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
			t.accoutID = t.accoutID[:SBMAXKEYS]
			for i := 0; i < SBMAXSUBTRANS; i++ {
				t.accoutID[i*SBMAXPARTS+0] = gen.GetKey(CHECKING, t.accessParts[0])
				t.accoutID[i*SBMAXPARTS+1] = gen.GetKey(CHECKING, t.accessParts[1])
			}
		} else {
			t.accessParts = t.accessParts[:1]
			t.accessParts[0] = pi
			t.accoutID = t.accoutID[:SBMAXKEYS]
			for i := 0; i < SBMAXSUBTRANS; i++ {
				t.accoutID[i*SBMAXPARTS+0] = gen.GetKey(CHECKING, pi)
				for {
					tmpKey := gen.GetKey(CHECKING, pi)
					if tmpKey != t.accoutID[0] {
						t.accoutID[i*SBMAXPARTS+1] = tmpKey
						break
					}
				}
			}
		}
	case SENDPAYMENT:
		if rnd.Intn(100) < cr { // cross-partition transaction
			t.accessParts = t.accessParts[:2]
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
			} else {
				t.accessParts[0] = tmpPi
				t.accessParts[1] = pi
			}
			t.accoutID = t.accoutID[:SBMAXKEYS]
			for i := 0; i < SBMAXSUBTRANS; i++ {
				t.accoutID[i*SBMAXPARTS+0] = gen.GetKey(CHECKING, t.accessParts[0])
				t.accoutID[i*SBMAXPARTS+1] = gen.GetKey(CHECKING, t.accessParts[1])
			}
		} else {
			t.accessParts = t.accessParts[:1]
			t.accessParts[0] = pi
			t.accoutID = t.accoutID[:SBMAXKEYS]
			for i := 0; i < SBMAXSUBTRANS; i++ {
				t.accoutID[i*SBMAXPARTS+0] = gen.GetKey(CHECKING, pi)
				for {
					tmpKey := gen.GetKey(CHECKING, pi)
					if tmpKey != t.accoutID[0] {
						t.accoutID[i*SBMAXPARTS+1] = tmpKey
						break
					}
				}
			}
		}
		t.ammount.floatVal = AMMT
	case DEPOSITCHECKING:
		t.accessParts = t.accessParts[:1]
		t.accessParts[0] = pi
		t.accoutID = t.accoutID[:SBMAXSUBTRANS]
		for i := 0; i < SBMAXSUBTRANS; i++ {
			t.accoutID[i] = gen.GetKey(CHECKING, pi)
		}
		t.ammount.floatVal = AMMT
	case TRANSACTIONSAVINGS:
		t.accessParts = t.accessParts[:1]
		t.accessParts[0] = pi
		t.accoutID = t.accoutID[:SBMAXSUBTRANS]
		for i := 0; i < SBMAXSUBTRANS; i++ {
			t.accoutID[i] = gen.GetKey(SAVINGS, pi)
		}
		t.ammount.floatVal = AMMT
	default:
		clog.Error("SmallBank does not support transaction %v\n", t.TXN)
	}

	return t
}

func (s *SBTransGen) ReleaseOneTrans(t Trans) {
	s.tail = (s.tail + 1) % QUEUESIZE
	s.transBuf[s.tail] = t.(*SBTrans)
}

type SBWorkload struct {
	padding1        [PADDING]byte
	transPercentage [SBTRANSNUM]int
	basic           *BasicWorkload
	transGen        []*SBTransGen
	zp              ZipfProb
	padding2        [PADDING]byte
}

func NewSmallBankWL(workload string, nParts int, isPartition bool, nWorkers int, s float64, transPercentage [SBTRANSNUM]int, cr float64, ps float64, initMode int, double bool) *SBWorkload {
	sbWorkload := &SBWorkload{}

	if nParts == 1 {
		isPartition = false
	}

	sbWorkload.transPercentage = transPercentage

	if isPartition {
		sbWorkload.zp = NewZipfProb(ps, *NumPart)
		sbWorkload.basic = NewBasicWorkload(workload, nParts, isPartition, nWorkers, s, NOPARTSKEW, initMode, double)
	} else {
		sbWorkload.zp = NewZipfProb(NOPARTSKEW, *NumPart)
		sbWorkload.basic = NewBasicWorkload(workload, nParts, isPartition, nWorkers, s, ps, initMode, double)
	}

	// Populating the Store
	hp := sbWorkload.basic.generators[0]
	for i := 0; i < sbWorkload.basic.tableCount; i++ {
		keyRange := sbWorkload.basic.IDToKeyRange[i]
		nKeys := sbWorkload.basic.nKeys[i]
		store := sbWorkload.basic.store
		keyLen := len(keyRange)
		//compKey := make([]int, keyLen)
		var key Key

		var k int = 0
		for j := 0; j < nKeys; j++ {

			//key := CKey(compKey)
			partNum := hp.GetPart(i, key)

			// Generate One Value
			if i == ACCOUNTS {
				at := &AccoutsTuple{
					accoutID: key[0],
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
					accoutID: key[0],
					balance:  BAL,
				}
				store.CreateRecByID(i, key, partNum, st)
			} else { // CHECKING
				ct := &CheckingTuple{
					accoutID: key[0],
					balance:  BAL,
				}
				store.CreateRecByID(i, key, partNum, ct)
			}

			for key[k]+1 >= keyRange[k] {
				key[k] = 0
				k++
				if k >= keyLen {
					break
				}
			}
			if k < keyLen {
				key[k]++
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
			nParts:          *NumPart,
			isPartition:     isPartition,
			validProb:       sbWorkload.zp.GetProb(i),
			timeInit:        false,
		}

		tg.partIndex = i

		tg.transBuf = make([]*SBTrans, QUEUESIZE+2*PADDINGINT64)
		tg.transBuf = tg.transBuf[PADDINGINT64 : QUEUESIZE+PADDINGINT64]
		for p := 0; p < QUEUESIZE; p++ {
			trans := &SBTrans{
				accessParts: make([]int, 0, SBMAXPARTS+2*PADDINGINT),
				accoutID:    make([]Key, 0, SBMAXKEYS+2*PADDINGKEY),
				//ammount:     FloatValue{},
				fv: make([]FloatValue, SBMAXKEYS),
				//ret:         FloatValue{},
			}
			trans.req.id = i
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

func (s *SBWorkload) ResetConf(transPercentage string, cr float64, ps float64) {
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

	s.zp.Reconf(ps)

	for i := 0; i < len(s.transGen); i++ {
		tg := s.transGen[i]
		tg.gen = s.basic.generators[i]
		tg.transPercentage = s.transPercentage
		tg.cr = cr
		tg.head = 0
		tg.tail = -1
		tg.validProb = s.zp.GetProb(i)
		tg.timeInit = false
	}

}

func (sb *SBWorkload) ResetPart(nParts int, isPartition bool) {
	sb.basic.ResetPart(nParts, isPartition)
	for _, tranGen := range sb.transGen {
		tranGen.isPartition = isPartition
		//tranGen.nParts = nParts
	}
}

func (s *SBWorkload) GetBasicWL() *BasicWorkload {
	return s.basic
}

func (s *SBWorkload) GetIDToKeyRange() [][]int {
	return s.basic.IDToKeyRange
}

func (s *SBWorkload) GetTableCount() int {
	return s.basic.tableCount
}

func (s *SBWorkload) GetPartitioner(partIndex int) []Partitioner {
	return s.basic.generators[partIndex].partitioner
}

func (s *SBWorkload) PrintChecking() {
	var total float32
	nKeys := s.basic.nKeys[CHECKING]
	gen := s.basic.generators[0]
	keyRange := s.basic.IDToKeyRange[CHECKING]
	keyLen := len(keyRange)
	//compKey := make([]int, keyLen)
	var key Key
	store := s.basic.store
	floatRB := &FloatValue{}

	var val Value
	var k int = 0
	for i := 0; i < nKeys; i++ {
		//key := CKey(compKey)
		partNum := gen.GetPart(CHECKING, key)
		val = store.GetValueByID(CHECKING, key, partNum, floatRB, CHECK_BAL)
		total += val.(*FloatValue).floatVal

		for key[k]+1 >= keyRange[k] {
			key[k] = 0
			k++
			if k >= keyLen {
				break
			}
		}
		if k < keyLen {
			key[k]++
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
	//compKey := make([]int, keyLen)
	var key Key
	store := s.basic.store
	floatRB := &FloatValue{}

	var k int = 0
	for i := 0; i < nKeys; i++ {
		//key := CKey(compKey)
		partNum := gen.GetPart(CHECKING, key)

		floatRB.floatVal = BAL
		store.SetValueByID(CHECKING, key, partNum, floatRB, CHECK_BAL)
		store.SetValueByID(SAVINGS, key, partNum, floatRB, SAVING_BAL)

		for key[k]+1 >= keyRange[k] {
			key[k] = 0
			k++
			if k >= keyLen {
				break
			}
		}
		if k < keyLen {
			key[k]++
			k = 0
		}
	}
}

func (sb *SBWorkload) OnlineReconf(keygens [][]KeyGen, partGens []PartGen, cr float64, transper [SBTRANSNUM]int, ps float64) {
	sb.zp.Reconf(ps)
	for i := 0; i < len(sb.transGen); i++ {
		tg := sb.transGen[i]
		tg.Lock()
		tg.gen.keyGens = keygens[i]
		tg.gen.partGen = partGens[i]
		tg.cr = cr
		tg.transPercentage = transper
		tg.validProb = sb.zp.GetProb(i)
		tg.timeInit = false
		tg.Unlock()
	}
}

func (sb *SBWorkload) Switch(nParts int, isPartition bool, tmpPS float64) {
	sb.GetStore().Switch()
	sb.ResetPart(nParts, isPartition)
	sb.zp.Reconf(tmpPS)
	for i := 0; i < len(sb.transGen); i++ {
		tg := sb.transGen[i]
		tg.validProb = sb.zp.GetProb(i)
		tg.timeInit = false
	}
}

func (sb *SBWorkload) SetWorkers(coord *Coordinator) {
	Workers := coord.Workers
	for i, w := range Workers {
		sb.transGen[i].w = w
	}
}
