package testbed

import (
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/totemtang/cc-testbed/clog"
	"github.com/totemtang/cc-testbed/spinlock"
)

const (
	INITVAL = 0
)

const (
	SINGLE = iota
)

const (
	TIMESLICE = 100 // 100 Milliseconds
)

const (
	SINGLETRANSNUM = 2
	SINGLEMAXPARTS = 10
	SINGLEMAXKEYS  = 100
)

const (
	SINGLE_ID = iota
	SINGLE_VAL
	SINGLE_STR1
	SINGLE_STR2
	SINGLE_STR3
	SINGLE_STR4
	SINGLE_STR5
	SINGLE_STR6
	SINGLE_STR7
	SINGLE_STR8
	SINGLE_STR9
	SINGLE_STR10
	SINGLE_STR11
	SINGLE_STR12
	SINGLE_STR13
	SINGLE_STR14
	SINGLE_STR15
	SINGLE_STR16
	SINGLE_STR17
	SINGLE_STR18
	SINGLE_STR19
	SINGLE_STR20
)

// Length for Single string
const (
	CAP_SINGLE_STR = 10
)

//const CONST_STR_SINGLE = "afsldfjskdflsjdkfljsdklfsjdklfsjdkslfdjsklfjdsklfjdsklfjdsklfjsdklfjsdlfksjdkfsdjfiwekfwe,fdjsifdsj"
const CONST_STR_SINGLE = "abcdekdifd"

type SingleTuple struct {
	padding1 [PADDING]byte
	id       int
	val      int
	str1     [CAP_SINGLE_STR]byte
	str2     [CAP_SINGLE_STR]byte
	str3     [CAP_SINGLE_STR]byte
	str4     [CAP_SINGLE_STR]byte
	str5     [CAP_SINGLE_STR]byte
	str6     [CAP_SINGLE_STR]byte
	str7     [CAP_SINGLE_STR]byte
	str8     [CAP_SINGLE_STR]byte
	str9     [CAP_SINGLE_STR]byte
	str10    [CAP_SINGLE_STR]byte
	str11    [CAP_SINGLE_STR]byte
	str12    [CAP_SINGLE_STR]byte
	str13    [CAP_SINGLE_STR]byte
	str14    [CAP_SINGLE_STR]byte
	str15    [CAP_SINGLE_STR]byte
	str16    [CAP_SINGLE_STR]byte
	str17    [CAP_SINGLE_STR]byte
	str18    [CAP_SINGLE_STR]byte
	str19    [CAP_SINGLE_STR]byte
	str20    [CAP_SINGLE_STR]byte
	padding2 [PADDING]byte
}

func (st *SingleTuple) GetValue(val Value, col int) {
	var str *[CAP_SINGLE_STR]byte
	switch col {
	case SINGLE_ID:
		val.(*IntValue).intVal = st.id
		return
	case SINGLE_VAL:
		val.(*IntValue).intVal = st.val
		return
	case SINGLE_STR1:
		str = &st.str1
	case SINGLE_STR2:
		str = &st.str2
	case SINGLE_STR3:
		str = &st.str3
	case SINGLE_STR4:
		str = &st.str4
	case SINGLE_STR5:
		str = &st.str5
	case SINGLE_STR6:
		str = &st.str6
	case SINGLE_STR7:
		str = &st.str7
	case SINGLE_STR8:
		str = &st.str8
	case SINGLE_STR9:
		str = &st.str9
	case SINGLE_STR10:
		str = &st.str10
	case SINGLE_STR11:
		str = &st.str11
	case SINGLE_STR12:
		str = &st.str12
	case SINGLE_STR13:
		str = &st.str13
	case SINGLE_STR14:
		str = &st.str14
	case SINGLE_STR15:
		str = &st.str15
	case SINGLE_STR16:
		str = &st.str16
	case SINGLE_STR17:
		str = &st.str17
	case SINGLE_STR18:
		str = &st.str18
	case SINGLE_STR19:
		str = &st.str19
	case SINGLE_STR20:
		str = &st.str20
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}

	sv := val.(*StringValue)
	sv.stringVal = sv.stringVal[:CAP_SINGLE_STR]
	for i, b := range *str {
		sv.stringVal[i] = b
	}
}

func (st *SingleTuple) SetValue(val Value, col int) {
	var str *[CAP_SINGLE_STR]byte
	switch col {
	case SINGLE_ID:
		st.id = val.(*IntValue).intVal
		return
	case SINGLE_VAL:
		st.val = val.(*IntValue).intVal
		return
	case SINGLE_STR1:
		str = &st.str1
	case SINGLE_STR2:
		str = &st.str2
	case SINGLE_STR3:
		str = &st.str3
	case SINGLE_STR4:
		str = &st.str4
	case SINGLE_STR5:
		str = &st.str5
	case SINGLE_STR6:
		str = &st.str6
	case SINGLE_STR7:
		str = &st.str7
	case SINGLE_STR8:
		str = &st.str8
	case SINGLE_STR9:
		str = &st.str9
	case SINGLE_STR10:
		str = &st.str10
	case SINGLE_STR11:
		str = &st.str11
	case SINGLE_STR12:
		str = &st.str12
	case SINGLE_STR13:
		str = &st.str13
	case SINGLE_STR14:
		str = &st.str14
	case SINGLE_STR15:
		str = &st.str15
	case SINGLE_STR16:
		str = &st.str16
	case SINGLE_STR17:
		str = &st.str17
	case SINGLE_STR18:
		str = &st.str18
	case SINGLE_STR19:
		str = &st.str19
	case SINGLE_STR20:
		str = &st.str20
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}

	sv := val.(*StringValue)
	for i, b := range sv.stringVal {
		(*str)[i] = b
	}
}

func (st *SingleTuple) DeltaValue(val Value, col int) {
	switch col {
	case SINGLE_VAL:
		st.val = st.val + val.(*IntValue).intVal
		return
	default:
		clog.Error("Column Index %v Does not support DeltaValue", col)
	}
}

type SingleTrans struct {
	padding1    [PADDING]byte
	TXN         int
	accessParts []int
	keys        []Key
	parts       []int
	iv          []IntValue
	sv          []StringValue
	intRB       IntValue
	strRB       StringValue
	trial       int
	rnd         *rand.Rand
	rr          int
	req         LockReq
	padding2    [PADDING]byte
}

func (s *SingleTrans) GetTXN() int {
	return s.TXN
}

func (s *SingleTrans) GetAccessParts() []int {
	return s.accessParts
}

func (s *SingleTrans) SetTID(tid TID) {
	s.req.tid = tid
}

func (s *SingleTrans) SetTrial(trial int) {
	s.trial = trial
}

func (s *SingleTrans) GetTrial() int {
	return s.trial
}

func (s *SingleTrans) DecTrial() {
	s.trial--
}

type SingleTransGen struct {
	padding1 [PADDING]byte
	spinlock.Spinlock
	rnd             *rand.Rand
	transPercentage [SINGLETRANSNUM]int
	gen             *Generator
	transBuf        []*SingleTrans
	head            int
	tail            int
	cr              float64
	partIndex       int
	nParts          int
	isPartition     bool
	tlen            int
	rr              int
	mp              int
	validProb       float64
	validTime       time.Time
	endTime         time.Time
	timeInit        bool
	dt              DummyTrans
	padding2        [PADDING]byte
}

func (s *SingleTransGen) GenOneTrans(mode int) Trans {
	s.Lock()
	defer s.Unlock()

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
	/*
		if mode == PARTITION {
			pi = s.partIndex
		} else {
			pi = rnd.Intn(s.nParts)
		}*/
	nParts := s.nParts
	isPart := s.isPartition && s.tlen > 1 && s.nParts > 1 && s.mp > 1
	tlen := s.tlen
	mp := s.mp

	txn := rnd.Intn(100)
	for i, v := range s.transPercentage {
		if txn < v {
			txn = i + 1
			break
		}
	}

	t.TXN = txn + SINGLEBASE

	if isPart {
		pi = s.partIndex
	} else {
		pi = gen.GenOnePart()
	}

	if rnd.Intn(100) < cr {
		ap := nParts
		if ap > mp {
			ap = mp
		}
		if ap > tlen {
			ap = tlen
		}
		t.accessParts = t.accessParts[:ap]
		start := gen.GenOnePart()
		//start := rnd.Intn(nParts)
		end := (start + ap - 1) % nParts
		wrap := false
		if start >= end {
			wrap = true
		}
		//clog.Info("start %v; end %v; pi %v", start, end, pi)
		if (!wrap && pi >= start && pi < end) || (wrap && (pi >= start || pi < end)) { // The Extending Parts Includes the Home Partition
			if start+ap <= nParts {
				for i := 0; i < ap; i++ {
					t.accessParts[i] = start + i
				}
			} else {
				for i := 0; i < ap; i++ {
					tmp := start + i
					if tmp >= nParts {
						t.accessParts[tmp-nParts] = tmp - nParts
					} else {
						t.accessParts[ap-nParts+start+i] = tmp
					}
				}
			}
		} else {
			if !wrap { // Conseculative Partitions; No Wrap
				if pi < start { // pi to the left
					t.accessParts[0] = pi
					for i := 0; i < ap-1; i++ {
						t.accessParts[i+1] = start + i
					}
				} else { // pi to the right
					t.accessParts[ap-1] = pi
					for i := 0; i < ap-1; i++ {
						t.accessParts[i] = start + i
					}
				}
			} else { // Wrap
				t.accessParts[ap-nParts+start-1] = pi
				for i := 0; i < ap-1; i++ {
					tmp := start + i
					if tmp >= nParts {
						t.accessParts[tmp-nParts] = tmp - nParts
					} else {
						t.accessParts[ap-nParts+start+i] = tmp
					}
				}
			}
		}
	} else {
		t.accessParts = t.accessParts[:1]
		t.accessParts[0] = pi
	}

	t.keys = t.keys[:tlen]
	t.parts = t.parts[:tlen]
	j := 0
	for i := 0; i < len(t.keys); i++ {
		t.parts[i] = t.accessParts[j]
		t.keys[i] = gen.GetKey(SINGLE, t.parts[i])
		j = (j + 1) % len(t.accessParts)
	}

	t.rr = s.rr

	return t
}

func (s *SingleTransGen) ReleaseOneTrans(t Trans) {
	s.tail = (s.tail + 1) % QUEUESIZE
	s.transBuf[s.tail] = t.(*SingleTrans)
}

type SingelWorkload struct {
	padding1        [PADDING]byte
	transPercentage [SINGLETRANSNUM]int
	basic           *BasicWorkload
	transGen        []*SingleTransGen
	zp              ZipfProb
	padding2        [PADDING]byte
}

func NewSingleWL(workload string, nParts int, isPartition bool, nWorkers int, s float64, transPercentage string, cr float64, tlen int, rr int, mp int, ps float64, initMode int) *SingelWorkload {
	singleWL := &SingelWorkload{}

	if nParts == 1 {
		isPartition = false
	}

	tp := strings.Split(transPercentage, ":")
	if len(tp) != SINGLETRANSNUM {
		clog.Error("Wrong format of transaction percentage string %s\n", transPercentage)
	}

	for i, str := range tp {
		per, err := strconv.Atoi(str)
		if err != nil {
			clog.Error("TransPercentage Format Error %s\n", str)
		}
		if i != 0 {
			singleWL.transPercentage[i] = singleWL.transPercentage[i-1] + per
		} else {
			singleWL.transPercentage[i] = per
		}
	}

	if singleWL.transPercentage[SINGLETRANSNUM-1] != 100 {
		clog.Error("Wrong format of transaction percentage string %s; Sum should be 100\n", transPercentage)
	}

	if isPartition {
		singleWL.zp = NewZipfProb(ps, *NumPart)
		singleWL.basic = NewBasicWorkload(workload, nParts, isPartition, nWorkers, s, NOPARTSKEW, initMode)
	} else {
		singleWL.zp = NewZipfProb(NOPARTSKEW, *NumPart)
		singleWL.basic = NewBasicWorkload(workload, nParts, isPartition, nWorkers, s, ps, initMode)
	}

	// Populating the Store
	hp := singleWL.basic.generators[0]
	keyRange := singleWL.basic.IDToKeyRange[SINGLE]
	nKeys := singleWL.basic.nKeys[SINGLE]
	store := singleWL.basic.store
	keyLen := len(keyRange)
	//compKey := make([]int, keyLen)
	var key Key

	var k int = 0
	for j := 0; j < nKeys; j++ {

		partNum := hp.GetPart(SINGLE, key)

		st := &SingleTuple{
			id:  int(key[0]),
			val: int(INITVAL),
		}

		store.CreateRecByID(SINGLE, key, partNum, st)

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

	// Prepare for generating transactions
	singleWL.transGen = make([]*SingleTransGen, nWorkers)
	for i := 0; i < nWorkers; i++ {
		tg := &SingleTransGen{
			gen:             singleWL.basic.generators[i],
			rnd:             rand.New(rand.NewSource(time.Now().UnixNano() / int64(i*13+17))),
			transPercentage: singleWL.transPercentage,
			cr:              cr,
			nParts:          nParts,
			isPartition:     isPartition,
			tlen:            tlen,
			rr:              rr,
			mp:              mp,
			validProb:       singleWL.zp.GetProb(i),
			timeInit:        false,
		}

		tg.partIndex = i

		tg.transBuf = make([]*SingleTrans, QUEUESIZE+2*PADDINGINT64)
		tg.transBuf = tg.transBuf[PADDINGINT64 : PADDINGINT64+QUEUESIZE]
		for p := 0; p < QUEUESIZE; p++ {
			trans := &SingleTrans{
				accessParts: make([]int, 0, mp+2*PADDINGINT),
				keys:        make([]Key, 0, SINGLEMAXKEYS+2*PADDINGKEY),
				parts:       make([]int, 0, SINGLEMAXKEYS+2*PADDINGINT),
				iv:          make([]IntValue, SINGLEMAXKEYS),
				sv:          make([]StringValue, SINGLEMAXKEYS),
			}
			trans.accessParts = trans.accessParts[PADDINGINT:PADDINGINT]
			trans.keys = trans.keys[PADDINGKEY:PADDINGKEY]
			trans.parts = trans.parts[PADDINGINT:PADDINGINT]
			trans.rnd = rand.New(rand.NewSource(time.Now().UnixNano() / int64(i*17+19)))
			trans.rr = rr
			trans.req.id = i
			for q := 0; q < SINGLEMAXKEYS; q++ {
				trans.sv[q].stringVal = make([]byte, CAP_SINGLE_STR+2*PADDINGBYTE)
				trans.sv[q].stringVal = trans.sv[q].stringVal[PADDINGBYTE : PADDINGBYTE+CAP_SINGLE_STR]
			}
			trans.strRB.stringVal = make([]byte, CAP_SINGLE_STR+2*PADDINGBYTE)
			trans.strRB.stringVal = trans.strRB.stringVal[PADDINGBYTE : PADDINGBYTE+CAP_SINGLE_STR]
			tg.transBuf[p] = trans
		}
		tg.head = 0
		tg.tail = -1
		singleWL.transGen[i] = tg
	}

	return singleWL
}

func (s *SingelWorkload) GetTransGen(partIndex int) TransGen {
	if partIndex >= len(s.transGen) {
		clog.Error("Part Index %v Out of Range %v for TransGen\n", partIndex, len(s.transGen))
	}
	return s.transGen[partIndex]
}

func (s *SingelWorkload) GetStore() *Store {
	return s.basic.store
}

func (s *SingelWorkload) GetTableCount() int {
	return s.basic.tableCount
}

func (singleWL *SingelWorkload) ResetConf(transPercentage string, cr float64, mp int, tlen int, rr int, ps float64) {
	tp := strings.Split(transPercentage, ":")
	if len(tp) != SINGLETRANSNUM {
		clog.Error("Wrong format of transaction percentage string %s\n", transPercentage)
	}
	for i, str := range tp {
		per, err := strconv.Atoi(str)
		if err != nil {
			clog.Error("TransPercentage Format Error %s\n", str)
		}
		if i != 0 {
			singleWL.transPercentage[i] = singleWL.transPercentage[i-1] + per
		} else {
			singleWL.transPercentage[i] = per
		}
	}

	if singleWL.transPercentage[SINGLETRANSNUM-1] != 100 {
		clog.Error("Wrong format of transaction percentage string %s; Sum should be 100\n", transPercentage)
	}

	singleWL.zp.Reconf(ps)

	for i := 0; i < len(singleWL.transGen); i++ {
		tg := singleWL.transGen[i]
		tg.gen = singleWL.basic.generators[i]
		tg.transPercentage = singleWL.transPercentage
		tg.cr = cr
		tg.tlen = tlen
		tg.rr = rr
		tg.mp = mp
		tg.head = 0
		tg.tail = -1
		tg.validProb = singleWL.zp.GetProb(i)
		tg.timeInit = false
	}
}

func (singleWL *SingelWorkload) ResetData() {
	nKeys := singleWL.basic.nKeys[SINGLE]
	gen := singleWL.basic.generators[0]
	keyRange := singleWL.basic.IDToKeyRange[SINGLE]
	keyLen := len(keyRange)
	//compKey := make([]int, keyLen)
	var key Key
	store := singleWL.basic.store
	iv := &IntValue{
		intVal: INITVAL,
	}
	var k int = 0
	for i := int(0); i < nKeys; i++ {

		partNum := gen.GetPart(SINGLE, key)
		store.SetValueByID(SINGLE, key, partNum, iv, SINGLE_VAL)

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

func (singleWL *SingelWorkload) GetIDToKeyRange() [][]int {
	return singleWL.basic.IDToKeyRange
}

func (singleWL *SingelWorkload) GetBasicWL() *BasicWorkload {
	return singleWL.basic
}

func (singleWL *SingelWorkload) OnlineReconf(keygens [][]KeyGen, partGens []PartGen, cr float64, mp int, tlen int, rr int, ps float64) {
	singleWL.zp.Reconf(ps)
	for i := 0; i < len(singleWL.transGen); i++ {
		tg := singleWL.transGen[i]
		tg.Lock()
		tg.gen.keyGens = keygens[i]
		tg.gen.partGen = partGens[i]
		tg.cr = cr
		tg.tlen = tlen
		tg.rr = rr
		tg.mp = mp
		tg.validProb = singleWL.zp.GetProb(i)
		tg.timeInit = false
		tg.Unlock()
	}
}

func (singleWL *SingelWorkload) GetPartitioner(partIndex int) []Partitioner {
	return singleWL.basic.generators[partIndex].partitioner
}

func (s *SingelWorkload) PrintSum() {
	var total int
	nKeys := s.basic.nKeys[SINGLE]
	gen := s.basic.generators[0]
	keyRange := s.basic.IDToKeyRange[SINGLE]
	keyLen := len(keyRange)
	var key Key
	store := s.basic.store
	intRB := &IntValue{}

	var k int = 0
	for i := 0; i < nKeys; i++ {
		partNum := gen.GetPart(SINGLE, key)
		err := store.GetValueByID(SINGLE, key, partNum, intRB, SINGLE_VAL)
		if err != nil {
			clog.Info("Get Value Error")
		}
		total += intRB.intVal

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

	clog.Info("Sum: %v\n", total)
}

func (singleWL *SingelWorkload) ResetPart(nParts int, isPartition bool) {
	singleWL.basic.ResetPart(nParts, isPartition)
	for _, tranGen := range singleWL.transGen {
		tranGen.isPartition = isPartition
		tranGen.nParts = nParts
	}
}
