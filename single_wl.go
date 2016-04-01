package testbed

import (
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/totemtang/cc-testbed/clog"
)

const (
	INITVAL = 0
)

const (
	SINGLE = iota
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
)

// Length for Single string
const (
	CAP_SINGLE_STR = 10
)

//const CONST_STR_SINGLE = "afsldfjskdflsjdkfljsdklfsjdklfsjdkslfdjsklfjdsklfjdsklfjdsklfjsdklfjsdlfksjdkfsdjfiwekfwe,fdjsifdsj"
const CONST_STR_SINGLE = "abcdekdifd"

type SingleTuple struct {
	padding1 [PADDING]byte
	id       int64
	val      int64
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
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}

	sv := val.(*StringValue)
	for i, b := range sv.stringVal {
		(*str)[i] = b
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
	tid         TID
	padding2    [PADDING]byte
}

func (s *SingleTrans) GetTXN() int {
	return s.TXN
}

func (s *SingleTrans) GetAccessParts() []int {
	return s.accessParts
}

func (s *SingleTrans) SetTID(tid TID) {
	s.tid = tid
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
}

func (s *SingleTransGen) GenOneTrans() Trans {
	t := s.transBuf[s.head]
	s.head = (s.head + 1) % QUEUESIZE

	rnd := s.rnd
	gen := s.gen
	cr := int(s.cr)
	pi := s.partIndex
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

	if isPart && rnd.Intn(100) < cr {
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

	if WDTRIAL > 0 {
		t.trial = rnd.Intn(WDTRIAL)
	}

	//t.readNum = (s.rr * s.tlen) / 100
	t.rr = s.rr

	return t
}

func (s *SingleTransGen) ReleaseOneTrans(t Trans) {
	s.tail = (s.tail + 1) % QUEUESIZE
	s.transBuf[s.tail] = t.(*SingleTrans)
}

type SingelWorkload struct {
	transPercentage [SINGLETRANSNUM]int
	basic           *BasicWorkload
	transGen        []*SingleTransGen
}

func NewSingleWL(workload string, nParts int, isPartition bool, isPhysical bool, nWorkers int, s float64, transPercentage string, cr float64, tlen int, rr int, mp int, ps float64) *SingelWorkload {
	singleWL := &SingelWorkload{}

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

	singleWL.basic = NewBasicWorkload(workload, nParts, isPartition, isPhysical, nWorkers, s, ps)

	// Populating the Store
	hp := singleWL.basic.generators[0]
	keyRange := singleWL.basic.IDToKeyRange[SINGLE]
	nKeys := singleWL.basic.nKeys[SINGLE]
	store := singleWL.basic.store
	keyLen := len(keyRange)
	compKey := make([]OneKey, keyLen)

	var k int = 0
	for j := int64(0); j < nKeys; j++ {

		key := CKey(compKey)
		partNum := hp.GetPart(SINGLE, key)

		st := &SingleTuple{
			id:  int64(compKey[0]),
			val: int64(INITVAL),
		}

		store.CreateRecByID(SINGLE, key, partNum, st)

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
		}
		if isPartition {
			tg.partIndex = i
		} else {
			tg.partIndex = 0
		}
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

func (singleWL *SingelWorkload) ResetConf(transPercentage string, cr float64, mp int, tlen int, rr int) {
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
	}
}

func (singleWL *SingelWorkload) ResetData() {
	nKeys := singleWL.basic.nKeys[SINGLE]
	gen := singleWL.basic.generators[0]
	keyRange := singleWL.basic.IDToKeyRange[SINGLE]
	keyLen := len(keyRange)
	compKey := make([]OneKey, keyLen)
	store := singleWL.basic.store
	iv := &IntValue{
		intVal: INITVAL,
	}
	var k int = 0
	for i := int64(0); i < nKeys; i++ {
		key := CKey(compKey)
		partNum := gen.GetPart(SINGLE, key)
		store.SetValueByID(SINGLE, key, partNum, iv, SINGLE_VAL)

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

func (singleWL *SingelWorkload) GetIDToKeyRange() [][]int64 {
	return singleWL.basic.IDToKeyRange
}

func (singleWL *SingelWorkload) GetBasicWL() *BasicWorkload {
	return singleWL.basic
}

func (s *SingelWorkload) PrintSum() {
	var total int64
	nKeys := s.basic.nKeys[SINGLE]
	gen := s.basic.generators[0]
	keyRange := s.basic.IDToKeyRange[SINGLE]
	keyLen := len(keyRange)
	compKey := make([]OneKey, keyLen)
	store := s.basic.store
	intRB := &IntValue{}

	var k int = 0
	for i := int64(0); i < nKeys; i++ {
		key := CKey(compKey)
		partNum := gen.GetPart(SINGLE, key)
		err := store.GetValueByID(SINGLE, key, partNum, intRB, SINGLE_VAL)
		if err != nil {
			clog.Info("Get Value Error")
		}
		total += intRB.intVal

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

	clog.Info("Sum: %v\n", total)
}

func (singleWL *SingelWorkload) ResetPart(nParts int, isPartition bool) {
	singleWL.basic.ResetPart(nParts, isPartition)
	for i, tranGen := range singleWL.transGen {
		if isPartition {
			tranGen.partIndex = i
		} else {
			tranGen.partIndex = 0
		}
		tranGen.isPartition = isPartition
		tranGen.nParts = nParts
	}
}
