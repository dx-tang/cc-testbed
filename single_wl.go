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
	SINGLE_STR21
	SINGLE_STR22
	SINGLE_STR23
	SINGLE_STR24
	SINGLE_STR25
)

// Length for Single string
const (
	CAP_SINGLE_STR = 20
)

//const CONST_STR_SINGLE = "afsldfjskdflsjdkfljsdklfsjdklfsjdkslfdjsklfjdsklfjdsklfjdsklfjsdklfjsdlfksjdkfsdjfiwekfwe,fdjsifdsj"
const CONST_STR_SINGLE = "abcdekdifddsdfdfedse"

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
	str21    [CAP_SINGLE_STR]byte
	str22    [CAP_SINGLE_STR]byte
	str23    [CAP_SINGLE_STR]byte
	str24    [CAP_SINGLE_STR]byte
	str25    [CAP_SINGLE_STR]byte
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
	case SINGLE_STR21:
		str = &st.str21
	case SINGLE_STR22:
		str = &st.str22
	case SINGLE_STR23:
		str = &st.str23
	case SINGLE_STR24:
		str = &st.str24
	case SINGLE_STR25:
		str = &st.str25
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
	case SINGLE_STR21:
		str = &st.str21
	case SINGLE_STR22:
		str = &st.str22
	case SINGLE_STR23:
		str = &st.str23
	case SINGLE_STR24:
		str = &st.str24
	case SINGLE_STR25:
		str = &st.str25
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
	penalty     time.Time
	start       time.Time
	home        bool
	homePart    int
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

func (s *SingleTrans) GetPenalty() time.Time {
	return s.penalty
}

func (s *SingleTrans) SetPenalty(penalty time.Time) {
	s.penalty = penalty
}

func (s *SingleTrans) isHome() bool {
	return s.home
}

func (s *SingleTrans) getHome() int {
	return s.homePart
}

func (s *SingleTrans) SetStartTime(start time.Time) {
	s.start = start
}

func (s *SingleTrans) GetStartTime() time.Time {
	return s.start
}

func (s *SingleTrans) AddStartTime(genTime time.Duration) {
	s.start = s.start.Add(genTime)
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
	isPartAlign     bool
	tlen            int
	rr              int
	mp              int
	validProb       float64
	validTime       time.Time
	endTime         time.Time
	timeInit        bool
	dt              DummyTrans
	w               *Worker
	clusterNPart    []int
	otherNPart      []int
	start           []int
	end             []int
	crMix           []float64
	crRange         int
	partRnd         *rand.Rand
	padding2        [PADDING]byte
}

func (s *SingleTransGen) GenOneTrans(mode int) Trans {
	s.w.Lock()
	defer s.w.Unlock()

	t := s.transBuf[s.head]
	s.head = (s.head + 1) % QUEUESIZE

	rnd := s.rnd
	gen := s.gen
	var pi int

	//nParts := s.nParts
	tlen := s.tlen

	txn := rnd.Intn(100)
	for i, v := range s.transPercentage {
		if txn < v {
			txn = i + 1
			break
		}
	}

	t.TXN = txn + SINGLEBASE

	if *SysType == ADAPTIVE {
		if *Hybrid {
			pi = gen.GenOnePart()
		} else {
			pi = s.partIndex
		}
	} else {
		if s.isPartAlign {
			pi = s.partIndex
		} else {
			pi = gen.GenOnePart()
		}
	}

	t.homePart = pi

	if pi == s.partIndex {
		t.home = true
	} else {
		t.home = false
	}

	if s.crRange > 1 && pi < s.crRange && rnd.Intn(100) < int(s.crMix[pi]) {
		t.accessParts = t.accessParts[:2]
		tmpPi := (pi + s.partRnd.Intn(s.crRange-1) + 1) % s.crRange
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

	t.keys = t.keys[:tlen]
	t.parts = t.parts[:tlen]
	j := 0
	for i := 0; i < len(t.keys); i++ {
		t.parts[i] = t.accessParts[j]
		t.keys[i] = gen.GetKey(SINGLE, t.parts[i])
		if *Hybrid && t.keys[i][0] < HOTREC { // isHot
			t.keys[i][3] |= HOTBIT
		}
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

func NewSingleWL(workload string, nParts int, isPartition bool, nWorkers int, s float64, transPercentage string, cr float64, tlen int, rr int, mp int, ps float64, initMode int, double bool, partAlign bool, useLatch []bool) *SingelWorkload {
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
		singleWL.basic = NewBasicWorkload(workload, nParts, isPartition, nWorkers, s, NOPARTSKEW, initMode, double, useLatch)
	} else {
		singleWL.zp = NewZipfProb(NOPARTSKEW, *NumPart)
		singleWL.basic = NewBasicWorkload(workload, nParts, isPartition, nWorkers, s, ps, initMode, double, useLatch)
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

		store.CreateRecByID(SINGLE, key, partNum, st, nil)

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

	if isPartition && *Report && *SysType == ADAPTIVE {
		for i, table := range store.priTables {
			table.MergeLoad(store.secTables[i], nil, 0, *NumPart, nil)
		}
		for i, table := range store.secTables {
			table.BulkLoad(store.backTables[i], nil, 0, 1, hp.partitioner[i])
		}
		for i, _ := range store.secTables {
			store.secTables[i].Clean()
			store.backTables[i].Clean()
		}
	} else if !isPartition && *Report && *SysType == ADAPTIVE {
		for i, table := range store.priTables {
			table.BulkLoad(store.secTables[i], nil, 0, 1, hp.partitioner[i])
		}
		for i, table := range store.secTables {
			table.MergeLoad(store.backTables[i], nil, 0, *NumPart, nil)
		}
		for i, _ := range store.secTables {
			store.secTables[i].Clean()
			store.backTables[i].Clean()
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
			nParts:          *NumPart,
			isPartition:     isPartition,
			isPartAlign:     partAlign,
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

		tg.crMix = make([]float64, *NumPart+PADDINGINT64*2)
		tg.crMix = tg.crMix[PADDINGINT64 : *NumPart+PADDINGINT64]
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
		rec, _, _, _ := store.GetRecByID(SINGLE, key, partNum)
		rec.SetValue(iv, SINGLE_VAL)

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
		tg.w.Lock()
		tg.gen.keyGens = keygens[i]
		tg.gen.partGen = partGens[i]
		tg.cr = cr
		tg.tlen = tlen
		tg.rr = rr
		tg.mp = mp
		tg.validProb = singleWL.zp.GetProb(i)
		tg.timeInit = false
		tg.w.Unlock()
	}
}

func (singleWL *SingelWorkload) OnlineMixReconf(tc []TestCase, keygens [][]KeyGen) {
	for i := 0; i < len(singleWL.transGen); i++ {
		tg := singleWL.transGen[i]
		tg.w.Lock()
		tg.gen.keyGens = keygens[i]
		tg.cr = tc[0].CR
		tg.tlen = tc[0].Tlen
		tg.rr = tc[0].RR
		for j := 0; j < len(tc); j++ {
			tg.crMix[j] = tc[j].CR
		}
		tg.crRange = tc[0].Range
		tg.w.Unlock()
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
		rec, _, _, err := store.GetRecByID(SINGLE, key, partNum)
		if err != nil {
			clog.Info("Get Value Error")
		}
		rec.GetValue(intRB, SINGLE_VAL)
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

func (singleWL *SingelWorkload) ResetPartAlign(isPartAlign bool) {
	for _, tranGen := range singleWL.transGen {
		tranGen.isPartAlign = isPartAlign
	}
}

func (singleWL *SingelWorkload) ResetPart(nParts int, isPartition bool) {
	singleWL.basic.ResetPart(*NumPart, isPartition)
	for _, tranGen := range singleWL.transGen {
		tranGen.isPartition = isPartition
		//tranGen.nParts = nParts
	}
}

func (singleWL *SingelWorkload) Switch(nParts int, isPartition bool, tmpPS float64) {
	singleWL.GetStore().Switch()
	singleWL.ResetPart(nParts, isPartition)
	singleWL.zp.Reconf(tmpPS)
	for i := 0; i < len(singleWL.transGen); i++ {
		tg := singleWL.transGen[i]
		tg.validProb = singleWL.zp.GetProb(i)
		tg.timeInit = false
	}
}

func (singleWL *SingelWorkload) SetWorkers(coord *Coordinator) {
	Workers := coord.Workers
	for i, w := range Workers {
		singleWL.transGen[i].w = w
	}
}

func (singleWL *SingelWorkload) MixConfig(wc []WorkerConfig) {
	for i := 0; i < len(singleWL.transGen); i++ {
		tg := singleWL.transGen[i]
		tg.partRnd = rand.New(rand.NewSource(time.Now().UnixNano() / int64(i+1)))
		tg.start = make([]int, len(wc))
		tg.end = make([]int, len(wc))
		tg.clusterNPart = make([]int, len(wc))
		tg.otherNPart = make([]int, len(wc))
		for j := 0; j < len(wc); j++ {
			tg.start[j] = int(wc[j].start)
			tg.end[j] = int(wc[j].end)
			tg.clusterNPart[j] = tg.end[j] - tg.start[j] + 1
			tg.otherNPart[j] = *NumPart - tg.clusterNPart[j]
		}
	}
}
