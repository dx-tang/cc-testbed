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
)

type SingleTuple struct {
	padding1 [64]byte
	id       int64
	val      int64
	padding2 [64]byte
}

func (st *SingleTuple) GetValue(col int) Value {
	switch col {
	case 0:
		return &st.id
	case 1:
		return &st.val
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
	return nil
}

func (st *SingleTuple) SetValue(val Value, col int) {
	switch col {
	case 0:
		st.id = val.(*IntValue).intVal
	case 1:
		st.val = val.(*IntValue).intVal
	default:
		clog.Error("Column Index %v Out of Range\n", col)
	}
}

type SingleTrans struct {
	padding1    [PADDING]byte
	TXN         int
	accessParts []int
	keys        []Key
	parts       []int
	iv          []IntValue
	trial       int
	readNum     int
	padding2    [PADDING]byte
}

func (s *SingleTrans) GetTXN() int {
	return s.TXN
}

func (s *SingleTrans) GetAccessParts() []int {
	return s.accessParts
}

func (s *SingleTrans) DoNothing() {

}

type SingleTransGen struct {
	rnd             *rand.Rand
	transPercentage [SINGLETRANSNUM]int
	gen             *Generator
	trans           *SingleTrans
	cr              float64
	partIndex       int
	nParts          int
	isPartition     bool
	tlen            int
	rr              int
	mp              int
}

func (s *SingleTransGen) GenOneTrans() Trans {
	t := s.trans
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

	t.readNum = (s.rr * s.tlen) / 100

	return t
}

type SingelWorkload struct {
	transPercentage [SINGLETRANSNUM]int
	basic           *BasicWorkload
	transGen        []*SingleTransGen
}

func NewSingleWL(workload string, nParts int, isPartition bool, nWorkers int, s float64, transPercentage string, cr float64, tlen int, rr int, mp int, ps float64) *SingelWorkload {
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

	singleWL.basic = NewBasicWorkload(workload, nParts, isPartition, nWorkers, s, ps)

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
		trans := &SingleTrans{
			accessParts: make([]int, 0, mp+2*PADDINGINT),
			keys:        make([]Key, 0, SINGLEMAXKEYS+2*PADDINGKEY),
			parts:       make([]int, 0, SINGLEMAXKEYS+2*PADDINGINT),
			iv:          make([]IntValue, SINGLEMAXKEYS),
		}
		trans.accessParts = trans.accessParts[PADDINGINT:PADDINGINT]
		trans.keys = trans.keys[PADDINGKEY:PADDINGKEY]
		trans.parts = trans.parts[PADDINGINT:PADDINGINT]
		tg.trans = trans
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

	var val Value
	var k int = 0
	for i := int64(0); i < nKeys; i++ {
		key := CKey(compKey)
		partNum := gen.GetPart(SINGLE, key)
		val = store.GetValueByID(SINGLE, key, partNum, SINGLE_VAL)
		total += *val.(*int64)

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
