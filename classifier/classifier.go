package classifier

//#cgo CFLAGS: -I/usr/include/python2.7
//#cgo LDFLAGS:-lpython2.7
//#include "classifier.h"
import "C"

import (
	"github.com/totemtang/cc-testbed/clog"
)

const (
	SINGLEWL = iota
	SMALLBANKWL
	TPCCWL
)

type Classifier interface {
	Predict(curType int, partAvg float64, partSkew float64, recAvg float64, latency float64, readRate float64, homeConf float64, confRate float64) int
	GetIndexProb() float64
	GetPartProb() float64
	GetOCCProb() float64
	GetPureProb() float64
	Finalize()
}

type SingleClassifier struct {
	padding1 [64]byte
	clf      *C.PyObject
	padding2 [64]byte
}

func NewClassifier(path string, partFile string, occFile string, pureFile string, indexFile string, WLTYPE int) Classifier {
	addPath := "sys.path.append('" + path + "')"
	C.Init(C.CString(addPath))
	sc := &SingleClassifier{}
	part := C.CString(partFile)
	occ := C.CString(occFile)
	pure := C.CString(pureFile)
	index := C.CString(indexFile)
	if WLTYPE == SINGLEWL {
		sc.clf = C.SingleTrain(part, occ, pure, index)
		if sc.clf == nil {
			clog.Error("Training Error")
		}
	} else if WLTYPE == SMALLBANKWL {
		sc.clf = C.SBTrain(part, occ, pure, index)
		if sc.clf == nil {
			clog.Error("Training Error")
		}
	} else {
		sc.clf = C.TPCCTrain(part, occ, pure, index)
		if sc.clf == nil {
			clog.Error("Training Error")
		}
	}

	return sc
}

func (sc *SingleClassifier) Predict(curType int, partAvg float64, partSkew float64, recAvg float64, latency float64, readRate float64, homeconf float64, confRate float64) int {
	cCurType := C.int(curType)
	cPartAvg := C.double(partAvg)
	cPartSkew := C.double(partSkew)
	cRecAvg := C.double(recAvg)
	cLatency := C.double(latency)
	cReadRate := C.double(readRate)
	cHomeConf := C.double(homeconf)
	cConfRate := C.double(confRate)

	c := C.Predict(sc.clf, cCurType, cPartAvg, cPartSkew, cRecAvg, cLatency, cReadRate, cHomeConf, cConfRate)
	if c < 0 {
		clog.Error("Predict Error")
	}
	return int(c)
}

func (sc *SingleClassifier) GetIndexProb() float64 {
	c := C.GetIndexProb(sc.clf)
	if c < 0 {
		clog.Error("Get Prob Error")
	}
	return float64(c)
}

func (sc *SingleClassifier) GetPartProb() float64 {
	c := C.GetPartProb(sc.clf)
	if c < 0 {
		clog.Error("Get Prob Error")
	}
	return float64(c)
}

func (sc *SingleClassifier) GetOCCProb() float64 {
	c := C.GetOCCProb(sc.clf)
	if c < 0 {
		clog.Error("Get Prob Error")
	}
	return float64(c)
}

func (sc *SingleClassifier) GetPureProb() float64 {
	c := C.GetPureProb(sc.clf)
	if c < 0 {
		clog.Error("Get Prob Error")
	}
	return float64(c)
}

func (sc *SingleClassifier) Finalize() {
	C.Final()
}
