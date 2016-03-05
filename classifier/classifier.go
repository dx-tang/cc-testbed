package classifier

//#cgo CFLAGS: -I/usr/include/python2.7
//#cgo LDFLAGS:-lpython2.7
//#include "classifier.h"
import "C"

import (
	"github.com/totemtang/cc-testbed/clog"
)

type Classifier interface {
	Predict(partAvg float64, partSkew float64, partLenSkew float64, recAvg float64, hitRate float64, readRate float64, confRate float64) int
	Finalize()
}

type SingleClassifier struct {
	padding1 [64]byte
	partClf  *C.PyObject
	occClf   *C.PyObject
	padding2 [64]byte
}

func NewSingleClassifier(path string, partTS string, occTS string) *SingleClassifier {
	addPath := "sys.path.append('" + path + "')"
	C.Init(C.CString(addPath))
	sc := &SingleClassifier{}
	part := C.CString(partTS)
	occ := C.CString(occTS)
	sc.partClf = C.SinglePartTrain(part)
	if sc.partClf == nil {
		clog.Error("Single Part Training Error")
	}
	sc.occClf = C.SingleOCCTrain(occ)
	if sc.occClf == nil {
		clog.Error("Single OCC Training Error")
	}
	return sc
}

func (sc *SingleClassifier) Predict(partAvg float64, partSkew float64, partLenSkew float64, recAvg float64, hitRate float64, readRate float64, confRate float64) int {
	cPartAvg := C.double(partAvg)
	cPartSkew := C.double(partSkew)
	cPartLenSkew := C.double(partLenSkew)
	cRecAvg := C.double(recAvg)
	cHitRate := C.double(hitRate)
	cReadRate := C.double(readRate)
	cConfRate := C.double(confRate)

	c := C.SinglePartPredict(sc.partClf, cPartAvg, cPartSkew, cPartLenSkew, cRecAvg, cHitRate, cReadRate)
	if c < 0 {
		clog.Error("Single Part Predict Error")
	}
	if c != 0 {
		c = C.SingleOCCPredict(sc.occClf, cRecAvg, cHitRate, cReadRate, cConfRate)
		if c < 0 {
			clog.Error("Single OCC Predict Error")
		}
	}
	return int(c)
}

func (sc *SingleClassifier) Finalize() {
	C.Final()
}

type SmallbankClassifier struct {
	padding1 [64]byte
	partClf  *C.PyObject
	occClf   *C.PyObject
	padding2 [64]byte
}

func NewSBClassifier(path string, partTS string, occTS string) *SmallbankClassifier {
	addPath := "sys.path.append('" + path + "')"
	C.Init(C.CString(addPath))
	sbc := &SmallbankClassifier{}
	part := C.CString(partTS)
	occ := C.CString(occTS)
	sbc.partClf = C.SBPartTrain(part)
	if sbc.partClf == nil {
		clog.Error("Smallbank Part Training Error")
	}
	sbc.occClf = C.SBOCCTrain(occ)
	if sbc.partClf == nil {
		clog.Error("Smallbank Part Training Error")
	}
	return sbc
}

func (sbc *SmallbankClassifier) Predict(partAvg float64, partSkew float64, partLenSkew float64, recAvg float64, hitRate float64, readRate float64, confRate float64) int {
	cPartAvg := C.double(partAvg)
	cPartSkew := C.double(partSkew)
	cPartLenSkew := C.double(partLenSkew)
	cRecAvg := C.double(recAvg)
	cHitRate := C.double(hitRate)
	cReadRate := C.double(readRate)
	cConfRate := C.double(confRate)

	c := C.SBPartPredict(sbc.partClf, cPartAvg, cPartSkew, cPartLenSkew, cRecAvg, cHitRate, cReadRate)
	if c < 0 {
		clog.Error("Smallbank Part Predict Error")
	}
	if c != 0 {
		c = C.SBOCCPredict(sbc.occClf, cRecAvg, cHitRate, cReadRate, cConfRate)
		if c < 0 {
			clog.Error("Smallbank OCC Predict Error")
		}
	}
	return int(c)
}

func (sbc *SmallbankClassifier) Finalize() {
	C.Final()
}
