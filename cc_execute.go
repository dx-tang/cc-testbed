package testbed

//import (
//	"github.com/totemtang/cc-testbed/clog"
//)

type ETransaction interface {
	Reset()
	Read(k Key, partNum int) (*BRecord, error)
	WriteInt64(k Key, intValue int64, partNum int) error
	WriteString(k Key, sa *StrAttr, partNum int) error
	Abort() TID
	Commit() TID
	Store() *Store
	Worker() *Worker
}

// Partition Transaction Implementation
type PTransaction struct {
	padding0 [128]byte
	w        *Worker
	s        *Store
	padding  [128]byte
}

func StartPTransaction(w *Worker) *PTransaction {
	tx := &PTransaction{
		w: w,
		s: w.store,
	}
	return tx
}

func (p *PTransaction) Reset() {

}

func (p *PTransaction) Read(k Key, partNum int) (*BRecord, error) {
	br := p.s.GetRecord(k, partNum)
	if br == nil {
		return nil, ENORETRY
	}
	return br, nil
}

func (p *PTransaction) WriteInt64(k Key, intValue int64, partNum int) error {
	s := p.s
	success := s.SetRecord(k, intValue, partNum)
	if !success {
		return ENORETRY
	}
	return nil
}

func (p *PTransaction) WriteString(k Key, sa *StrAttr, partNum int) error {
	s := p.s
	success := s.SetRecord(k, sa, partNum)
	if !success {
		return ENORETRY
	}
	return nil
}

func (p *PTransaction) Abort() TID {
	return 0
}

func (p *PTransaction) Commit() TID {
	return 1
}

func (p *PTransaction) Store() *Store {
	return p.s
}

func (p *PTransaction) Worker() *Worker {
	return p.w
}
