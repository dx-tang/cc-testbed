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
	Commit(q *Query) TID
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
		return nil, ENOKEY
	}
	return br, nil
}

func (p *PTransaction) WriteInt64(k Key, intValue int64, partNum int) error {
	s := p.s
	success := s.SetRecord(k, intValue, partNum)
	if !success {
		return ENOKEY
	}
	return nil
}

func (p *PTransaction) WriteString(k Key, sa *StrAttr, partNum int) error {
	s := p.s
	success := s.SetRecord(k, sa, partNum)
	if !success {
		return ENOKEY
	}
	return nil
}

func (p *PTransaction) Abort() TID {
	return 0
}

func (p *PTransaction) Commit(q *Query) TID {
	//store := p.Store()
	//partitioner := q.partitioner
	//rwSets := q.rwSets

	/*for _, wkey := range rwSets.wKeys {
		store.GetRecord(wkey, partitioner.GetPartition(wkey))
		//br := store.GetRecord(wkey, partitioner.GetPartition(wkey))
		//clog.Info("Woker %v Key %v", p.w.ID, ParseKey(wkey))
		//br.Lock()
	}*/
	/*
		for i, rkey := range rwSets.rKeys {
			br := store.GetRecord(rkey, partitioner.GetPartition(rkey))
			if rwSets.rTIDs[i] != br.GetTID() {
				clog.Error("Confict")
			}
		}
	*/
	//retValue := rwSets.wValue.(*RetIntValue)

	/*for i, wkey := range rwSets.wKeys {
		//br := store.GetRecord(wkey, partitioner.GetPartition(wkey))
		err := p.WriteInt64(wkey, retValue.intVals[i], partitioner.GetPartition(wkey))
		if err != nil {
			clog.Error("Write Error")
		}
		//br.last = 1
		//store.GetRecord(wkey, partitioner.GetPartition(wkey))
		//br.Unlock()
	}*/

	return 1
}

func (p *PTransaction) Store() *Store {
	return p.s
}

func (p *PTransaction) Worker() *Worker {
	return p.w
}
