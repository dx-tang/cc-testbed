package testbed

import (
	"log"
	"runtime/debug"
)

const (
	POINTPADDING = 8
)

type LockReqList struct {
	header *ReqEntry
	tailer *ReqEntry
}

// ReqEntry is an element of a linked list.
type ReqEntry struct {
	padding1 [PADDING]byte
	Next     *ReqEntry
	Prev     *ReqEntry
	id       int
	tid      TID
	reqType  int
	state    chan int
	padding2 [PADDING]byte
}

func NewLockReqList() *LockReqList {
	return &LockReqList{}
}

func (l *LockReqList) InsertBefore(newE *ReqEntry, afterE *ReqEntry) {
	if afterE == l.header {
		l.InsertHeader(newE)
	} else {
		beforeE := afterE.Prev
		beforeE.Next = newE
		newE.Prev = beforeE
		newE.Next = afterE
		afterE.Prev = newE
	}
}

func (l *LockReqList) InsertTailer(newE *ReqEntry) {
	if l.tailer == nil {
		l.tailer = newE
		l.header = newE
		newE.Next = nil
		newE.Prev = nil
	} else {
		l.tailer.Next = newE
		newE.Prev = l.tailer
		newE.Next = nil
		l.tailer = newE
	}
}

func (l *LockReqList) InsertHeader(newE *ReqEntry) {
	if l.header == nil {
		l.header = newE
		l.tailer = newE
		newE.Next = nil
		newE.Prev = nil
	} else {
		l.header.Prev = newE
		newE.Next = l.header
		newE.Prev = nil
		l.header = newE
	}
}

func (l *LockReqList) Header() *ReqEntry {
	return l.header
}

func (l *LockReqList) Tailer() *ReqEntry {
	return l.tailer
}

func (l *LockReqList) RemoveHeader() *ReqEntry {
	ret := l.header
	l.header = ret.Next
	if l.header != nil {
		l.header.Prev = nil
	} else {
		l.tailer = nil
	}
	return ret
}

func (l *LockReqList) Remove(oldE *ReqEntry) {
	if oldE == l.header {
		l.header = oldE.Next
		if l.header != nil {
			l.header.Prev = nil
		} else {
			l.tailer = nil
		}
	} else if oldE == l.tailer {
		l.tailer = oldE.Prev
		if l.tailer != nil {
			l.tailer.Next = nil
		} else {
			l.header = nil
		}
	} else {
		beforeOld := oldE.Prev
		afterOld := oldE.Next
		beforeOld.Next = afterOld
		afterOld.Prev = beforeOld
	}
}

var globalBuf []LockReqBuffer

const (
	maxwaiters = 10
)

func allocEntry(id int) *ReqEntry {
	return globalBuf[id].AllocateReqEntry()
}

func releaseElem(id int, e *ReqEntry) {
	globalBuf[id].ReleaseReqEntry(e)
}

func InitGlobalBuffer() {
	// Initilize GlobleBuf
	globalBuf = make([]LockReqBuffer, *NumPart)
}

func InitLockReqBuffer(partIndex int) {
	globalBuf[partIndex].head = 0
	globalBuf[partIndex].tail = -1
	globalBuf[partIndex].size = maxwaiters
	globalBuf[partIndex].capacity = maxwaiters
	globalBuf[partIndex].buf = make([]*ReqEntry, maxwaiters+2*POINTPADDING)
	globalBuf[partIndex].id = partIndex

	globalBuf[partIndex].buf = globalBuf[partIndex].buf[POINTPADDING : POINTPADDING+maxwaiters]

	//listBuf.buf = listBuf.buf[POINTPADDING : POINTPADDING+size]
	for i := 0; i < maxwaiters; i++ {
		globalBuf[partIndex].buf[i] = &ReqEntry{
			state: make(chan int, 1),
			id:    partIndex,
		}
	}
}

type LockReqBuffer struct {
	padding1 [PADDING]byte
	head     int
	tail     int
	capacity int
	size     int
	id       int
	buf      []*ReqEntry
	padding2 [PADDING]byte
}

func NewLockReqBuffer(size int, id int) *LockReqBuffer {
	listBuf := &LockReqBuffer{
		head:     0,
		tail:     -1,
		size:     size,
		capacity: size,
		buf:      make([]*ReqEntry, size+2*POINTPADDING),
		id:       id,
	}

	listBuf.buf = listBuf.buf[POINTPADDING : POINTPADDING+size]
	for i := 0; i < size; i++ {
		listBuf.buf[i] = &ReqEntry{
			state: make(chan int, 1),
			id:    id,
		}
	}
	return listBuf
}

func (leBuf *LockReqBuffer) ReleaseReqEntry(e *ReqEntry) {
	if leBuf.size == leBuf.capacity {
		debug.PrintStack()
		log.Fatalln("Buffer Full; Release Not Allowed")
	}
	leBuf.tail = (leBuf.tail + 1) % leBuf.capacity
	leBuf.size++
	leBuf.buf[leBuf.tail] = e
	//log.Printf("Released %v %v", leBuf.id, leBuf.size)
}

func (leBuf *LockReqBuffer) AllocateReqEntry() *ReqEntry {
	if leBuf.size == 0 {
		debug.PrintStack()
		log.Fatalln("Buffer Empty; Allocate Not Allowed")
	}
	retElem := leBuf.buf[leBuf.head]
	leBuf.head = (leBuf.head + 1) % leBuf.capacity
	leBuf.size--
	//log.Printf("Allocated %v %v", leBuf.id, leBuf.size)
	return retElem
}
