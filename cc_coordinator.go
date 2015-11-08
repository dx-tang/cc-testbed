package testbed

import (
	"fmt"
	"os"
	"time"
)

type Coordinator struct {
	padding0     [128]byte
	Workers      []*Worker
	store        *Store
	NStats       []int64
	NGen         time.Duration
	NExecute     time.Duration
	NWait        time.Duration
	NLockAcquire int64
	padding1     [128]byte
}

const (
	PERSEC = 1000000000
)

func NewCoordinator(nWorkers int, store *Store) *Coordinator {
	coordinator := &Coordinator{
		Workers: make([]*Worker, nWorkers),
		store:   store,
		NStats:  make([]int64, LAST_STAT),
	}

	for i := range coordinator.Workers {
		coordinator.Workers[i] = NewWorker(i, store)
	}

	return coordinator
}

func (coord *Coordinator) gatherStats() {
	for _, worker := range coord.Workers {
		coord.NStats[NABORTS] += worker.NStats[NABORTS]
		coord.NStats[NENOKEY] += worker.NStats[NENOKEY]
		coord.NStats[NTXN] += worker.NStats[NTXN]
		coord.NStats[NCROSSTXN] += worker.NStats[NCROSSTXN]
		coord.NStats[NREADKEYS] += worker.NStats[NREADKEYS]
		coord.NStats[NWRITEKEYS] += worker.NStats[NWRITEKEYS]
		coord.NGen += worker.NGen
		coord.NExecute += worker.NExecute
		coord.NWait += worker.NWait
		coord.NLockAcquire += worker.NLockAcquire
	}
}

func (coord *Coordinator) PrintStats(f *os.File) {
	coord.gatherStats()

	f.WriteString("================\n")
	f.WriteString("Print Statistics\n")
	f.WriteString("================\n")
	f.WriteString(fmt.Sprintf("Issue %v Transactions in Total\n", coord.NStats[NTXN]))
	f.WriteString(fmt.Sprintf("Abort %v Transactions\n", coord.NStats[NABORTS]))
	f.WriteString(fmt.Sprintf("Cross Partition %v Transactions\n", coord.NStats[NCROSSTXN]))
	f.WriteString(fmt.Sprintf("Error of No Key %v Transactions\n", coord.NStats[NENOKEY]))

	for i, worker := range coord.Workers {
		f.WriteString(fmt.Sprintf("Worker %v Issue %v Transactions\n", i, worker.NStats[NTXN]))
		f.WriteString(fmt.Sprintf("Worker %v Issue %v Cross Transactions\n", i, worker.NStats[NCROSSTXN]))
		f.WriteString(fmt.Sprintf("Worker %v Spends %v secs\n", i, float64(worker.NExecute.Nanoseconds())/float64(PERSEC)))
		f.WriteString(fmt.Sprintf("Worker %v Waits %v secs\n", i, float64(worker.NWait.Nanoseconds())/float64(PERSEC)))
		f.WriteString(fmt.Sprintf("Worker %v Crosswaits %v secs\n", i, float64(worker.NCrossWait.Nanoseconds())/float64(PERSEC)))
	}

	/*
		var sum int64
		for i := int64(0); i < coord.store.nKeys; i++ {
			k := Key(i)
			rec := coord.store.GetRecord(k, 0)
			sum += rec.Value().(int64)
		}

		f.WriteString(fmt.Sprintf("Total Value %v\n", sum))
	*/

	f.WriteString(fmt.Sprintf("Read %v Keys\n", coord.NStats[NREADKEYS]))
	f.WriteString(fmt.Sprintf("Write %v Keys\n", coord.NStats[NWRITEKEYS]))

	f.WriteString(fmt.Sprintf("Transaction Generation Spends %v secs\n", float64(coord.NGen.Nanoseconds())/float64(PERSEC)))
	f.WriteString(fmt.Sprintf("Transaction Processing Spends %v secs\n", float64(coord.NExecute.Nanoseconds())/float64(PERSEC)))
	f.WriteString(fmt.Sprintf("Transaction Waiting Spends %v secs\n", float64(coord.NWait.Nanoseconds())/float64(PERSEC)))

	f.WriteString(fmt.Sprintf("Has Acquired %v Locks\n", coord.NLockAcquire))

	f.WriteString("\n")

}
