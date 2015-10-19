package testbed

import (
	"fmt"
	"os"
)

type Coordinator struct {
	padding0 [128]byte
	Workers  []*Worker
	store    *Store
	NStats   []int64
	padding1 [128]byte
}

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
	}
}

func (coord *Coordinator) PrintStats(f *os.File) {
	coord.gatherStats()
	f.WriteString("================\n")
	f.WriteString("Print Statistics\n")
	f.WriteString("================\n")
	f.WriteString(fmt.Sprintf("Issue %v Transactions\n", coord.NStats[NTXN]))
	f.WriteString(fmt.Sprintf("Abort %v Transactions\n", coord.NStats[NABORTS]))
	f.WriteString(fmt.Sprintf("Cross Partition %v Transactions\n", coord.NStats[NCROSSTXN]))
	f.WriteString(fmt.Sprintf("Error of No Key %v Transactions\n", coord.NStats[NENOKEY]))
	f.WriteString(fmt.Sprintf("Read %v Keys\n", coord.NStats[NREADKEYS]))
	f.WriteString(fmt.Sprintf("Write %v Keys\n", coord.NStats[NWRITEKEYS]))

	var incrtotal int64
	for _, part := range coord.store.store {
		for _, chunk := range part.data {
			for _, br := range chunk.rows {
				incrtotal += br.intVal
			}
		}
	}

	f.WriteString(fmt.Sprintf("Increments %v in total\n", incrtotal))

	f.WriteString("\n")
}
