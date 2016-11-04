package testbed

import (
	"flag"
)

// Number of Loaders for Index Partitioning
// Number of Mergers for Index Merging
var (
	NLOADERS = flag.Int("loaders", 1, "Number of Loaders")
	NMERGERS = flag.Int("mergers", 1, "Number of Mergers")
)

var NumPart = flag.Int("ncores", 2, "number of partitions; equals to the number of cores")
var SysType = flag.Int("sys", PARTITION, "System Type we will use")
var SpinLock = flag.Bool("spinlock", true, "Use spinlock or mutexlock")
var NoWait = flag.Bool("nw", true, "Use Waitdie or NoWait for 2PL")

var Hybrid = flag.Bool("hybrid", false, "Use 2PL/OCC Hybrid")

var (
	HOTREC  = 1000 * 32
	HOTBIT  = 1 << 31
	HOTMASK = HOTBIT - 1
)
