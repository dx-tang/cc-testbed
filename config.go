package testbed

// TPCC Pre-allocation
// Records Pre-allocated
const (
	ORDER_PER_ALLOC     = 1000
	ORDERLINE_PER_ALLOC = 10000
	HISTORY_PER_ALLOC   = 10000
)

// Index Pre-allocation
const (
	ORDER_INDEX_PER_ALLOC     = 200
	ORDER_SECINDEX_PER_ALLOC  = 200
	ORDERLINE_INDEX_PER_ALLOC = 200
	HISTORY_INDEX_PER_ALLOC   = 100 // History Bucket Number 256
	NEWORDER_INDEX_PER_ALLOC  = 10  // NewOrder Bucket Number warehouse*dist_count
)

// Index Pre-allocation for original data
const (
	ORDER_INDEX_ORIGINAL     = 20
	ORDERLINE_INDEX_ORIGINAL = 20
	HISTORY_INDEX_ORIGINAL   = 20
	NEWORDER_INDEX_ORIGINAL  = 20
)

// Entry Size for Customized HashTable
const (
	CAP_NEWORDER_ENTRY = 1000
	CAP_HISTORY_ENTRY  = 1000
	CAP_CUSTOMER_ENTRY = 5

	CAP_BUCKET_COUNT           = 2000
	CAP_ORDER_SEC_ENTRY        = 5
	CAP_ORDER_BUCKET_ENTRY     = 5
	CAP_ORDERLINE_BUCKET_ENTRY = 50
)

// Location for Classifier and corresponding data
const (
	CLASSIFERPATH    = "/home/totemtang/ACC/workspace/src/github.com/totemtang/cc-testbed/classifier"
	SINGLEPARTTRAIN  = "single-part-train.out"
	SINGLEOCCTRAIN   = "single-occ-train.out"
	SINGLEPURETRAIN  = "single-pure-train.out"
	SINGLEINDEXTRAIN = "single-index-train.out"
	SBPARTTRAIN      = "sb-part-train.out"
	SBOCCTRAIN       = "sb-occ-train.out"
	SBPURETRAIN      = "sb-pure-train.out"
	SBINDEXTRAIN     = "sb-index-train.out"
	TPCCPARTTRAIN    = "tpcc-part-train.out"
	TPCCOCCTRAIN     = "tpcc-occ-train.out"
	TPCCPURETRAIN    = "tpcc-pure-train.out"
	TPCCINDEXTRAIN   = "tpcc-index-train.out"
)

// Sample Configuration
const (
	HISTOGRAMLEN = 100
	CACHESIZE    = 1000
	BUFSIZE      = 5
	TRIAL        = 20
	RECSR        = 2000
)

// OCC waits on write locks or not
var occ_wait bool = true

// For DBx1000 version 2PL, use 2PL-waitdie or 2PL-nowait
var locking_wait bool = false

// Max waiters for DBx1000 2PL-waitdie
const (
	maxwaiters = 10
)

const (
	TRAINPART = iota
	TRAINOCCPART
	TRAINOCCPURE
	TRAININDEX
	TESTING
)

const (
	PCC = iota
	OCCPART
	LOCKPART
	OCCSHARE
	LOCKSHARE
	TOTALCC
)

const (
	HEAD = 0
)
