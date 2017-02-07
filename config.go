package testbed

// TPCC Table Size
const (
	WAREHOUSESIZE_PER_WAREHOUSE = 1
	DISTRICTSIZE_PER_WAREHOUSE  = 10
	CUSTOMERSIZE_PER_WAREHOUSE  = 30000
	STOCKSIZE_PER_WAREHOUSE     = 100000
	ITEMSIZE                    = 100000
)

// Index allocation for loading
const (
	ORDER_INDEX_LOADING        = 200
	ORDER_SECINDEX_PER_LOADING = 200
	ORDERLINE_INDEX_LOADING    = 200
	NEWORDER_INDEX_LOADING     = 200
)

// TPCC Pre-allocation
// Records Pre-allocated
const (
	ORDER_PER_ALLOC     = 700000
	ORDERLINE_PER_ALLOC = 7000000
	HISTORY_PER_ALLOC   = 3000000
)

// Index Pre-allocation
const (
	ORDER_INDEX_PER_ALLOC     = 100000
	ORDER_SECINDEX_PER_ALLOC  = 100000
	ORDERLINE_INDEX_PER_ALLOC = 100000
	HISTORY_INDEX_PER_ALLOC   = 3000  // History Bucket Number 256
	NEWORDER_INDEX_PER_ALLOC  = 800 // NewOrder Bucket Number warehouse*dist_count
)

// Index Pre-allocation for original data
const (
        ORDER_INDEX_ORIGINAL     = 200000
        ORDERLINE_INDEX_ORIGINAL = 200000
        HISTORY_INDEX_ORIGINAL   = 20
        NEWORDER_INDEX_ORIGINAL  = 1600
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
	CLASSIFERPATH    = "/data/totemtang/ACC/workspace/src/github.com/totemtang/cc-testbed/classifier"
	SINGLEPARTTRAIN  = "single-pcc-train.out"
	SINGLEOCCTRAIN   = "single-occ-train.out"
	SINGLEPURETRAIN  = "single-pure-train.out"
	SINGLEINDEXTRAIN = "single-index-train.out"
	SBPARTTRAIN      = "sb-pcc-train.out"
	SBOCCTRAIN       = "sb-occ-train.out"
	SBPURETRAIN      = "sb-pure-train.out"
	SBINDEXTRAIN     = "sb-index-train.out"
	TPCCPARTTRAIN    = "tpcc-pcc-train.out"
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

// Use Mediated Switch or not
var mediated_switch bool = true

// For DBx1000 version 2PL, use 2PL-waitdie or 2PL-nowait
var locking_wait bool = false

// Max waiters for DBx1000 2PL-waitdie
const (
	maxwaiters = 10
)

const (
        TRAINPCC = iota
        TRAINOCC
        TESTING
)

const (
        PCC = iota
        OCCPART
        LOCKPART
        TOTALCC
)

const (
        HEAD        = 32
        PARTVARRATE = 100
)
