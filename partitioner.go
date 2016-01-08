package testbed

type Partitioner interface {
	GetPartition(key Key) int
	GetPartitionN(key Key) int
	GetKey(partIndex int, rank int64) Key
	GetRank(key Key) int64
}

type HashPartitioner struct {
	padding1 [64]byte
	NParts   int64
	NKeys    int64
	padding2 [64]byte
}

func (hp *HashPartitioner) GetPartition(key Key) int {
	k := int64(key)
	ret := k % hp.NParts
	return int(ret)
}

func (hp *HashPartitioner) GetKey(partIndex int, rank int64) Key {
	p := int64(partIndex)
	return Key(rank*hp.NParts + p)
}

func (hp *HashPartitioner) GetPartitionN(key Key) int {
	k := int64(key)
	ret := k % hp.NParts
	return int(ret)
}

func (hp *HashPartitioner) GetRank(key Key) int64 {
	k := int64(key)
	return k / hp.NParts
}
