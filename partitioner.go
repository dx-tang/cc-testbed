package testbed

type Partitioner interface {
	GetPartition(key Key) int
	GetPartitionN(key int64) int
	GetKey(partIndex int, rank int64) int64
	GetRank(key int64) int64
}

type HashPartitioner struct {
	NParts int64
	NKeys  int64
}

func (hp *HashPartitioner) GetPartition(key Key) int {
	intKey := int64(key[0]) + int64(key[1])<<8 + int64(key[2])<<16 + int64(key[3])<<24 + int64(key[4])<<32 + int64(key[5])<<40 + int64(key[6])<<48 + int64(key[7])<<56
	ret := (intKey % hp.NParts)
	return int(ret)
}

func (hp *HashPartitioner) GetKey(partIndex int, rank int64) int64 {
	p := int64(partIndex)
	return rank*hp.NParts + p
}

func (hp *HashPartitioner) GetPartitionN(key int64) int {
	ret := (key % hp.NParts)
	return int(ret)
}

func (hp *HashPartitioner) GetRank(key int64) int64 {
	return key / hp.NParts
}
