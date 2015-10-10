package testbed

type Partitioner interface {
	GetPartition(key Key) int
}

type HashPartitioner struct {
	NParts int64
}

func (hp *HashPartitioner) GetPartition(key Key) int {
	intKey := int64(key[0]) + int64(key[1])<<8 + int64(key[2])<<16 + int64(key[3])<<24 + int64(key[4])<<32 + int64(key[5])<<40 + int64(key[6])<<48 + int64(key[7])<<56
	ret := (intKey % hp.NParts)
	return int(ret)
}
