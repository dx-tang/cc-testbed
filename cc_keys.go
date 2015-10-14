package testbed

func CKey(x int64) Key {
	var b [16]byte
	var i uint64
	for i = 0; i < 8; i++ {
		b[i] = byte(x >> (i * 8))
	}
	return Key(b)
}

func ParseKey(key Key) int64 {
	intKey := int64(key[0]) + int64(key[1])<<8 + int64(key[2])<<16 + int64(key[3])<<24 + int64(key[4])<<32 + int64(key[5])<<40 + int64(key[6])<<48 + int64(key[7])<<56
	return intKey
}
