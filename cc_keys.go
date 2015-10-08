package testbed

func CKey(x int64) Key {
	var b [16]byte
	var i uint64
	for i = 0; i < 8; i++ {
		b[i] = byte(x >> (i * 8))
	}
	return Key(b)
}
