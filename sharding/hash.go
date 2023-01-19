package sharding

import "hash/crc32"

func str2key(measurement string) uint64 {
	n := crc32.ChecksumIEEE([]byte(measurement))
	return uint64(n)
}

func jumpHashStr(key string, buckets int32) int32 {
	n := crc32.ChecksumIEEE([]byte(key))
	return jumpHash(uint64(n), buckets)
}

// Hash takes a 64 bit key and the number of buckets. It outputs a bucket
// number in the range [0, buckets).
// If the number of buckets is less than or equal to 0 then one 1 is used.
func jumpHash(key uint64, buckets int32) int32 {
	var b, j int64

	if buckets <= 0 {
		buckets = 1
	}

	for j < int64(buckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}

func jumpHashForReplica(key uint64, replica int, numBuckets int32) []int32 {
	// error instead of panic?
	if int32(replica) >= numBuckets {
		panic("jump: invalid replica")
	}

	buckets := make([]int32, numBuckets)
	for i := range buckets {
		buckets[i] = int32(i)
	}

	replicas := make([]int32, replica+1)

	for i := 0; i <= replica; i++ {
		b := jumpHash(key, int32(len(buckets)))

		replicas[i] = buckets[b]

		buckets[b], buckets = buckets[len(buckets)-1], buckets[:len(buckets)-1]

		key = xorshiftMult64(key)
	}

	return replicas
}

// 64-bit xorshift multiply rng from http://vigna.di.unimi.it/ftp/papers/xorshift.pdf
func xorshiftMult64(x uint64) uint64 {
	x ^= x >> 12 // a
	x ^= x << 25 // b
	x ^= x >> 27 // c
	return x * 2685821657736338717
}
