package util

import (
	"errors"
	"hash/fnv"
)

func GetHashId(input string) int {
	to_hash := []byte(input)
	hash := fnv.New32a()
	hash.Write(to_hash)
	hash_id := hash.Sum32()
	return int(hash_id)
}

func CheckKeyInFrontendRange(key string, keyRanges map[string]*KeyRange, addr string) bool {
	// Compute the id by hashing the string
	hash := GetHashId(key)
	kr := keyRanges[addr]
	return float32(hash) > kr.Min && float32(hash) <= kr.Max
}

// Should check which frontend this hash belongs to.
func FindPrimaryFrontendForKey(key string, keyRanges map[string]*KeyRange) (string, error) {
	// Compute the id by hashing the string
	hash := GetHashId(key)

	for a, kr := range keyRanges {
		if float32(hash) > kr.Min && float32(hash) <= kr.Max {
			// This address is responsible for this key.
			return a, nil
		}
	}

	return "", errors.New("Fatal Error: Could not have key to frontend.")
}

func FindPrimaryBackendForKey(key string, hashToAddrMap map[int]string, backendHashId []int) (string, error) {
	// Find the hash
	hash := GetHashId(key)
	// log.Printf("Key %s --> hash %d", key, hash)

	// Handle wrap around case
	if hash < backendHashId[0] || hash >= backendHashId[len(backendHashId)-1] {
		// log.Printf("wrapcase: using backend hash %d", backendHashId[0])

		// Retry Logic in the case that the identified backend is dead
		for i := 0; i < len(backendHashId); i++ {
			// Check the health of backend at index i
			is_crashed, err := CheckBackendCrashed(hashToAddrMap[backendHashId[i]])
			if is_crashed || err != nil {
				continue
			}

			return hashToAddrMap[backendHashId[i]], nil
		}

	}

	for i := 0; i < len(backendHashId); i++ {
		if hash > backendHashId[i] && hash <= backendHashId[i+1] {
			// log.Printf("using backend hash %d", backendHashId[i+1])

			// Retry Logic in the case that the identified backend is dead
			for j := 0; j < len(backendHashId); j++ {
				idx := GetIndexOnRing((i + 1 + j), len(backendHashId))
				// Check the health of backend (hashToAddrMap[backendHashId[idx]]) at index idx
				is_crashed, err := CheckBackendCrashed(hashToAddrMap[backendHashId[idx]])
				if !is_crashed && err == nil {
					return hashToAddrMap[backendHashId[idx]], nil
				}
			}
		}
	}

	return "", errors.New("Fatal Error: Could not map hash to a backend.")
}

func GetIndexOnRing(idx int, ringSize int) int {
	return ((idx + ringSize) % ringSize)
}
