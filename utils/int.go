package utils

import "math/rand"

func RandInt(min, max int) int {
	if max <= 1 {
		return max
	}
	return min + rand.Intn(max-min)
}

func GetRandSlot(max int) int {
	if max == 1 {
		return 0
	} else {
		return rand.Intn(max - 1)
	}

	return 0
}
