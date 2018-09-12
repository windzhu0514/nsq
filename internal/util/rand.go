package util

import (
	"math/rand"
)

// 随机获取quantity个0-maxval之间的数字
func UniqRands(quantity int, maxval int) []int {
	if maxval < quantity {
		quantity = maxval
	}

	intSlice := make([]int, maxval)
	for i := 0; i < maxval; i++ {
		intSlice[i] = i
	}

	// 每次把第i个和随机下标进行交换
	for i := 0; i < quantity; i++ {
		j := rand.Int()%maxval + i // +i保证随机下标不会比i小
		// swap
		intSlice[i], intSlice[j] = intSlice[j], intSlice[i]
		maxval-- // 减小每次的随机范围

	}
	return intSlice[0:quantity]
}
