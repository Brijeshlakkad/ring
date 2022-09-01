package ring

func binarySearchUint64(list []uint64, left int, right int, target uint64) int {
	for left < right {
		mid := (left + right) / 2
		if list[mid] == target {
			return mid
		} else if list[mid] > target {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return left
}
