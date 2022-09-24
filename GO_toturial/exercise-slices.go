package main

import "golang.org/x/tour/pic"

func Pic(dx, dy int) [][]uint8 {
	var ans = make([][]uint8, dy)
	for i := 0; i < dy; i++ {
		ans[i] = make([]uint8, dx)
		for j := 0; j < dx; j++ {
			ans[i][j] = uint8(i*j)
		}
	}
	return ans
}

func main() {
	pic.Show(Pic)
}
