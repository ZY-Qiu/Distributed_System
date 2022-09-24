package main

import (
	"strings"
	"golang.org/x/tour/wc"
)

func WordCount(s string) map[string]int {
	m := make(map[string]int)
	ss := strings.Fields(s)
	for _, str := range ss {
		m[str] += 1
	}
	return m
}

func main() {
	wc.Test(WordCount)
}
