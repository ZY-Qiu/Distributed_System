package main

import "sync"

var a string
var done bool

func setup(mu sync.Mutex, cond *sync.Cond) {
	mu.Lock()
	a = "hello, world"
	done = true
	cond.Broadcast()
	mu.Unlock()
}

func main() {
	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)
	go setup(mu, cond)
	mu.Lock()
	for !done {
		cond.Wait()
	}
	mu.Unlock()
	print(a)
}
