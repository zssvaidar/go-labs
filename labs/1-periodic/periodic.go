// Package periodic is lab 1: cancelling a periodic goroutine with a
// mutex-protected flag.
package periodic

import (
	"sync"
	"time"
)

var done bool
var mu sync.Mutex

// Run starts a ticker goroutine, lets it tick for a while, then cancels it.
func Run() {
	time.Sleep(1 * time.Second)
	println("started")
	go periodic()
	time.Sleep(5 * time.Second) // wait for a while so we can observe what ticker does
	mu.Lock()
	done = true
	mu.Unlock()
	println("cancelled")
	time.Sleep(3 * time.Second) // observe no output
}

func periodic() {
	for !killed() {
		println("tick")
		time.Sleep(1 * time.Second)
	}
}

// killed reads done under the lock so the write in Run is visible here
// without a data race.
func killed() bool {
	mu.Lock()
	defer mu.Unlock()
	return done
}
