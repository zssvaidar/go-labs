package mr

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// Demo runs a word count over generated text with three workers. One
// worker crashes in the middle of its first task, so the coordinator has
// to notice and give that task to someone else.
func Demo() {
	must := func(err error) {
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	}
	dir, err := os.MkdirTemp("", "mr-demo-")
	must(err)
	defer os.RemoveAll(dir)

	files, err := GenerateInputs(dir, 8, 5000, 1)
	must(err)
	fmt.Printf("== Input: %d files of 5000 random words in %s\n", len(files), dir)

	const nReduce = 4
	c, err := MakeCoordinator(files, nReduce, time.Second)
	must(err)
	defer c.Close()
	fmt.Printf("== Coordinator: %d map tasks, %d reduce tasks, task timeout 1s\n", len(files), nReduce)

	start := time.Now()
	var wg sync.WaitGroup
	for w := 1; w <= 3; w++ {
		mapf := WcMap
		if w == 3 {
			crashed := false
			mapf = func(filename, contents string) []KeyValue {
				if !crashed {
					crashed = true
					fmt.Printf("  [%4dms] worker 3 crashes while mapping %s\n",
						time.Since(start).Milliseconds(), shortName(filename))
					panic(errCrash)
				}
				return WcMap(filename, contents)
			}
		}
		wg.Add(1)
		go func(w int, mapf MapFunc) {
			defer wg.Done()
			Worker(c.Sock(), dir, mapf, WcReduce)
			fmt.Printf("  [%4dms] worker %d exits\n", time.Since(start).Milliseconds(), w)
		}(w, mapf)
	}

	for !c.Done() {
		time.Sleep(50 * time.Millisecond)
	}
	wg.Wait()
	assigned, reissued := c.Stats()
	fmt.Printf("== Job done in %dms: %d tasks handed out, %d re-issued after a timeout\n",
		time.Since(start).Milliseconds(), assigned, reissued)

	got, err := ReadOutput(dir)
	must(err)
	fmt.Println("\n== Top 10 words:")
	for _, kv := range TopWords(got, 10) {
		fmt.Printf("  %-12s %s\n", kv.Key, kv.Value)
	}

	want, err := Sequential(files, WcMap, WcReduce)
	must(err)
	must(Compare(got, want))
	fmt.Printf("\n== Output matches a sequential run (%d distinct words)\n", len(got))
}

func shortName(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[i+1:]
		}
	}
	return path
}
