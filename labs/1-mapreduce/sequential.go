package mr

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Sequential runs the job in one goroutine with no coordinator: the
// reference answer the distributed version must match.
func Sequential(files []string, mapf MapFunc, reducef ReduceFunc) (map[string]string, error) {
	var kvs []KeyValue
	for _, f := range files {
		content, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, mapf(f, string(content))...)
	}
	groups := map[string][]string{}
	for _, kv := range kvs {
		groups[kv.Key] = append(groups[kv.Key], kv.Value)
	}
	out := map[string]string{}
	for k, vs := range groups {
		out[k] = reducef(k, vs)
	}
	return out, nil
}

// ReadOutput merges every mr-out-* file in dir into one map.
func ReadOutput(dir string) (map[string]string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "mr-out-*"))
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, name := range files {
		if strings.Contains(name, ".tmp-") {
			continue
		}
		f, err := os.Open(name)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			key, value, _ := strings.Cut(sc.Text(), " ")
			if _, dup := out[key]; dup {
				f.Close()
				return nil, fmt.Errorf("key %q appears in more than one output", key)
			}
			out[key] = value
		}
		f.Close()
	}
	return out, nil
}

// Compare returns an error describing the first difference between two
// outputs.
func Compare(got, want map[string]string) error {
	if len(got) != len(want) {
		return fmt.Errorf("got %d keys, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			return fmt.Errorf("key %q: got %q, want %q", k, got[k], v)
		}
	}
	return nil
}

var vocabulary = strings.Fields(`
	the a of to and in is it that for on with as was by be at this not are or
	from but have an they which one you were all we her she there would their
	go raft leader follower candidate term vote log entry commit apply snapshot
	shard group config client server request reply timeout crash partition
	network message map reduce worker coordinator task file key value append
	lock mutex channel goroutine thread replica majority quorum consensus
	election heartbeat persist restart retry duplicate linearizable consistent
`)

// GenerateInputs writes nFiles text files of random words to dir, with a
// skewed (Zipf) word frequency like real text, and returns their paths.
func GenerateInputs(dir string, nFiles, wordsPerFile int, seed int64) ([]string, error) {
	r := rand.New(rand.NewSource(seed))
	zipf := rand.NewZipf(r, 1.2, 1, uint64(len(vocabulary)-1))
	var files []string
	for i := 0; i < nFiles; i++ {
		var b strings.Builder
		for w := 0; w < wordsPerFile; w++ {
			b.WriteString(vocabulary[zipf.Uint64()])
			if w%12 == 11 {
				b.WriteString(".\n")
			} else {
				b.WriteString(" ")
			}
		}
		name := filepath.Join(dir, fmt.Sprintf("input-%d.txt", i))
		if err := os.WriteFile(name, []byte(b.String()), 0o644); err != nil {
			return nil, err
		}
		files = append(files, name)
	}
	return files, nil
}

// TopWords returns the n keys with the largest integer values.
func TopWords(out map[string]string, n int) []KeyValue {
	var kvs []KeyValue
	for k, v := range out {
		kvs = append(kvs, KeyValue{k, v})
	}
	sort.Slice(kvs, func(i, j int) bool {
		var a, b int
		fmt.Sscan(kvs[i].Value, &a)
		fmt.Sscan(kvs[j].Value, &b)
		if a != b {
			return a > b
		}
		return kvs[i].Key < kvs[j].Key
	})
	return kvs[:min(n, len(kvs))]
}
