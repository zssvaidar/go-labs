package mr

import (
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// Word count: how many times each word appears across all inputs.

func WcMap(filename string, contents string) []KeyValue {
	words := strings.FieldsFunc(contents, func(r rune) bool { return !unicode.IsLetter(r) })
	kvs := make([]KeyValue, 0, len(words))
	for _, w := range words {
		kvs = append(kvs, KeyValue{Key: w, Value: "1"})
	}
	return kvs
}

func WcReduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}

// Inverted index: for each word, which files contain it.

func IndexerMap(filename string, contents string) []KeyValue {
	words := strings.FieldsFunc(contents, func(r rune) bool { return !unicode.IsLetter(r) })
	seen := map[string]bool{}
	var kvs []KeyValue
	for _, w := range words {
		if !seen[w] {
			seen[w] = true
			kvs = append(kvs, KeyValue{Key: w, Value: filename})
		}
	}
	return kvs
}

func IndexerReduce(key string, values []string) string {
	sort.Strings(values)
	return strconv.Itoa(len(values)) + " " + strings.Join(values, ",")
}
