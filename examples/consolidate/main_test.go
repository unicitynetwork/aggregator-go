package main

import "testing"

func TestExampleRuns(t *testing.T) {
	for _, n := range []int{1, 4, 8} {
		if err := run(n, 2); err != nil {
			t.Fatalf("%d shards: %v", n, err)
		}
	}
}
