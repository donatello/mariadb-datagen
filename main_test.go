package main

import "testing"

func BenchmarkGenData(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = GenData(2040)
	}
}
