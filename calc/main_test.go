package main

import "testing"

func BenchmarkMain(b *testing.B) {
    for range b.N {
        main()
    }
}
