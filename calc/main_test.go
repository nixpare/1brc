package main

import (
	"os"
	"testing"
)

func BenchmarkMain(b *testing.B) {
    oldArgs := os.Args
    os.Args = os.Args[len(os.Args)-3:]
    defer func() { os.Args = oldArgs }()

    for range b.N {
        main()
    }
}
