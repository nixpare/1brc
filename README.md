# 1BRC by Pare
The computer is equiped with:
+ Ryzen 5 5600x 6 core 12 threads with a slight undervolt, increasing
sustained load frequency to 4.625 GHz. Cinebench R23 score of 11.400.
+ 16 GB of DDR4 3600 MHz Dual channel
+ Entire projec and files on SSD NVME Samsung 970 EVO Plus 1 TB
+ OS is Windows 11 Pro

## Current Leaderboard

### 1) Go -> 5.85 seconds (branch `develop` and `main`)
Reached 5.85s after a profile run, which ran in 6.25s. Go is as standard as it can be, version 1.22.4.

Memory usage: ~ 256 MB

Run procedure:
+ First run: `go build -o calc.exe && .\calc.exe ..\measurements-x.txt ..\result-x.txt profile`
+ Second run: `go build -o calc.exe && .\calc.exe ..\measurements-x.txt ..\result-x.txt`

### 2) Go -> 6.47 seconds (branch `manual-mem`)
Reached 6.47s after a profile run, which ran in 6.75s. Go is as standard as it can be, version 1.22.4, but every slice, string and
heap object was created and managed by an arena allocator based on GLibc `malloc`, profided by the package `github.com/nixpare/mem`.

Memory usage: ~ 1.7 GB

Run procedure:
+ First run: `go build -o calc.exe && .\calc.exe ..\measurements-x.txt ..\result-x.txt profile`
+ Second run: `go build -o calc.exe && .\calc.exe ..\measurements-x.txt ..\result-x.txt`

### 3) V Lang -> 6.50 seconds (branch `vlang`)
Always reaching close to 6.5s. V is version `0.4.6 4c30d35`, with garbage collector disabled and manual memory management
enabled, plus other optimizations. Everything was not allocated with arenas and was deallocated as soon as possible.

Memory usage: Undefined (TaskManager was showing ~ 12.5 MB during the entire run, which is impossible)

Run procedure:
+ Build step: `v -prod -cg -skip-unused -fast-math -cflags -march=native -gc none -manualfree -o calc.exe .`
+ Run: `.\calc.exe ..\measurements-x.txt ..\result-x.txt`

### 4) Go -> 6.69 seconds (branch `go-arena`)
Reached 6.69s after a profile run, which ran in 7.65s. Go is version 1.22.4 with the arena
experimental feature enabled. Every slice and heap object is created with the arena.

Memory usage: ~ 2 GB

Run procedure:
+ Setup: `$Env:GOEXPERIMENT="arenas"`
+ First run: `go build -o calc.exe && .\calc.exe ..\measurements-x.txt ..\result-x.txt profile`
+ Second run: `go build -o calc.exe && .\calc.exe ..\measurements-x.txt ..\result-x.txt`
