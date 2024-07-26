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

### 2) Go -> 6.8 seconds (branch `manual-mem`)
A lot of variation, between 6.8s and 7.8s. Go is as standard as it can be, version 1.22.4, but every slice, string and
heap object was created and managed by an arena allocator based on GLibc `malloc`, profided by the package `github.com/nixpare/mem`.

Memory usage: ~ 470 MB

No difference even with the profile run:
+ First run: `go build -o calc.exe && .\calc.exe ..\measurements-x.txt ..\result-x.txt profile`
+ Second run: `go build -o calc.exe && .\calc.exe ..\measurements-x.txt ..\result-x.txt`
