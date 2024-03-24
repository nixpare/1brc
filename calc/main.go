package main

import (
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/maps"
)

type WeatherStationInfo struct {
	name  string
	min   int16
	max   int16
	acc   int64
	count int
}

func main() {
	start := time.Now()

	if len(os.Args) < 3 {
		log.Fatalln("Required source and dest path")
	}

	out, err := os.Create(os.Args[len(os.Args)-1])
	if err != nil {
		log.Fatalln(err)
	}
	defer out.Close()

    inFilePath := os.Args[len(os.Args)-2]

    inInfo, err := os.Stat(inFilePath)
    if err != nil {
		log.Fatalln(err)
	}

    fileSize := inInfo.Size()
    workers := runtime.NumCPU()
    var chunkSize int64
    if workers > 1 {
        chunkSize = fileSize / int64(workers-1)
    } else {
        chunkSize = fileSize
    }

    var wg sync.WaitGroup
    partials := make([][]*WeatherStationInfo, workers+1)
    overflows := make([][]byte, workers*2-2)
    wg.Add(workers)

    for i := range workers {
        from := chunkSize*int64(i)
        to := from + chunkSize
        if to > fileSize {
            to = fileSize
        }

        go func() {
            defer wg.Done()
            partials[i] = compute(inFilePath, from, to, i, workers, overflows)
        }()
    }
    wg.Wait()

    leftover := make([]byte, 0)
    for i := 0; i < len(overflows); i += 2 {
        leftover = append(leftover, overflows[i]...)
        leftover = append(leftover, overflows[i+1]...)
        leftover = append(leftover, '\n')
    }

    leftoverM := make(map[uint64]*WeatherStationInfo)
    h := fnv.New64a()
    computeChunk(leftover, h, leftoverM)
    partials[len(partials)-1] = sortedValues(leftoverM)

	result := mergeSortMulti(partials)
	printResult(out, result)

	fmt.Println(time.Since(start))
}

const BUFFER_SIZE = 2048 * 2048

func compute(filePath string, from int64, to int64, workerID int, workers int, overflows [][]byte) []*WeatherStationInfo {
    if from == to {
        return nil
    }

    m := make(map[uint64]*WeatherStationInfo)
    h := fnv.New64a()
    
    f, err := os.OpenFile(filePath, os.O_RDONLY, 0)
    if err != nil {
        panic(err)
    }
    defer f.Close()

    _, err = f.Seek(from, io.SeekStart)
    if err != nil {
        panic(err)
    }

    var buf [BUFFER_SIZE]byte
    leftover := make([]byte, 0, 128)

    times := (to-from) / BUFFER_SIZE
    var read int

    for i := range times+1 {
        var size int64 = BUFFER_SIZE
        if i == times && int64(read) + BUFFER_SIZE > to - from {
            size = to - from - int64(read)
        }

        n, err := f.Read(buf[:size])
        if err != nil && !errors.Is(err, io.EOF) {
            panic(err)
        }
        read += n

        var firstLineIndex int
        for ; firstLineIndex < n ; firstLineIndex++ {
            if buf[firstLineIndex] == '\n' {
                break
            }
        }

        if workerID != 0 && i == 0 {
            copy(overflows[workerID*2-1], buf[:firstLineIndex])
        } else {
            leftover = append(leftover, buf[:firstLineIndex]...)
            parseLine(leftover, h, m)
            leftover = leftover[:0]
        }

        var lastLineIndex int = n-1
        for ; lastLineIndex > firstLineIndex ; lastLineIndex-- {
            if buf[lastLineIndex] == '\n' {
                break
            }
        }

        if workerID != workers-1 && i == times {
            copy(overflows[workerID*2], buf[lastLineIndex+1:])
        } else {
            leftover = append(leftover, buf[lastLineIndex+1:]...)
        }

        computeChunk(buf[firstLineIndex+1:lastLineIndex+1], h, m)
    }
    
    return sortedValues(m)
}

func sortedValues(m map[uint64]*WeatherStationInfo) []*WeatherStationInfo {
    values := maps.Values(m)
    slices.SortFunc(values, func(a *WeatherStationInfo, b *WeatherStationInfo) int {
        return strings.Compare(a.name, b.name)
    })
    return values
}

func computeChunk(chunk []byte, h hash.Hash64, m map[uint64]*WeatherStationInfo) {
    var nextStart int
    for i, b := range chunk {
        if b == '\n' {
            parseLine(chunk[nextStart:i], h, m)
            nextStart = i+1
        }
    }
}

func parseLine(line []byte, h hash.Hash64, m map[uint64]*WeatherStationInfo) {
    if len(line) == 0 {
        return
    }

    var splitIndex int
    for i, c := range line {
        if c == ';' {
            splitIndex = i
            break
        }
    }

    h.Reset()
    h.Write(line[:splitIndex])
    nameHash := h.Sum64()

    var temp int16
    var exp int16 = 1
loop:
    for i := len(line)-1; i > splitIndex; i-- {
        switch line[i] {
        case '.':
            continue loop
        case '-':
            temp *= -1
        case '0':
        case '1':
            temp += 1 * exp
        case '2':
            temp += 2 * exp
        case '3':
            temp += 3 * exp
        case '4':
            temp += 4 * exp
        case '5':
            temp += 5 * exp
        case '6':
            temp += 6 * exp
        case '7':
            temp += 7 * exp
        case '8':
            temp += 8 * exp
        case '9':
            temp += 9 * exp
        }
        
        exp *= 10
    }

    value, ok := m[nameHash]
    if !ok {
        m[nameHash] = &WeatherStationInfo{
            name: string(line[:splitIndex]),
            min: temp, max: temp,
            acc: int64(temp), count: 1,
        }
    } else {
        if temp < value.min {
            value.min = temp
        }
        if temp > value.max {
            value.max = temp
        }
        value.acc += int64(temp)
        value.count++
    }
}
