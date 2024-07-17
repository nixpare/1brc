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

	"github.com/nixpare/mem"
)

const (
    BUFFER_SIZE = 2048 * 2048
    WORKERS_MULTIPLIER = 20
)

type WeatherStationInfo struct {
	name  string
	min   int16
	max   int16
	acc   int64
	count int
}

func (wsi *WeatherStationInfo) Compare(other *WeatherStationInfo) int {
    return strings.Compare(wsi.name/* .String() */, wsi.name/* .String() */)
}

const ARENA_ALLOC_STEP = 1024 * 1024 * 64

func main() {
    start := time.Now()

    mainArena := NewArena(ARENA_ALLOC_STEP)
    arenas := []*Arena{ mainArena }
    defer func() {
        for _, arena := range arenas {
            arena.Free()
        }
    }()

	if len(os.Args) < 3 {
		log.Fatalln("Required source and dest path")
	}

	out, err := os.Create(os.Args[2])
	if err != nil {
		log.Fatalln(err)
	}
	defer out.Close()

    inFilePath := os.Args[1]

    inInfo, err := os.Stat(inFilePath)
    if err != nil {
		log.Fatalln(err)
	}

    fileSize := inInfo.Size()
    workers := runtime.NumCPU() * WORKERS_MULTIPLIER

    if fileSize < BUFFER_SIZE {
        workers = 1
    }

    var chunkSize int64
    if workers > 1 {
        chunkSize = fileSize / int64(workers-1)
    } else {
        chunkSize = fileSize
    }

    partials := mem.NewSlice[mem.Slice[*WeatherStationInfo]](workers+1, workers+1, mainArena.AllocN)
    overflows := mem.NewSlice[mem.Slice[byte]](workers*2-2, workers*2-2, mainArena.AllocN)
    
    var wg sync.WaitGroup
    wg.Add(workers)
    for i := range workers {
        from := chunkSize*int64(i)
        to := from + chunkSize
        if to > fileSize {
            to = fileSize
        }

        arena := NewArena(ARENA_ALLOC_STEP)
        arenas = append(arenas, arena)

        go func() {
            defer wg.Done()
            partials[i] = compute(inFilePath, from, to, i, workers, arena, overflows)
        }()
    }
    wg.Wait()

    leftover := mem.NewSlice[byte](0, 128, mainArena.AllocN)

    for i := 0; i < len(overflows); i += 2 {
        leftover.Append(nil, mainArena.AllocN, overflows[i]...)
        leftover.Append(nil, mainArena.AllocN, overflows[i+1]...)
        leftover.Append(nil, mainArena.AllocN, '\n')
    }

    leftoverM := make(map[uint64]*WeatherStationInfo)
    h := fnv.New64a()
    
    computeChunk(leftover, h, leftoverM, mainArena)
    partials[len(partials)-1] = sortedValues(leftoverM, mainArena)

	result := mergeMatrix(partials, mainArena)
	printResult(out, result)

    end := time.Since(start)
	fmt.Println(end)
}

func compute(filePath string, from int64, to int64, workerID int, workers int, arena *Arena, overflows mem.Slice[mem.Slice[byte]]) mem.Slice[*WeatherStationInfo] {
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
    var leftoverStack [128]byte
    leftover := leftoverStack[:]
    leftover = leftover[:0]

    times := (to-from) / BUFFER_SIZE
    var read int

    for i := range times+1 {
        var size int64 = BUFFER_SIZE
        if i == times && int64(read) + BUFFER_SIZE > to - from {
            size = to - from - int64(read)
        }

        fBuf := buf[:size]
        n, err := f.Read(fBuf)
        if err != nil && !errors.Is(err, io.EOF) {
            panic(err)
        }
        read += n

        var firstLineIndex int
        for ; ; firstLineIndex++ {
            if buf[firstLineIndex] == '\n' {
                break
            }
        }

        if workerID != 0 && i == 0 {
            o := mem.NewSlice[byte](firstLineIndex, firstLineIndex, arena.AllocN)
            copy(o, buf[:firstLineIndex])

            overflows[workerID*2-1] = o
        } else {
            leftover = append(leftover, buf[:firstLineIndex]...)
            parseLine(leftover, h, m, arena)
            leftover = leftover[:0]
        }

        var lastLineIndex int
        for lastLineIndex = n-1 ; ; lastLineIndex-- {
            if buf[lastLineIndex] == '\n' {
                break
            }
        }

        if workerID != workers-1 && i == times {
            o := mem.NewSlice[byte](len(buf) - lastLineIndex+1, len(buf) - lastLineIndex+1, arena.AllocN)
            copy(o, buf[lastLineIndex+1:])
            
            overflows[workerID*2] = o
        } else {
            leftover = append(leftover, buf[lastLineIndex+1:]...)
        }

        computeChunk(buf[firstLineIndex+1:lastLineIndex+1], h, m, arena)
    }
    
    return sortedValues(m, arena)
}

func sortedValues(m map[uint64]*WeatherStationInfo, arena *Arena) []*WeatherStationInfo {
    values := mem.NewSlice[*WeatherStationInfo](0, len(m), arena.AllocN)
    for _, value := range m {
        values.Append(nil, arena.AllocN, value)
    }
    slices.SortFunc(values, func(a, b *WeatherStationInfo) int {
        return a.Compare(b)
    })
    return values
}

func computeChunk(chunk []byte, h hash.Hash64, m map[uint64]*WeatherStationInfo, arena *Arena) {
    var nextStart int
    for i, b := range chunk {
        if b == '\n' {
            parseLine(chunk[nextStart:i], h, m, arena)
            nextStart = i+1
        }
    }
}

func parseLine(line []byte, h hash.Hash64, m map[uint64]*WeatherStationInfo, arena *Arena) {
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
            break loop
        default:
            temp += int16(line[i] - '0') * exp
            exp *= 10
        }
    }

    x, ok := m[nameHash]
    if !ok {
        wsi := mem.New[WeatherStationInfo](arena.Alloc)
        *wsi = WeatherStationInfo{
            //name: mem.StringFromGO(string(line[:splitIndex]), arena.AllocN),
            name: string(line[:splitIndex]),
            min: temp, max: temp,
            acc: int64(temp), count: 1,
        }
        m[nameHash] = wsi
    } else {
        if temp < x.min {
            x.min = temp
        }
        if temp > x.max {
            x.max = temp
        }
        x.acc += int64(temp)
        x.count++
    }
}

func printResult(out io.Writer, result []*WeatherStationInfo) {
	fmt.Fprint(out, "{\n")
	first := true

	for _, x := range result {
        if first {
			first = false
			fmt.Fprintf(out, "\t%s=%.1f/%.1f/%.1f", x.name, float32(x.min) / 10.0, float64(x.acc) / 10.0 / float64(x.count), float32(x.max) / 10.0)
		} else {
			fmt.Fprintf(out, ",\n\t%s=%.1f/%.1f/%.1f", x.name, float32(x.min) / 10.0, float64(x.acc) / 10.0 / float64(x.count), float32(x.max) / 10.0)
		}
	}
	fmt.Fprint(out, "\n}\n")
}
