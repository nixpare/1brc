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
	"strings"
	"sync"
	"time"

	"github.com/nixpare/mem"
	"github.com/nixpare/sorting"
)

const (
    BUFFER_SIZE = 1024 * 1024 * 4
    WORKERS_MULTIPLIER = 1//20

    ARENA_SINGLE_ALLOC = 1024 * 256
    //ARENA_MULTI_ALLOC = 1024 * 1024 * 8
    ARENA_MAIN_MULTI_ALLOC = 1024 * 1024 * 512
)

type WeatherStationInfo struct {
	name  mem.String
	min   int16
	max   int16
	acc   int64
	count int
}

func (wsi *WeatherStationInfo) Compare(other *WeatherStationInfo) int {
    return strings.Compare(string(wsi.name), string(other.name))
}

func main() {
    start := time.Now()

    arena := NewArena(ARENA_SINGLE_ALLOC, ARENA_MAIN_MULTI_ALLOC)
    defer func() {
        fmt.Printf("%d %d\n", len(arena.single), len(arena.multi))
        arena.Free()
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

    partials := mem.NewSlice[mem.Slice[*WeatherStationInfo]](workers+1, workers+1, arena.AllocN)
    overflows := mem.NewSlice[mem.Slice[byte]](workers*2-2, workers*2-2, arena.AllocN)
    
    var wg sync.WaitGroup
    wg.Add(workers)
    for i := range workers {
        from := chunkSize*int64(i)
        to := from + chunkSize
        if to > fileSize {
            to = fileSize
        }

        go func() {
            defer wg.Done()
            partials[i] = compute(inFilePath, from, to, i, workers, arena, overflows)
        }()
    }
    wg.Wait()

    leftover := mem.NewSlice[byte](0, 128, arena.AllocN)

    for i := 0; i < len(overflows); i += 2 {
        leftover.Append(nil, arena.AllocN, overflows[i]...)
        leftover.Append(nil, arena.AllocN, overflows[i+1]...)
        leftover.Append(nil, arena.AllocN, '\n')
    }

    leftoverM := make(map[uint64]*WeatherStationInfo)
    h := fnv.New64a()
    
    computeChunk(leftover, h, leftoverM, arena)
    partials[len(partials)-1] = sortedValues(leftoverM, arena)

	result := mergeMatrix(mem.ToSliceMatrix(partials), arena)
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

    buf := mem.NewSlice[byte](BUFFER_SIZE, BUFFER_SIZE, arena.AllocN)
    leftover := mem.NewSlice[byte](0, 128, arena.AllocN)

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

    sorting.Sort(values)
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

    wsi, ok := m[nameHash]
    if !ok {
        name := string(line[:splitIndex])
        wsi = mem.New[WeatherStationInfo](arena.Alloc)

        *wsi = WeatherStationInfo{
            name: mem.StringFromGO(name, arena.AllocN),
            min: temp, max: temp,
            acc: int64(temp), count: 1,
        }

        m[nameHash] = wsi
    } else {
        if temp < wsi.min {
            wsi.min = temp
        }
        if temp > wsi.max {
            wsi.max = temp
        }
        wsi.acc += int64(temp)
        wsi.count++
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
