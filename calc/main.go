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
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/nixpare/sorting"
)

const (
    BUFFER_SIZE = 1024 * 1024
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
	return strings.Compare(wsi.name, other.name)
}

func main() {
	if len(os.Args) > 3 && os.Args[3] == "profile" {
		f, err := os.Create("default.pgo")
		if err != nil {
			log.Fatalln(err)
		}

		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatalln(err)
		}

		defer pprof.StopCPUProfile()
	}
	
	start := time.Now()

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

	partials := make([][]*WeatherStationInfo, workers+1)
	overflows := make([][]byte, workers*2-2)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := range workers {
		from := chunkSize * int64(i)
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

    leftover := make([]byte, 0, 128)
    for i := 0; i < len(overflows); i += 2 {
        leftover = append(leftover, overflows[i]...)
        leftover = append(leftover, overflows[i+1]...)
        leftover = append(leftover, '\n')
    }

	leftoverM := make(map[uint64]*WeatherStationInfo)
	h := fnv.New64a()

	computeChunk(leftover, h, leftoverM)
	partials[len(partials)-1] = sortedValues(leftoverM)

	result := mergeMatrix(partials)
	printResult(out, result)

	end := time.Since(start)
	fmt.Println(end)
}

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
	var _leftover [128]byte
	leftover := _leftover[:0:128]

	times := (to - from) / BUFFER_SIZE
	var read int

	for i := range times + 1 {
		var size int64 = BUFFER_SIZE
		if i == times && int64(read)+BUFFER_SIZE > to-from {
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
			o := make([]byte, firstLineIndex)
			copy(o, buf[:firstLineIndex])

			overflows[workerID*2-1] = o
		} else {
			leftover = append(leftover, buf[:firstLineIndex]...)
			parseLine(leftover, h, m)
			leftover = leftover[:0]
		}

		var lastLineIndex int
		for lastLineIndex = n - 1; ; lastLineIndex-- {
			if buf[lastLineIndex] == '\n' {
				break
			}
		}

		if workerID != workers-1 && i == times {
			o := make([]byte, len(buf)-lastLineIndex+1)
			copy(o, buf[lastLineIndex+1:])

			overflows[workerID*2] = o
		} else {
			leftover = append(leftover, buf[lastLineIndex+1:]...)
		}

		computeChunk(buf[firstLineIndex+1:lastLineIndex+1], h, m)
	}

	return sortedValues(m)
}

func sortedValues(m map[uint64]*WeatherStationInfo) []*WeatherStationInfo {
    values := make([]*WeatherStationInfo, 0, len(m))
    for _, value := range m {
        values = append(values, value)
    }
    
    sorting.Sort(values)
    return values
}

func computeChunk(chunk []byte, h hash.Hash64, m map[uint64]*WeatherStationInfo) {
	var nextStart int
	for i, b := range chunk {
		if b == '\n' {
			parseLine(chunk[nextStart:i], h, m)
			nextStart = i + 1
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
	for i := len(line) - 1; i > splitIndex; i-- {
		switch line[i] {
		case '.':
			continue loop
		case '-':
			temp *= -1
			break loop
		default:
			temp += int16(line[i]-'0') * exp
			exp *= 10
		}
	}

	wsi, ok := m[nameHash]
	if !ok {
		m[nameHash] = &WeatherStationInfo{
			name: string(line[:splitIndex]),
			min:  temp, max: temp,
			acc: int64(temp), count: 1,
		}
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
			fmt.Fprintf(out, "\t%s=%.1f/%.1f/%.1f", x.name, float32(x.min)/10.0, float64(x.acc)/10.0/float64(x.count), float32(x.max)/10.0)
		} else {
			fmt.Fprintf(out, ",\n\t%s=%.1f/%.1f/%.1f", x.name, float32(x.min)/10.0, float64(x.acc)/10.0/float64(x.count), float32(x.max)/10.0)
		}
	}
	fmt.Fprint(out, "\n}\n")
}
