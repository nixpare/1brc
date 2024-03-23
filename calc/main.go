package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/highwayhash"
	"golang.org/x/exp/maps"
)

type WeatherStationInfo struct {
	name  string
	min   float64
	max   float64
	acc   float64
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

	in, err := os.Open(os.Args[len(os.Args)-2])
	if err != nil {
		log.Fatalln(err)
	}
	defer in.Close()

	results := compute(in)
	result := mergeSortMulti(results)
	printResult(out, result)

	fmt.Println(time.Since(start))
}

const (
	CHAN_SIZE   = 2048
	BUFFER_SIZE = 2048 * 2048 * 16
)

func compute(in io.Reader) [][]WeatherStationInfo {
	n := int('Z' - 'A')

	leftovers := make(chan []byte, CHAN_SIZE)
	var leftoverWG sync.WaitGroup
	leftoverWG.Add(1)

    results := make([][]WeatherStationInfo, n+1)
	readLock := new(sync.Mutex)

	var readerWG sync.WaitGroup
	readerWG.Add(n)

	go func() {
		rd, wr := io.Pipe()
		br := bufio.NewReaderSize(rd, BUFFER_SIZE)

		go func() {
			defer leftoverWG.Done()

			m := make(map[uint64]WeatherStationInfo)

			for {
				line, err := br.ReadBytes('\n')
				if err != nil {
                    if errors.Is(err, io.EOF) {
					    break
                    }
                    log.Fatalln(err)
				}

				if len(line) > 0 {
                    if line[len(line)-1] == '\n' {
                        line = line[:len(line)-1]
                    }

                    if len(line) > 0 {
                        computeLine(line, m)
                    }
				}
			}

			results[n] = computeResult(m)
		}()

		for lo := range leftovers {
			wr.Write(lo)
		}
		wr.Close()
	}()

	for i := range n {
		go func() {
			defer readerWG.Done()

			m := make(map[uint64]WeatherStationInfo)

			for {
				buf := make([]byte, BUFFER_SIZE)

				readLock.Lock()
				read, err := in.Read(buf)

				if err != nil && !errors.Is(err, io.EOF) {
					log.Fatalln(err)
				}

				if read > 0 {
					func() {
						index := bytes.IndexByte(buf, '\n')
						if index == -1 {
							leftovers <- buf
							return
						}

						leftovers <- buf[:index+1]
						buf = buf[index+1:]

						index = bytes.LastIndexByte(buf, '\n')
						if index == -1 {
							leftovers <- buf
							return
						}

						leftovers <- buf[index+1:]
						buf = buf[:index+1]
					}()
					readLock.Unlock()

					computeChunk(buf, m)
					results[i] = computeResult(m)
				} else {
					readLock.Unlock()
				}

				if err != nil {
					// This is only if io.EOF
					break
				}
			}
		}()
	}

	readerWG.Wait()
	close(leftovers)
	leftoverWG.Wait()

	return results
}

func computeChunk(buf []byte, m map[uint64]WeatherStationInfo) {
	for {
		index := bytes.IndexByte(buf, '\n')
		if index == -1 {
			break
		}

		line := buf[:index]
		buf = buf[index+1:]

		computeLine(line, m)
	}
}

func computeResult(m map[uint64]WeatherStationInfo) []WeatherStationInfo {
	values := maps.Values(m)
	slices.SortFunc(values, func(x WeatherStationInfo, y WeatherStationInfo) int {
		return strings.Compare(x.name, y.name)
	})

	return values
}

func computeLine(line []byte, result map[uint64]WeatherStationInfo) {
	index := bytes.IndexByte(line, ';')
    if index == -1 {
        log.Printf("No index: len: %d\n", len(line))
        debug.PrintStack()
        os.Exit(1)
    }
	temp, err := strconv.ParseFloat(string(line[index+1:]), 64)
	if err != nil {
		log.Fatalln(string(line[:index]), err)
	}

	var key [32]byte
	hash := highwayhash.Sum64(line[:index], key[:])

	info, found := result[hash]
	if !found {
		result[hash] = WeatherStationInfo{
			name: string(line[:index]),
			min:  temp, max: temp,
			acc: temp, count: 1,
		}
	} else {
		if temp < info.min {
			info.min = temp
		}
		if temp > info.max {
			info.max = temp
		}

		info.acc += temp
		info.count++

		result[hash] = info
	}
}

func mergeSortMulti(results [][]WeatherStationInfo) []WeatherStationInfo {
    if len(results) == 0 {
        return nil
    }
    
    if len(results) == 1 {
        return results[0]
    }

	mid := len(results) / 2
	return mergeMulti(results[:mid], results[mid:])
}

func mergeMulti(resultsA [][]WeatherStationInfo, resultsB [][]WeatherStationInfo) []WeatherStationInfo {
	chA := make(chan []WeatherStationInfo)
	chB := make(chan []WeatherStationInfo)

	go func() {
		chA <- mergeSortMulti(resultsA)
	}()
	go func() {
		chB <- mergeSortMulti(resultsB)
	}()

	resA := mergeSort(<-chA)
	resB := mergeSort(<-chB)

	res := make([]WeatherStationInfo, 0, len(resA)+len(resB))

	var i, j int
	for i < len(resA) && j < len(resB) {
        switch strings.Compare(resA[i].name, resB[j].name) {
        case -1:
            res = append(res, resA[i])
			i++
        case 0:
            x := resA[i]
            y := resB[j]

            if y.min < x.min {
                x.min = y.min
            }
            if y.max > x.max {
                x.max = y.max
            }
            x.acc += y.acc
            x.count += y.count

            res = append(res, x)
            i++
            j++
        case 1:
            res = append(res, resB[j])
			j++
        }
	}

    res = append(res, resA[i:]...)
    res = append(res, resB[j:]...)

    return res
}

func mergeSort(s []WeatherStationInfo) []WeatherStationInfo {
    if len(s) < 2 {
        return s
    }
    
    mid := len(s) / 2
    
    a := mergeSort(s[:mid])
    b := mergeSort(s[mid:])

    return merge(a, b)
}

func merge(a []WeatherStationInfo, b []WeatherStationInfo) []WeatherStationInfo {
    res := make([]WeatherStationInfo, 0, len(a) + len(b))
    var i, j int

    for i < len(a) && j < len(b) {
        if a[i].name <= b[j].name {
            res = append(res, a[i])
            i++
        } else {
            res = append(res, b[j])
            j++
        }
    }

    res = append(res, a[i:]...)
    res = append(res, b[j:]...)

    return res
}

func printResult(out io.Writer, result []WeatherStationInfo) {
	fmt.Fprint(out, "{\n")
	first := true

	for _, x := range result {
		if first {
			first = false
			fmt.Fprintf(out, "\t%s=%3.1f/%3.1f/%3.1f", x.name, x.min, math.Round(x.acc/float64(x.count)), x.max)
		} else {
			fmt.Fprintf(out, ",\n\t%s=%3.1f/%3.1f/%3.1f", x.name, x.min, math.Round(x.acc/float64(x.count)), x.max)
		}
	}
	fmt.Fprint(out, "\n}\n")
}
