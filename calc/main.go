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
	_ "runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/highwayhash"
	"golang.org/x/exp/maps"
)

type WeatherStationInfo struct {
    name string
    min float64
    max float64
    acc float64
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
    printResult(out, results)

    fmt.Println(time.Since(start))
}

const (
    CHAN_SIZE = 2048
    BUFFER_SIZE = 2048 * 2048 * 16
)

func compute(in io.Reader) [][]WeatherStationInfo {
    n := int('Z'-'A'+1)

    results := make([][]WeatherStationInfo, n)

    consumers := make([]chan []byte, n)
    leftovers := make(chan []byte, CHAN_SIZE)
    var consumerWG sync.WaitGroup
    consumerWG.Add(n+1)

    readLock := new(sync.Mutex)

    var readerWG sync.WaitGroup
    readerWG.Add(n)

    go func() {
        rd, wr := io.Pipe()
        br := bufio.NewReaderSize(rd, BUFFER_SIZE)
        
        go func() {
            defer consumerWG.Done()

            for {
                line, err := br.ReadBytes('\n')
                if err != nil && !errors.Is(err, io.EOF) {
                    log.Fatalln(err)
                }
    
                if len(line) > 0 {
                    dispatchLine(line, consumers)
                }
    
                if err != nil {
                    return
                }
            }
        }()
        
        for lo := range leftovers {
            wr.Write(lo)
        }
        wr.Close()
    }()
    
    for i := range n {
        consumers[i] = make(chan []byte, CHAN_SIZE)

        go func() {
            defer readerWG.Done()

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

                    dispatchLine(buf, consumers)
                } else {
                    readLock.Unlock()
                }

                if err != nil {
                    // This is only if io.EOF
                    break
                }
            }
        }()

        go func() {
            defer consumerWG.Done()

            m := make(map[uint64]WeatherStationInfo)

            for line := range consumers[i] {
                computeLine(line, m)
            }

            values := maps.Values(m)
            slices.SortFunc(values, func(x WeatherStationInfo, y WeatherStationInfo) int {
                return strings.Compare(x.name, y.name)
            })

            results[i] = values
        }()
    }

    readerWG.Wait()
    for _, ch := range consumers {
        close(ch)
    }
    close(leftovers)
    consumerWG.Wait()

    return results
}

func dispatchLine(buf []byte, consumers []chan []byte) {
    n := len(consumers)

    for {
        index := bytes.IndexRune(buf, '\n')
        if index == -1 {
            break
        }

        line := buf[:index]
        buf = buf[index+1:]

        worker := int(line[0]-'A')
        if worker > n-1 {
            worker = n-1
        }
        consumers[worker] <- line
    }
}

func computeLine(line []byte, result map[uint64]WeatherStationInfo) {
    index := bytes.IndexRune(line, ';')
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
            min: temp, max: temp,
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

func printResult(out io.Writer, results [][]WeatherStationInfo) {
    fmt.Fprint(out, "{\n")
    first := true

    for _, res := range results {
        for _, x := range res {
            if first {
                first = false
                fmt.Fprintf(out, "\t%s=%3.1f/%3.1f/%3.1f", x.name, x.min, math.Round(x.acc / float64(x.count)), x.max)
            } else {
                fmt.Fprintf(out, ",\n\t%s=%3.1f/%3.1f/%3.1f", x.name, x.min, math.Round(x.acc / float64(x.count)), x.max)
            }
        }
    }
    fmt.Fprint(out, "\n}\n")
}

/*

if read != 0 {
                    if len(leftover) > 0 {
                        index := bytes.IndexRune(buf, '\n')
                        
                        if index == -1 {    
                            worker := int(leftover[0]-'A')
                            if worker > n-1 {
                                worker = n-1
                            }
                            consumers[worker] <- leftover
            
                            leftover = buf
                            continue
                        }
        
                        leftover = append(leftover, buf[:index]...)
                        buf = buf[index+1:]
        
                        worker := int(leftover[0]-'A')
                        if worker > n-1 {
                            worker = n-1
                        }
                        consumers[worker] <- leftover
        
                        leftover = nil
                    }
                    
                    for {
                        index := bytes.IndexRune(buf, '\n')
                        if index == -1 {
                            leftover = buf
                            break
                        }
            
                        line := buf[:index]
                        buf = buf[index+1:]
            
                        worker := int(line[0]-'A')
                        if worker > n-1 {
                            worker = n-1
                        }
                        consumers[worker] <- line
                    }
                } else {
                    readLock.Unlock()
                }

go func() {
        defer func() {
            for _, ch := range consumers {
                close(ch)
            }
        }()

        var leftover []byte
        for buf := range dispatcher {
            readChan <- struct{}{}

            if len(leftover) > 0 {
                index := bytes.IndexRune(buf, '\n')
                
                if index == -1 {    
                    worker := int(leftover[0]-'A')
                    if worker > n-1 {
                        worker = n-1
                    }
                    consumers[worker] <- leftover
    
                    leftover = buf
                    continue
                }

                leftover = append(leftover, buf[:index]...)
                buf = buf[index+1:]

                worker := int(leftover[0]-'A')
                if worker > n-1 {
                    worker = n-1
                }
                consumers[worker] <- leftover

                leftover = nil
            }
            
            for {
                index := bytes.IndexRune(buf, '\n')
                if index == -1 {
                    leftover = buf
                    break
                }
    
                line := buf[:index]
                buf = buf[index+1:]
    
                worker := int(line[0]-'A')
                if worker > n-1 {
                    worker = n-1
                }
                consumers[worker] <- line
            }
        }
    }()

*/
