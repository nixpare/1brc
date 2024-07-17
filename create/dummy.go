package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/maps"
)

type WeatherStationInfo struct {
    name string
    min float64
    max float64
    acc float64
    count int
}

func dummy(measurementsPath string, resultPath string) {
    start := time.Now()

	out, err := os.Create(resultPath)
	if err != nil {
		log.Fatalln(err)
	}
	defer out.Close()

    in, err := os.Open(measurementsPath)
    if err != nil {
		log.Fatalln(err)
	}
	defer in.Close()

    results := make(map[string]WeatherStationInfo)

    sc := bufio.NewScanner(in)
    for sc.Scan() {
        name, tempString, _ := strings.Cut(sc.Text(), ";")
        temp, err := strconv.ParseFloat(tempString, 64)
        if err != nil {
            log.Fatalln(name, err)
        }

        info,found := results[name]
        if !found {
            results[name] = WeatherStationInfo{
                name: name,
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

            results[name] = info
        }
    }

    ids := maps.Keys(results)
    slices.Sort(ids)

    fmt.Fprint(out, "{\n")
    first := true
    for _, key := range ids {
        value := results[key]
        if first {
            first = false
            fmt.Fprintf(out, "\t%s=%.1f/%.1f/%.1f", key, value.min, value.acc / float64(value.count), value.max)
        } else {
            fmt.Fprintf(out, ",\n\t%s=%.1f/%.1f/%.1f", key, value.min, value.acc / float64(value.count), value.max)
        }
    }
    fmt.Fprint(out, "\n}\n")

    fmt.Printf("Generated dummy result at <%s> in %v\n", resultPath, time.Since(start));
}