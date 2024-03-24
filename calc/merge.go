package main

import (
	"fmt"
	"io"
	"strings"
)

func mergeSortMulti(results [][]*WeatherStationInfo) []*WeatherStationInfo {
    if len(results) == 0 {
        return nil
    }
    
    if len(results) == 1 {
        return results[0]
    }

	mid := len(results) / 2
	return mergeMulti(results[:mid], results[mid:])
}

func mergeMulti(resultsA [][]*WeatherStationInfo, resultsB [][]*WeatherStationInfo) []*WeatherStationInfo {
	chA := make(chan []*WeatherStationInfo)
	chB := make(chan []*WeatherStationInfo)

	go func() {
		chA <- mergeSortMulti(resultsA)
	}()
	go func() {
		chB <- mergeSortMulti(resultsB)
	}()

	resA := mergeSort(<-chA)
	resB := mergeSort(<-chB)

	res := make([]*WeatherStationInfo, 0, len(resA)+len(resB))

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

func mergeSort(s []*WeatherStationInfo) []*WeatherStationInfo {
	if len(s) < 2 {
		return s
	}

	mid := len(s) / 2

	a := mergeSort(s[:mid])
	b := mergeSort(s[mid:])

	return merge(a, b)
}

func merge(a []*WeatherStationInfo, b []*WeatherStationInfo) []*WeatherStationInfo {
	res := make([]*WeatherStationInfo, 0, len(a)+len(b))
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

func printResult(out io.Writer, result []*WeatherStationInfo) {
	fmt.Fprint(out, "{\n")
	first := true

	for _, x := range result {
		if first {
			first = false
			fmt.Fprintf(out, "\t%s=%3.1f/%3.1f/%3.1f", x.name, float32(x.min) / 10.0, float64(x.acc) / 10.0 / float64(x.count), float32(x.max) / 10.0)
		} else {
			fmt.Fprintf(out, ",\n\t%s=%3.1f/%3.1f/%3.1f", x.name, float32(x.min) / 10.0, float64(x.acc) / 10.0 / float64(x.count), float32(x.max) / 10.0)
		}
	}
	fmt.Fprint(out, "\n}\n")
}
