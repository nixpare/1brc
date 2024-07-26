package main

func mergeMatrix(partials [][]*WeatherStationInfo) []*WeatherStationInfo {
    var n int
	for _, v := range partials {
		n += len(v)
	}

	result := make([]*WeatherStationInfo, n*2)

	var from int
	for len(partials) > 1 {
		if from >= n {
			from = 0
		} else {
			from = n
		}

		for i := 0; i+1 < len(partials); i += 2 {
			a, b := partials[i], partials[i+1]
			length := len(a) + len(b)

			actualLength := mergeMatrixInto(a, b, result[from:from+length])

			partials[i/2] = result[from : from+actualLength]
			from += actualLength
		}

		if len(partials) % 2 == 1 {
			partials[len(partials)/2] = partials[len(partials)-1]
			partials = partials[:len(partials)/2+1]
		} else {
			partials = partials[:len(partials)/2]
		}
	}

	return partials[0]
}

func mergeMatrixInto(a []*WeatherStationInfo, b []*WeatherStationInfo, into []*WeatherStationInfo) int {
	var i, j, k int
	for ; i < len(a) && j < len(b); k++ {
		switch a[i].Compare(b[j]) {
		case -1:
			into[k] = a[i]
			i++
		case 1:
			into[k] = b[j]
			j++
		case 0:
			x := a[i]
			y := b[j]

			if y.min < x.min {
				x.min = y.min
			}
			if y.max > x.max {
				x.max = y.max
			}
			x.acc += y.acc
			x.count += y.count

			into[k] = x
			i++; j++
		}
	}

	for i < len(a) {
		into[k] = a[i]
		i++; k++
	}

	for j < len(b) {
		into[k] = b[j]
		j++; k++
	}

	return k
}
