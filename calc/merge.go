package main

func mergeMatrix(partials [][]*WeatherStationInfo) []*WeatherStationInfo {
    var n int
	for _, v := range partials {
		n += len(v)
	}

	result := make([]*WeatherStationInfo, n)

	for len(partials) > 1 {
		var from int
		for i := 0; i+1 < len(partials); i += 2 {
			a, b := partials[i], partials[i+1]
			length := len(a) + len(b)

			n := mergeMatrixInto(a, b, result[from:from+length])

			partials[i/2] = result[from : from+n]
			from += n
		}

		oldLen := len(partials)
		if oldLen%2 == 1 {
			partials[oldLen/2+1] = partials[oldLen-1]
			partials = partials[:oldLen/2+1]
		} else {
			partials = partials[:oldLen/2]
		}
	}

	return partials[0]
}

func mergeMatrixInto(a []*WeatherStationInfo, b []*WeatherStationInfo, into []*WeatherStationInfo) int {
	var i, j, k int
	for i < len(a) && j < len(b) {
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
			i++
			j++
		}

		k++
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
