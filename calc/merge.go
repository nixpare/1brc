package main

func mergeMatrix(partials [][]*WeatherStationInfo) []*WeatherStationInfo {
    var n int
	for _, v := range partials {
		n += len(v)
	}

	result := make([]*WeatherStationInfo, n)
	tmp := make([][]*WeatherStationInfo, len(partials) / 2 + 1)

	for len(partials) > 1 {
		var from int
		for i := 0; i+1 < len(partials); i += 2 {
			length := len(partials[i]) + len(partials[i+1])
			n := mergeMatrixInto(partials[i], partials[i+1], result[from:from+length])
			
			partials[i] = result[from:from+n]
			from += length
		}

		tmp = tmp[:0]
		for i := 0; i < len(partials); i += 2 {
			tmp = append(tmp, partials[i])
		}

		partials = partials[:len(tmp)]
		copy(partials, tmp)
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
