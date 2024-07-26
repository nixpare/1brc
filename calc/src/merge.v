module main

@[direct_array_access; manualfree]
fn merge_matrix(mut partials [][]&WeatherStationInfo) []&WeatherStationInfo {
	mut n := 0
	for v in partials {
		n += v.len
	}

	mut result := unsafe{ []&WeatherStationInfo{ len: n*2 } }

	mut len := partials.len
	mut from_result := 0
	mut first := true
	for len > 1 {
		if from_result > n {
			from_result = 0
		} else {
			from_result = n
		}
		
		for i := 0; i+1 < len; i += 2 {
			a := partials[i]
			b := partials[i+1]
			length := a.len + b.len

			actual_length := merge_matrix_into(a, b, mut result[from_result..from_result+length])

			if first {
				first = false
				unsafe {
					partials[i].free()
					partials[i+1].free()
				}
			}
			
			partials[i/2] = result[from_result..from_result+actual_length]
			from_result += actual_length
		}

		if len % 2 == 1 {
			partials[len/2] = partials[len - 1]
			len = len / 2 + 1
		} else {
			len = len / 2
		}
	}

	return partials[0]
}

@[direct_array_access; manualfree]
fn merge_matrix_into(a []&WeatherStationInfo, b []&WeatherStationInfo, mut into []&WeatherStationInfo) int {
	mut i := 0
	mut j := 0
	mut k := 0

	for ; i < a.len && j < b.len; k++ {
		mut x := a[i]
		y := b[j]

		cmp := compare_strings(x.name, y.name)
		match cmp {
			-1 {
				into[k] = x
				i++
			}
			1 {
				into[k] = y
				j++
			}
			0 {
				if y.min < x.min {
					x.min = y.min
				}
				if y.max > x.max {
					x.max = y.max
				}
				x.acc += y.acc
				x.count += y.count

				into[k] = x
				unsafe { free(y) }

				i++; j++
			}
			else { panic("unreachable") }
		}
	}

	for i < a.len {
		into[k] = a[i]
		i++; k++
	}

	for j < b.len {
		into[k] = b[j]
		j++; k++
	}

	return k
}
