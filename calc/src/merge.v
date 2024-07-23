module main

@[manualfree]
fn merge_matrix(_partials [][]&WeatherStationInfo) []&WeatherStationInfo {
	mut partials := unsafe{ _partials }

	mut n := 0
	for v in partials {
		n += v.len
	}

	mut result := []&WeatherStationInfo{ cap: n }

	for partials.len > 1 {
		for i := 0; i+1 < partials.len; i += 2 {
			a := partials[i]
			b := partials[i+1]

			actual_length := merge_matrix_into(a, b, mut result)

			partials[i/2] = result[..actual_length]
			result = unsafe{ result[actual_length..] }
		}

		old_len := partials.len
		if old_len % 2 == 1 {
			partials[old_len/2+1] = partials[old_len - 1]
			partials = unsafe{ partials[..old_len / 2 + 1] }
		} else {
			partials = unsafe{ partials[..old_len / 2] }
		}
	}

	return partials[0]
}

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
				into << x
				i++
			}
			1 {
				into << y
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

				into << x

				i++; j++
			}
			else { panic("unreachable") }
		}
	}

	for i < a.len {
		into << a[i]
		i++; k++
	}

	for j < b.len {
		into << b[j]
		j++; k++
	}

	return k
}
