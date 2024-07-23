module main

import os
import time
import runtime
import hash.fnv1a
import io
import math

const (
    buffer_size = 1024 * 1024
    workers_multiplier = 1 //20
)

struct WeatherStationInfo {
	name  string
mut:
	min   i16
	max   i16
	acc   i64
	count i64
}

fn main() {
	start := time.now()

	if os.args.len < 3 {
		eprintln("Required source and dest path")
		exit(1)
	}

	mut out := os.create(os.args[2])!
	defer { out.close() }

	in_file_path := os.args[1]
	in_info := os.stat(in_file_path)!

	file_size := in_info.size
	workers := if file_size < buffer_size {
		1
	} else {
		runtime.nr_cpus() * workers_multiplier + 2
	}

	chunk_size := if workers == 1 {
		file_size
	} else {
		file_size / u64(workers-1)
	}

	mut partials := [][]&WeatherStationInfo{ cap: workers+1 }
	mut overflows := [][]u8{ len: workers*2 }

	mut threads := []thread ![]&WeatherStationInfo{ cap: workers }

	for i in 0..workers {
		from := chunk_size * u64(i)
		mut to := from + chunk_size
		if to > file_size {
			to = file_size
		}

		h := go compute(in_file_path, from, to, i, workers, mut overflows)
		threads << h
	}

	for h in threads {
		partials << h.wait()!
	}

	mut leftover := []u8{ len: 0, cap: 128 }
	mut i := 0
	for i < leftover.len {
		leftover << overflows[i]
		leftover << overflows[i+1]
		leftover << `\n`

		i += 2
	}

	mut leftover_m := map[u64]&WeatherStationInfo{}

	compute_chunk(leftover, mut leftover_m)
	partials << sorted_values(leftover_m)

	result := merge_matrix(partials)
	print_result(mut out, result)!

	end := time.since(start)
	println(end.str())
}

fn compute(filePath string, from i64, to i64, workerID int, workers int, mut overflows [][]u8) ![]&WeatherStationInfo {
	if from == to {
		return []&WeatherStationInfo{}
	}

	mut m := map[u64]&WeatherStationInfo{}

	mut f := os.open_file(filePath, "r", 0)!
	defer { f.close() }

	f.seek(from, .start)!

	mut buf := []u8{ len: buffer_size }
	mut leftover := []u8{ len: 0, cap: 128 }

	times := (to - from) / buffer_size
	mut read := 0

	for i in 0 .. times + 1 {
		mut size := i64(buffer_size)
		if i == times && i64(read)+buffer_size > to-from {
			size = to - from - i64(read)
		}

		n := f.read(mut buf[..size]) or {
			match err {
				os.Eof { break }
				else { return err }
			}
		}
		read += n

		mut first_line_index := 0
		for {
			if buf[first_line_index] == `\n` {
				break
			}
			first_line_index++
		}

		if workerID != 0 && i == 0 {
			mut o := []u8{ len: first_line_index }
			copy(mut o, buf[..first_line_index])

			overflows[workerID*2-1] = o
		} else {
			leftover << buf[..first_line_index]
			parse_line(leftover, mut m)
			leftover.clear()
		}

		mut last_line_index := n - 1
		for {
			if buf[last_line_index] == `\n` {
				break
			}
			last_line_index--
		}

		if workerID != workers-1 && i == times {
			mut o := []u8{ len: buf.len-last_line_index+1 }
			copy(mut o, buf[last_line_index+1..])

			overflows[workerID*2] = o
		} else {
			leftover << buf[last_line_index+1..]
		}

		compute_chunk(buf[first_line_index + 1..last_line_index + 1], mut m)
	}

	return sorted_values(m)
}

fn sorted_values(m map[u64]&WeatherStationInfo) []&WeatherStationInfo {
    mut values := []&WeatherStationInfo{ cap: m.len }
    for _, value in m {
        values << value 
    }

	values.sort(a.name < b.name)
    return values
}

fn compute_chunk(chunk []u8, mut m map[u64]&WeatherStationInfo) {
	mut next_start := 0
	for i, b in chunk {
		if b == `\n` {
			parse_line(chunk[next_start..i], mut m)
			next_start = i + 1
		}
	}
}

fn parse_line(line []u8, mut m map[u64]&WeatherStationInfo) {
	if line.len == 0 {
		return
	}

	mut split_idx := 0
	for i, c in line {
		if c == `;` {
			split_idx = i
			break
		}
	}

	name_hash := fnv1a.sum64(line[..split_idx])

	mut temp := i16(0)
	mut exp := i16(1)
	
	loop: for i := line.len - 1; i > split_idx; i-- {		
		match line[i] {
			`.` { continue loop }
			`-` {
				temp *= -1
				break loop
			}
			else {
				temp += i16(line[i]-`0`) * exp
				exp *= 10
			}
		}
	}
	
	if mut wsi := m[name_hash] {
		wsi.min = math.min(wsi.min, temp)
		wsi.max = math.max(wsi.max, temp)
		wsi.acc += i64(temp)
		wsi.count++
	} else {
		m[name_hash] = &WeatherStationInfo{
			name: line[..split_idx].bytestr(),
			min:  temp, max: temp,
			acc: i64(temp), count: 1,
		}
	}
}

fn print_result(mut out io.Writer, result []&WeatherStationInfo) ! {
	out.write("{\n".bytes())!

	mut first := true
	for x in result {
		if first {
			first = false
			out.write('\t${ x.name }=${ f32(x.min) / 10.0 : .1f }/${ f64(x.acc)/ 10.0 / f64(x.count) : .1f }/${ f32(x.max) / 10.0 : .1f }'.bytes())!
		} else {
			out.write(',\n\t${ x.name }=${ f32(x.min) / 10.0 : .1f }/${ f64(x.acc)/ 10.0 / f64(x.count) : .1f }/${ f32(x.max) / 10.0 : .1f }'.bytes())!
		}
	}

	out.write("\n}\n".bytes())!
}
