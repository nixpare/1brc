const std = @import("std");
const WeatherStationInfo = @import("main.zig").WeatherStationInfo;

pub fn mergeMatrix(_partials: [][]*WeatherStationInfo, allocator: std.mem.Allocator) ![]*WeatherStationInfo {
    var partials = _partials;

    var n: usize = 0;
    for (partials) |p| {
        n += p.len;
    }

    const result = try allocator.alloc(*WeatherStationInfo, n * 2);

    var from: usize = 0;
    while (partials.len > 1) {
        if (from > n) {
            from = 0;
        } else {
            from = n;
        }

        var i: usize = 0;
        while (i + 1 < partials.len) : (i += 2) {
            const a = partials[i];
            const b = partials[i + 1];
            const length = a.len + b.len;

            const finalLength = mergeMatrixInto(a, b, result[from .. from + length]);

            partials[i / 2] = result[from .. from + finalLength];
            from += finalLength;
        }

        if (i < partials.len) {
            partials[i / 2] = partials[i];
            partials = partials[0 .. partials.len / 2 + 1];
        } else {
            partials = partials[0 .. partials.len / 2];
        }
    }

    return partials[0];
}

fn mergeMatrixInto(a: []*WeatherStationInfo, b: []*WeatherStationInfo, into: []*WeatherStationInfo) usize {
    var i: usize = 0;
    var j: usize = 0;
    var k: usize = 0;

    while (i < a.len and j < b.len) : (k += 1) {
        var x = a[i];
        const y = b[j];

        switch (x.compare(y)) {
            -1 => {
                into[k] = x;
                i += 1;
            },
            1 => {
                into[k] = y;
                j += 1;
            },
            0 => {
                if (y.min < x.min) {
                    x.min = y.min;
                }
                if (y.max > x.max) {
                    x.max = y.max;
                }
                x.acc += y.acc;
                x.count += y.count;

                into[k] = x;

                i += 1;
                j += 1;
            },
            else => unreachable,
        }
    }

    while (i < a.len) : (k += 1) {
        into[k] = a[i];
        i += 1;
    }

    while (j < b.len) : (k += 1) {
        into[k] = b[j];
        j += 1;
    }

    return k;
}
