const std = @import("std");
const merge = @import("merge.zig");

const BUFFER_SIZE = 1024 * 1024;

pub const WeatherStationInfo = struct {
    name: []u8,
    min: i16,
    max: i16,
    acc: i64,
    count: u64,

    pub fn compare(self: *WeatherStationInfo, other: *WeatherStationInfo) i8 {
        var i: usize = 0;
        while (i < self.name.len and i < other.name.len) : (i += 1) {
            if (self.name[i] < other.name[i]) {
                return -1;
            } else if (self.name[i] > other.name[i]) {
                return 1;
            }
        }

        if (self.name.len == other.name.len) {
            return 0;
        } else if (self.name.len < other.name.len) {
            return -1;
        } else {
            return 1;
        }
    }

    pub fn lessThan(_: void, a: *WeatherStationInfo, b: *WeatherStationInfo) bool {
        return a.compare(b) < 0;
    }
};

const WSIHashMap = std.AutoHashMap(u64, *WeatherStationInfo);

pub fn main() !void {
    const start = try std.time.Instant.now();
    defer printEnd(start) catch @panic("Failed to get or print end time");

    var heapAllocator = std.heap.HeapAllocator.init();
    defer heapAllocator.deinit();
    var safeAllocator = std.heap.ThreadSafeAllocator{ .child_allocator = heapAllocator.allocator() };
    const allocator = safeAllocator.allocator();

    const args = try std.process.argsAlloc(allocator);

    if (args.len < 3) {
        try std.io.getStdErr().writeAll("Usage: calc <input file> <output file>\n");
        return;
    }

    const inputFile = args[1];
    var out = try std.fs.cwd().createFile(args[2], .{});
    defer out.close();

    const inInfo = try std.fs.cwd().statFile(inputFile);

    const fileSize = inInfo.size;
    const workers = if (fileSize < BUFFER_SIZE) 1 else try std.Thread.getCpuCount();
    const chunkSize = if (workers == 1) fileSize else fileSize / (workers - 1);

    var partials = try allocator.alloc([]*WeatherStationInfo, workers + 1);
    const overflows = try allocator.alloc([]u8, workers * 2 - 2);
    const threads = try allocator.alloc(std.Thread, workers);

    for (0..workers) |i| {
        const from = i * chunkSize;
        var to = from + chunkSize;
        if (to > fileSize) {
            to = fileSize;
        }

        threads[i] = try std.Thread.spawn(.{ .allocator = allocator }, computeThread, .{ inputFile, from, to, i, workers, partials, overflows, allocator });
    }

    for (threads) |thread| {
        thread.join();
    }

    var leftover = try std.ArrayList(u8).initCapacity(allocator, 128);
    var i: usize = 0;
    while (i < overflows.len) : (i += 2) {
        try leftover.appendSlice(overflows[i]);
        try leftover.appendSlice(overflows[i + 1]);
        try leftover.append('\n');
    }

    var leftoverMap = WSIHashMap.init(allocator);
    try computeChunk(leftover.items, &leftoverMap, allocator);

    partials[partials.len - 1] = try sortedValues(&leftoverMap, allocator);

    const result = try merge.mergeMatrix(partials, allocator);
    try printResult(&out, result);
}

fn computeThread(inputFile: []u8, from: usize, to: usize, i: usize, workers: usize, partials: [][]*WeatherStationInfo, overflows: [][]u8, allocator: std.mem.Allocator) !void {
    partials[i] = try compute(inputFile, from, to, i, workers, overflows, allocator);
}

fn compute(filePath: []u8, from: usize, to: usize, workerID: usize, workers: usize, overflows: [][]u8, allocator: std.mem.Allocator) ![]*WeatherStationInfo {
    if (from == to)
        return &[0]*WeatherStationInfo{};

    var map = WSIHashMap.init(allocator);
    const file = try std.fs.cwd().openFile(filePath, .{});
    defer file.close();

    try file.seekTo(from);

    const buffer = try allocator.alloc(u8, BUFFER_SIZE);
    var leftover = try std.ArrayList(u8).initCapacity(allocator, 128);

    const times = (to - from) / BUFFER_SIZE;
    var read: usize = 0;

    for (0..times + 1) |i| {
        var size: usize = BUFFER_SIZE;
        if (i == times and read + BUFFER_SIZE > to - from) {
            size = to - from - read;
        }

        const n = try file.read(buffer);
        read += n;

        var firstLineIndex: usize = 0;
        while (true) : (firstLineIndex += 1) {
            if (buffer[firstLineIndex] == '\n') {
                break;
            }
        }

        if (workerID != 0 and i == 0) {
            const o = try allocator.alloc(u8, firstLineIndex);
            std.mem.copyForwards(u8, o, buffer[0..firstLineIndex]);

            overflows[workerID * 2 - 1] = o;
        } else {
            try leftover.appendSlice(buffer[0..firstLineIndex]);
            try parseLine(leftover.items, &map, allocator);
            leftover.clearRetainingCapacity();
        }

        var lastLineIndex = n - 1;
        while (true) : (lastLineIndex -= 1) {
            if (buffer[lastLineIndex] == '\n') {
                break;
            }
        }

        if (workerID != workers - 1 and i == times) {
            const o = try allocator.alloc(u8, buffer.len - lastLineIndex + 1);
            std.mem.copyForwards(u8, o, buffer[lastLineIndex + 1 ..]);

            overflows[workerID * 2] = o;
        } else {
            try leftover.appendSlice(buffer[lastLineIndex + 1 ..]);
        }

        try computeChunk(buffer[firstLineIndex + 1 .. lastLineIndex + 1], &map, allocator);
    }

    return try sortedValues(&map, allocator);
}

fn computeChunk(chunk: []u8, map: *WSIHashMap, allocator: std.mem.Allocator) !void {
    var nextStart: usize = 0;
    for (chunk, 0..) |c, i| {
        if (c == '\n') {
            const line = chunk[nextStart..i];
            try parseLine(line, map, allocator);
            nextStart = i + 1;
        }
    }
}

fn parseLine(line: []u8, map: *WSIHashMap, allocator: std.mem.Allocator) !void {
    if (line.len == 0)
        return;

    var splitIndex: usize = 0;
    for (line, 0..) |c, i| {
        if (c == ';') {
            splitIndex = i;
            break;
        }
    }

    const nameHash: u64 = std.hash.Fnv1a_64.hash(line[0..splitIndex]);
    var temp: i16 = 0;
    var exp: i16 = 1;

    var i = line.len - 1;
    loop: while (i > splitIndex) : (i -= 1) {
        switch (line[i]) {
            '.' => {
                continue :loop;
            },
            '-' => {
                temp *= -1;
                break :loop;
            },
            else => {
                @setRuntimeSafety(false);
                temp += @as(i16, line[i] - '0') * exp;
                exp *= 10;
            },
        }
    }

    const wsiOpt = map.get(nameHash);
    if (wsiOpt) |wsi| {
        wsi.min = @min(wsi.min, temp);
        wsi.max = @max(wsi.max, temp);
        wsi.acc += temp;
        wsi.count += 1;
    } else {
        const wsi = try allocator.create(WeatherStationInfo);
        const wsiName = try allocator.alloc(u8, splitIndex);
        std.mem.copyForwards(u8, wsiName, line[0..splitIndex]);

        if (wsiName.len == 0)
            std.debug.print("Empty name\n", .{});

        wsi.* = .{ .name = wsiName, .min = temp, .max = temp, .acc = temp, .count = 1 };
        try map.put(nameHash, wsi);
    }
}

fn sortedValues(map: *WSIHashMap, allocator: std.mem.Allocator) ![]*WeatherStationInfo {
    var values = try std.ArrayList(*WeatherStationInfo).initCapacity(allocator, map.capacity());

    var valueIterator = map.valueIterator();
    var value = valueIterator.next();
    while (value != null) : (value = valueIterator.next()) {
        try values.append(value.?.*);
    }

    std.mem.sort(*WeatherStationInfo, values.items, {}, WeatherStationInfo.lessThan);
    return values.items;
}

fn printResult(out: *std.fs.File, result: []*WeatherStationInfo) !void {
    try out.writeAll("{\n");
    const writer = out.writer();

    var first = true;
    for (result) |x| {
        if (first) {
            first = false;
            try writer.print("\t{s}={d:.1}/{d:.1}/{d:.1}", .{ x.name, @as(f32, @floatFromInt(x.min)) / 10.0, @as(f64, @floatFromInt(x.acc)) / 10.0 / @as(f64, @floatFromInt(x.count)), @as(f32, @floatFromInt(x.max)) / 10.0 });
        } else {
            try writer.print(",\n\t{s}={d:.1}/{d:.1}/{d:.1}", .{ x.name, @as(f32, @floatFromInt(x.min)) / 10.0, @as(f64, @floatFromInt(x.acc)) / 10.0 / @as(f64, @floatFromInt(x.count)), @as(f32, @floatFromInt(x.max)) / 10.0 });
        }
    }
    try out.writeAll("\n}\n");
}

fn printEnd(start: std.time.Instant) !void {
    const end = try std.time.Instant.now();
    try std.io.getStdOut().writer().print("Time: {}\n", .{std.fmt.fmtDuration(end.since(start))});
}
