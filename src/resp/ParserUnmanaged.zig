//! Parses RESP values from a reader.

const std = @import("std");
const testing = std.testing;

const Value = @import("../resp/value.zig").Value;

pub fn parse(reader: std.io.AnyReader, allocator: std.mem.Allocator) anyerror!Value {
    const tag = try reader.readByte();

    return try switch (tag) {
        '*' => parseArray(reader, allocator),
        '+' => parseSimpleString(reader, allocator),
        '$' => parseBulkString(reader, allocator),
        ':' => parseInteger(reader),
        else => {
            var buffer = [_]u8{ 'x', 'x', 'x', 'x' };
            _ = try reader.read(&buffer);
            try std.io.getStdOut().writer().print("UNEXPECTED TAG: '{}'", .{tag});
            return error.Unexpected;
        },
    };
}

test "parse empty stream should return error.EndOfStream" {
    try testing.expectError(error.EndOfStream, parseTestValue(""));
}

test "parse should handle end of stream" {
    try testing.expectError(error.EndOfStream, parseTestValue("+as"));
}

test "parse should handle end of stream and free memory" {
    try testing.expectError(
        error.EndOfStream,
        parseTestValue("*2\r\n+ECH\r\n"),
    );
}

/// Frees the allocated memory of the specified value, if any (not all values require allocated memory).
pub fn free(value: Value, allocator: std.mem.Allocator) void {
    switch (value) {
        .List => |arr| {
            for (arr) |item| {
                free(item, allocator);
            }

            allocator.free(arr);
        },
        .String => |s| {
            allocator.free(s);
        },
        else => {},
    }
}

fn parseArray(reader: std.io.AnyReader, allocator: std.mem.Allocator) anyerror!Value {
    const length = try readLength(reader);

    const arr = .{ .List = try allocator.alloc(Value, length) };
    errdefer allocator.free(arr.List);

    for (0..length) |i| {
        // we might have allocated memory before parsing a subsequent list item fails
        errdefer {
            for (0..i) |j| {
                free(arr.List[j], allocator);
            }
        }

        arr.List[i] = try parse(reader, allocator);
    }

    return arr;
}

test "parse array of simple strings" {
    const v = try parseTestValue("*2\r\n+ECHO\r\n+hey\r\n");
    defer free(v, testing.allocator);

    const arr = v.List;
    try testing.expectEqual(arr.len, 2);
    try testing.expectEqualDeep(arr[0], Value{ .String = "ECHO" });
    try testing.expectEqualDeep(arr[1], Value{ .String = "hey" });
}

test "parse array of different types of values" {
    const v = try parseTestValue("*3\r\n+S1\r\n$2\r\nS2\r\n:123\r\n");
    defer free(v, testing.allocator);

    try testing.expectEqual(3, v.List.len);
    try testing.expectEqualDeep(v.List[0], Value{ .String = "S1" });
    try testing.expectEqualDeep(v.List[1], Value{ .String = "S2" });
    try testing.expectEqualDeep(v.List[2], Value{ .Integer = 123 });
}

test "parse array of arrays" {
    const v = try parseTestValue("*2\r\n*2\r\n:123\r\n$2\r\nS1\r\n*2\r\n+S1\r\n+12311113\r\n");
    defer free(v, testing.allocator);

    try testing.expectEqual(2, v.List.len);
    try testing.expectEqual(123, v.List[0].List[0].Integer);
    try testing.expectEqualStrings("S1", v.List[0].List[1].String);
    try testing.expectEqualStrings("S1", v.List[1].List[0].String);
    try testing.expectEqualStrings("12311113", v.List[1].List[1].String);
}

fn parseSimpleString(reader: std.io.AnyReader, allocator: std.mem.Allocator) anyerror!Value {
    return .{ .String = try readUntilCarriageReturn(reader, allocator) };
}

test "parse simple string" {
    const v = try parseTestValue("+foobar\r\n");
    defer free(v, testing.allocator);

    try testing.expectEqualDeep(v, Value{ .String = "foobar" });
}

fn parseBulkString(reader: std.io.AnyReader, allocator: std.mem.Allocator) anyerror!Value {
    const s = try read(reader, try readLength(reader), allocator);
    try readTerminator(reader);

    return .{ .String = s };
}

test "parse bulk string" {
    const v = try parseTestValue("$2\r\nAB\r\n");
    defer free(v, testing.allocator);

    try testing.expectEqualDeep(Value{ .String = "AB" }, v);
}

pub fn parseRdb(reader: std.io.AnyReader, allocator: std.mem.Allocator) anyerror![]const u8 {
    _ = try reader.readByte();
    const len = try readLength(reader);

    const s = allocator.alloc(u8, len) catch return error.Unexpected;
    _ = try reader.readAll(s);
    return s;
}

// :[<+|->]<value>\r\n
fn parseInteger(reader: std.io.AnyReader) anyerror!Value {
    var value: i64 = 0;
    var negative = false;

    var offset: u8 = 0;
    while (true) {
        const v = try reader.readByte();
        switch (v) {
            '-' => {
                if (offset > 0) {
                    return error.Unexpected;
                }
                negative = true;
            },
            '0'...'9' => {
                value *= 10;
                value += v - '0';
            },
            '\r' => {
                if (offset == 0) {
                    return error.Unexpected;
                }
                if (offset == 1 and negative) {
                    return error.Unexpected;
                }
                if (try reader.readByte() != '\n') return error.Unexpected;
                return .{ .Integer = if (negative) -value else value };
            },
            else => return error.Unexpected,
        }
        offset += 1;
    }
}

test "parse integer" {
    // Note: no free needed here
    try testing.expectEqualDeep(Value{ .Integer = 1234 }, try parseTestValue(":1234\r\n"));
    try testing.expectEqualDeep(Value{ .Integer = -1234 }, try parseTestValue(":-1234\r\n"));
}

test "parse integer with invalid value" {
    // Note: no free needed here
    try testing.expectError(error.Unexpected, parseTestValue(":a\r\n"));
    try testing.expectError(error.Unexpected, parseTestValue(":-\r\n"));
    try testing.expectError(error.Unexpected, parseTestValue(":1-1\r\n"));
    try testing.expectError(error.Unexpected, parseTestValue(":--1\r\n"));
}

fn readLength(reader: std.io.AnyReader) !usize {
    var len: usize = 0;

    while (true) {
        const v = try reader.readByte();

        switch (v) {
            '0'...'9' => {
                len *= 10;
                len += v - '0';
            },
            '\r' => {
                if (try reader.readByte() != '\n') return error.Unexpected;
                return len;
            },
            else => return error.unexpected,
        }
    }
}

fn readByte(reader: std.io.AnyReader) !u8 {
    return try reader.readByte();
}

fn read(reader: std.io.AnyReader, len: usize, allocator: std.mem.Allocator) ![]const u8 {
    var l = try std.ArrayList(u8).initCapacity(allocator, len);
    defer l.deinit();

    for (0..len) |_| {
        try l.writer().writeByte(try reader.readByte());
    }

    return l.toOwnedSlice();
}

fn readTerminator(reader: std.io.AnyReader) !void {
    if (try reader.readByte() != '\r') return error.Unexpected;
    if (try reader.readByte() != '\n') return error.Unexpected;
}

fn readUntilCarriageReturn(reader: std.io.AnyReader, allocator: std.mem.Allocator) ![]const u8 {
    var buffer = try std.ArrayList(u8).initCapacity(allocator, 10);
    defer buffer.deinit();

    try reader.streamUntilDelimiter(buffer.writer(), '\r', null);

    if (try reader.readByte() != '\n') return error.Unexpected;

    return buffer.toOwnedSlice();
}

fn parseTestValue(comptime data: []const u8) !Value {
    var s = std.io.fixedBufferStream(data);

    return try parse(s.reader().any(), testing.allocator);
}
