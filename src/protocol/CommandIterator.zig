const std = @import("std");
const testing = std.testing;

const Types = @import("./types.zig");
const Command = Types.Command;
const RESP = Types.RESP;

const Self = @This();

reader: std.io.AnyReader,
allocator: std.heap.ArenaAllocator,

pub fn init(reader: std.io.AnyReader, allocator: std.mem.Allocator) Self {
    return Self{
        .reader = reader,
        .allocator = std.heap.ArenaAllocator.init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    self.allocator.deinit();
}

/// Blocks until a message is received.
pub fn next(self: *Self) anyerror!?RESP {
    const tag = self.reader.readByte() catch |err| {
        return if (err == error.EndOfStream) null else err;
    };

    return switch (tag) {
        '*' => try self.parse_array(),
        '+' => try self.parse_simple_string(),
        '$' => try self.parse_bulk_string(),
        ':' => try self.parse_integer(),
        else => error.Unexpected,
    };
}

fn parse_array(self: *Self) anyerror!RESP {
    const length = try self.parse_length();

    const arr = RESP{
        .array = .{
            .allocator = self.allocator.allocator(),
            .values = try self.allocator.allocator().alloc(RESP, length),
        },
    };

    for (0..length) |i| {
        if (self.next() catch null) |v| {
            arr.array.values[i] = v;
        }
    }

    return arr;
}

fn parse_simple_string(self: *Self) !RESP {
    var s = try std.ArrayList(u8).initCapacity(self.allocator.allocator(), 10);
    errdefer s.deinit();

    try self.reader.streamUntilDelimiter(s.writer(), '\r', null);
    _ = try self.reader.readByte(); // \n

    return RESP{ .string = try s.toOwnedSlice() };
}

fn parse_bulk_string(self: *Self) !RESP {
    const len = try self.parse_length();

    const s = self.allocator.allocator().alloc(u8, len) catch return error.Unexpected;
    _ = try self.reader.readAll(s);
    _ = try self.reader.readByte();
    _ = try self.reader.readByte();
    return RESP{ .string = s };
}

// :[<+|->]<value>\r\n
fn parse_integer(self: *Self) !RESP {
    var i: i64 = 0;
    var negative = false;

    while (true) {
        const v = try self.reader.readByte();
        switch (v) {
            '-' => negative = true,
            '0'...'9' => {
                i *= 10;
                i += v - '0';
            },
            '\r' => {
                _ = try self.reader.readByte(); // TODO: must be \n
                return RESP{ .integer = if (negative) -i else i };
            },
            else => return error.Unexpected,
        }
    }
}

test "parse empty stream" {
    var s = std.io.fixedBufferStream("");
    const b = s.reader().any();

    var it = Self.init(b, testing.allocator);
    try testing.expectEqualDeep(try it.next(), null);
}

test "get next after EOF" {
    var s = std.io.fixedBufferStream(":123\r\n");
    const b = s.reader().any();

    var it = Self.init(b, testing.allocator);
    _ = try it.next();
    try testing.expectEqualDeep(try it.next(), null);
}

test "parse array of simple strings" {
    var s = std.io.fixedBufferStream("*2\r\n+ECHO\r\n+hey\r\n");
    const b = s.reader().any();

    var it = Self.init(b, testing.allocator);
    defer it.deinit();
    const arr = (try it.next()).?;
    try testing.expectEqual(arr.array.values.len, 2);
    try testing.expectEqualDeep(arr.array.values[0], RESP{ .string = "ECHO" });
    try testing.expectEqualDeep(arr.array.values[1], RESP{ .string = "hey" });
}

test "parse integer" {
    var s = std.io.fixedBufferStream(":1234\r\n");
    const b = s.reader().any();

    var it = Self.init(b, testing.allocator);
    try testing.expectEqualDeep(try it.next(), RESP{ .integer = 1234 });
}

test "parse bulk string" {
    var s = std.io.fixedBufferStream("$4\r\n1234\r\n");
    const b = s.reader().any();

    var it = Self.init(b, testing.allocator);
    try testing.expectEqualDeep(try it.next(), RESP{ .string = "1234" });
    _ = try it.next();
}

test "parse simple string" {
    var s = std.io.fixedBufferStream("+foobar\r\n");
    const b = s.reader().any();

    var it = Self.init(b, testing.allocator);
    defer it.deinit();
    try testing.expectEqualDeep(try it.next(), RESP{ .string = "foobar" });
}

/// Assumes the positiong already points the first character of the length,
/// i.e. the type character has already been popped.
fn parse_length(self: *Self) !usize {
    var l: usize = 0;

    while (true) {
        const v = try self.reader.readByte();
        switch (v) {
            '0'...'9' => {
                l *= 10;
                l += v - '0';
            },
            '\r' => {
                _ = try self.reader.readByte();
                return l;
            },
            else => return error.unexpected,
        }
    }
}
