const std = @import("std");
const testing = std.testing;

const Types = @import("./types.zig");
const Command = Types.Command;
const RESP = Types.RESP;
const Self = @This();

allocator: std.mem.Allocator,
input: []const u8,
position: usize = 0,

pub fn parse(input: []const u8, allocator: std.mem.Allocator) !RESP {
    var parser = Self{ .input = input, .allocator = allocator };
    return (&parser).parse_resp();
}

fn peek(self: *Self) !u8 {
    return if (self.position >= self.input.len) error.eof else self.input[self.position];
}

fn pop(self: *Self, expected: ?u8) !u8 {
    const c = try self.peek();

    if (expected) |e| {
        if (e != c) {
            return error.unexpected;
        }
    }

    self.position += 1;
    return c;
}

fn parse_resp(self: *Self) error{ eof, unexpected, OutOfMemory }!RESP {
    return switch (try self.peek()) {
        '*' => self.parse_array(),
        '+', '$' => self.parse_string(),
        ':' => self.parse_integer(),
        else => error.unexpected,
    };
}

/// Assumes the positiong already points the first character of the length,
/// i.e. the type character has already been popped.
fn parse_length(self: *Self) !usize {
    var l: usize = 0;

    while (true) {
        const v = try self.pop(null);
        switch (v) {
            '0'...'9' => {
                l *= 10;
                l += v - '0';
            },
            '\r' => {
                _ = try self.pop('\n');
                return l;
            },
            else => return error.unexpected,
        }
    }
}

fn parse_array(self: *Self) error{ eof, unexpected, OutOfMemory }!RESP {
    _ = try self.pop('*');

    const length = try self.parse_length();

    const arr = RESP{
        .array = .{
            .allocator = self.allocator,
            .values = try self.allocator.alloc(RESP, length),
        },
    };

    for (0..length) |i| {
        arr.array.values[i] = try self.parse_resp();
    }

    return arr;
}

test "parse array of integers" {
    const arr = try Self.parse("*2\r\n:1\r\n:2\r\n", testing.allocator);
    defer arr.deinit();

    try testing.expectEqual(arr.array.values.len, 2);
    try testing.expectEqualDeep(arr.array.values[0], RESP{ .integer = 1 });
    try testing.expectEqualDeep(arr.array.values[1], RESP{ .integer = 2 });
}

test "parse array of bulk strings" {
    const arr = try Self.parse("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n", testing.allocator);
    defer arr.deinit();

    try testing.expectEqual(arr.array.values.len, 2);
    try testing.expectEqualDeep(arr.array.values[0], RESP{ .string = "ECHO" });
    try testing.expectEqualDeep(arr.array.values[1], RESP{ .string = "hey" });
}

test "parse array of simple strings" {
    const arr = try Self.parse("*2\r\n+ECHO\r\n+hey\r\n", testing.allocator);
    defer arr.deinit();

    try testing.expectEqual(arr.array.values.len, 2);
    try testing.expectEqualDeep(arr.array.values[0], RESP{ .string = "ECHO" });
    try testing.expectEqualDeep(arr.array.values[1], RESP{ .string = "hey" });
}

fn pop_terminator(self: *Self) !void {
    _ = try self.pop('\r');
    _ = try self.pop('\n');
}

// :[<+|->]<value>\r\n
fn parse_integer(self: *Self) !RESP {
    _ = try self.pop(':');

    var i: i64 = 0;
    var negative = false;

    while (true) {
        const v = try self.pop(null);
        switch (v) {
            '-' => negative = true,
            '0'...'9' => {
                i *= 10;
                i += v - '0';
            },
            '\r' => {
                _ = try self.pop('\n');
                return RESP{ .integer = if (negative) -i else i };
            },
            else => return error.unexpected,
        }
    }
}

test "parse integer" {
    try testing.expectEqualDeep(try Self.parse(":123\r\n", testing.allocator), RESP{ .integer = 123 });
    try testing.expectEqualDeep(try Self.parse(":-123\r\n", testing.allocator), RESP{ .integer = -123 });
}

fn parse_string(self: *Self) error{ eof, unexpected, OutOfMemory }!RESP {
    const c = try self.pop(null);
    const resp: error{ eof, unexpected, OutOfMemory }!RESP = switch (c) {
        // +<string>\r\n
        '+' => {
            if (std.mem.indexOf(u8, self.input[self.position..], "\r")) |end| {
                const s = try self.allocator.alloc(u8, end - self.position);
                const r = RESP{ .string = s };
                std.mem.copyForwards(u8, s, self.input[self.position..(self.position + end)]);
                self.position += end;
                try self.pop_terminator();
                return r;
            } else {
                return error.unexpected;
            }
        },
        // $<length>\r\n<data>\r\n
        '$' => {
            const len = try self.parse_length();
            const s = try self.allocator.alloc(u8, len);
            const r = RESP{ .string = s };
            std.mem.copyForwards(u8, s, self.input[self.position..(self.position + len)]);

            self.position += len;
            try self.pop_terminator();
            return r;
        },
        else => error.unexpected,
    };

    try self.pop_terminator();
    return resp;
}

test "parse simple string" {
    try testing.expectEqualDeep(try Self.parse("+OK\r\n", testing.allocator), RESP{ .string = "OK" });
    try testing.expectEqualDeep(try Self.parse("+FOOOO\r\n", testing.allocator), RESP{ .string = "FOOOO" });
}

test "parse bulk string" {
    try testing.expectEqualDeep(try Self.parse("$2\r\nOK\r\n", testing.allocator), RESP{ .string = "OK" });
    try testing.expectEqualDeep(try Self.parse("$6\r\nfoobar\r\n", testing.allocator), RESP{ .string = "foobar" });
}

test "parse ECHO command" {
    var p = try Self.parse("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n", testing.allocator);
    defer p.deinit();

    try testing.expectEqualDeep(p.to_command().?, Command{ .echo = "hey" });
}

test "parse PING command" {
    var p = try Self.parse("+PING\r\n", testing.allocator);
    defer p.deinit();

    try testing.expectEqualDeep(p.to_command().?, Command{ .ping = {} });
}
