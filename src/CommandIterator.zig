//! Turns a reader into an command iterator.

const std = @import("std");
const testing = std.testing;

const ParserUnmanaged = @import("./resp/ParserUnmanaged.zig");
const Value = @import("./resp/value.zig").Value;
const Command = @import("./command.zig").Command;

const Self = @This();

reader: std.io.CountingReader(std.io.AnyReader),
arena: std.heap.ArenaAllocator,
lastCommandBytes: u64 = 0,

pub fn init(reader: std.io.AnyReader, allocator: std.mem.Allocator) Self {
    return Self{
        .reader = std.io.countingReader(reader),
        .arena = std.heap.ArenaAllocator.init(allocator),
    };
}

/// Releases all allocated memory of all values returned by the itereator.
/// Memory of returned values can also be released per instance.
pub fn deinit(self: *Self) void {
    self.arena.deinit();
}

pub fn free(self: *Self, value: Value) void {
    ParserUnmanaged.free(value, self.arena.allocator());
}

test "deinit returned values AND iterator" {
    var it = Self.init(getTestReader("$4\r\n1234\r\n$4\r\n1234\r\n"), testing.allocator);
    defer it.deinit();

    var count: u8 = 0;
    while (try it.next()) |command| {
        defer it.free(command);

        count += 1;
    }

    try testing.expect(count == 2);
}

test "deinit iterator only" {
    var it = Self.init(getTestReader("$4\r\n1234\r\n$4\r\n1234\r\n"), testing.allocator);

    _ = (try it.next());
    _ = (try it.next());

    it.deinit();
}

/// Blocks until a message is received. Return null when the reader is closed.s
pub fn next(self: *Self) anyerror!?Command {
    const before = self.reader.bytes_read;
    const v = ParserUnmanaged.parse(self.reader.reader().any(), self.arena.allocator()) catch |err| {
        if (err == error.EndOfStream) return null;

        return err;
    };

    self.lastCommandBytes = self.reader.bytes_read - before;
    return Command.parse(v);
}

test "next should return null on end of stream" {
    var it = Self.init(getTestReader("$4\r\n1234\r\n$4\r\n1234\r\n"), testing.allocator);
    defer it.deinit();

    _ = try it.next();
    _ = try it.next();

    try testing.expect((try it.next()) == null);
}

test "next should return null for empty stream" {
    var it = Self.init(getTestReader(""), testing.allocator);
    defer it.deinit();

    try testing.expect((try it.next()) == null);
}

test "get next after EOF" {
    var it = Self.init(getTestReader(":123\r\n"), testing.allocator);
    defer it.deinit();

    _ = try it.next();

    try testing.expectEqual(try it.next(), null);
}

// inlined to make sure the test code get a stable reference on the stack
inline fn getTestReader(comptime string: []const u8) std.io.AnyReader {
    var s = std.io.fixedBufferStream(string);
    return s.reader().any();
}
