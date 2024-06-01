const std = @import("std");

const Value = @import("../resp/value.zig").Value;

// XADD <stream key> <entry key> <k> <v> <k> <v>

const Self = @This();

id: []const u8,
entries: std.StringHashMap([]const [2]Value),
allocator: std.mem.Allocator,

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .id = "12312312",
        .entries = std.StringHashMap([]const [2]Value).init(allocator),
        .allocator = allocator,
    };
}

pub fn add(self: *Self, id: ?[]const u8, props: []const [2]Value) !void {
    const key = if (id) |i| i else "1526919030474-0"; // TODO: generate
    // TODO: clone
    try self.entries.put(key, props);
    return key;
}
