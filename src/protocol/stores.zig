const std = @import("std");
const testing = std.testing;

const Types = @import("./types.zig");

pub const KeyValueStore = struct {
    const Self = @This();

    map: std.StringHashMap(Types.RESP),

    pub fn init(allocator: std.mem.Allocator) Self {
        return KeyValueStore{
            .map = std.StringHashMap(Types.RESP).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        (&self.map).deinit();
    }

    pub fn set(self: *Self, key: []const u8, value: Types.RESP) !void {
        try self.map.put(key, value);
    }

    pub fn get(self: *Self, key: []const u8) !?Types.RESP {
        return self.map.get(key);
    }
};

pub const Store = struct {
    const Self = @This();

    kv: KeyValueStore,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{ .kv = KeyValueStore.init(allocator) };
    }

    pub fn deinit(self: *Self) void {
        self.kv.deinit();
    }
};
