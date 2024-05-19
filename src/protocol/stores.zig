const std = @import("std");
const testing = std.testing;

const Types = @import("./types.zig");

pub const KeyValueStore = struct {
    const Self = @This();

    map: std.StringHashMap(Entry),

    pub const Entry = struct { value: Types.RESP, exp: ?i64 };

    pub fn init(allocator: std.mem.Allocator) Self {
        return KeyValueStore{
            .map = std.StringHashMap(Entry).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        (&self.map).deinit();
    }

    pub fn set(self: *Self, key: []const u8, value: Types.RESP, exp: ?i64) !void {
        try self.map.put(key, .{ .value = value, .exp = exp });
    }

    pub fn get(self: *Self, key: []const u8) !?Types.RESP {
        const r = self.map.get(key);
        if (r) |entry| {
            if (entry.exp) |exp| {
                if (exp < std.time.milliTimestamp()) {
                    _ = self.map.remove(key);
                    return null;
                }
            }
            return entry.value;
        } else {
            return null;
        }
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
