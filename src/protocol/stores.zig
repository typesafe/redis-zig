const std = @import("std");
const testing = std.testing;

const Value = @import("../resp/value.zig").Value;

pub const KeyValueStore = struct {
    const Self = @This();

    map: std.StringHashMap(Entry),
    arena: std.heap.ArenaAllocator,

    pub const Entry = struct { value: Value, exp: ?i64 };

    pub fn init(allocator: std.mem.Allocator) Self {
        const arena = std.heap.ArenaAllocator.init(allocator);
        return KeyValueStore{
            .arena = arena,
            .map = std.StringHashMap(Entry).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.arena.deinit();
        (&self.map).deinit();
    }

    pub fn set(self: *Self, key: []const u8, value: Value, exp: ?i64) !void {
        const k = try self.arena.allocator().alloc(u8, key.len);
        @memcpy(k, key);

        try self.map.put(
            k,
            .{ .value = try value.copy(self.arena.allocator()), .exp = exp },
        );
    }

    pub fn get(self: *Self, key: []const u8) !?Value {
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
