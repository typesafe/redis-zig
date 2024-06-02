const std = @import("std");

const Value = @import("../resp/value.zig").Value;

const Self = @This();

id: []const u8,
entries: std.ArrayList(Entry),
allocator: std.mem.Allocator,
mutex: std.Thread.Mutex,

pub const Entry = struct { id: EntryId, props: []const Value };
pub const EntryId = struct { id: u64, seq: usize };
pub const EntryIdInput = struct {
    id: ?u64,
    seq: ?usize,

    pub fn parse(value: []const u8) !EntryIdInput {
        if (std.mem.eql(u8, value, "*")) {
            return .{
                .id = null,
                .seq = null,
            };
        }

        var it = std.mem.splitScalar(u8, value, '-');
        return .{
            .id = if (it.next()) |id| try std.fmt.parseInt(u64, id, 10) else null,
            .seq = if (it.next()) |seq| (if (std.mem.eql(u8, seq, "*")) null else try std.fmt.parseInt(u64, seq, 10)) else null,
        };
    }
};

pub fn init(allocator: std.mem.Allocator) Self {
    return .{
        .id = "12312312",
        .entries = std.ArrayList(Entry).init(allocator),
        .allocator = allocator,
        .mutex = std.Thread.Mutex{},
    };
}

pub fn add(self: *Self, id: EntryIdInput, props: []const Value) !EntryId {
    self.mutex.lock();
    defer self.mutex.unlock();

    var newEntryId = EntryId{
        .id = if (id.id) |v| v else @bitCast(std.time.milliTimestamp()),
        .seq = if (id.seq) |v| v else if (id.id == 0) 1 else 0,
    };

    if (newEntryId.id == 0 and newEntryId.seq == 0) {
        return error.IdMustBeGreaterThanZero;
    }

    if (self.entries.items.len > 0) {
        const latestId = self.entries.items[self.entries.items.len - 1].id;
        if (newEntryId.id < latestId.id or newEntryId.id == latestId.id and newEntryId.seq <= latestId.seq) {
            if (id.seq == null) {
                newEntryId.seq = latestId.seq + 1;
            } else {
                return error.IdLessThenLatest;
            }
        }
    }

    const entryProps = try self.allocator.alloc(Value, props.len);
    for (props, 0..) |p, i| {
        entryProps[i] = try p.copy(self.allocator);
    }
    const e = try self.entries.addOne();
    e.* = .{ .id = newEntryId, .props = entryProps };
    return newEntryId;
}
