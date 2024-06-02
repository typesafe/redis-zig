const std = @import("std");

const Value = @import("../resp/value.zig").Value;

const Self = @This();

id: []const u8,
entries: std.ArrayList(Entry),
allocator: std.mem.Allocator,
mutex: std.Thread.Mutex,
events: std.AutoHashMap(*std.Thread.ResetEvent, void),

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
        .events = std.AutoHashMap(*std.Thread.ResetEvent, void).init(allocator),
    };
}

pub const EntryIterator = struct {
    stream: *Self,
    count: usize,
    slice: []const Entry,
    offset: usize = 0,

    pub fn next(self: *EntryIterator) ?Entry {
        if (self.offset < self.slice.len) {
            const ret = self.slice[self.offset];
            self.offset += 1;
            return ret;
        }

        self.stream.mutex.unlock();
        return null;
    }

    pub fn deinit(self: EntryIterator) void {
        self.stream.mutex.unlock();
    }
};

pub fn notify(self: *Self, event: *std.Thread.ResetEvent) !void {
    // TODO: (dedicated) mutex lock
    try self.events.put(event, {});
}

/// Returns an interator over the matching entries.
/// Locks the stream until `iterator.deinit` is called.
pub fn xrange(self: *Self, from: []const u8, to: []const u8, read: bool) !EntryIterator {
    self.mutex.lock();

    var startIndex: usize = 0;
    var endIndex = self.entries.items.len;

    // TODO: improve this with a binary search

    if (!std.mem.eql(u8, from, "-")) {
        const s = try EntryIdInput.parse(from);
        if (s.id) |id| {
            const sq = if (s.seq) |seq| seq else 0;
            for (self.entries.items[0..endIndex], 0..) |e, i| {
                if (read) {
                    startIndex = i;
                }

                if (e.id.id == id and e.id.seq == sq) {
                    startIndex = i;
                    break;
                }
            }
        }
    }
    if (read) {
        startIndex += 1;
    }

    if (!std.mem.eql(u8, to, "+")) {
        const e = try EntryIdInput.parse(to);

        var i = endIndex;
        while (i > startIndex) {
            const entry = self.entries.items[i - 1];
            if (entry.id.id == e.id) {
                if (e.seq != null) {
                    if (entry.id.seq == e.seq) {
                        endIndex = i;
                        break;
                    }
                } else {
                    endIndex = i;
                    break;
                }
            }
            i -= 1;
        }
    }
    const count = endIndex - startIndex;
    return EntryIterator{
        .stream = self,
        .count = endIndex - startIndex,
        .slice = if (count > 0) self.entries.items[startIndex..endIndex] else self.entries.items[0..0],
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

    var it = self.events.keyIterator();
    while (it.next()) |ev| {
        ev.*.set();
    }

    self.events.clearAndFree();

    return newEntryId;
}
