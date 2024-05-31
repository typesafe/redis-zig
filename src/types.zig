const std = @import("std");

const Replica = @import("./Replica.zig");
const Command = @import("./command.zig").Command;

pub const ReplicationState = struct {
    const Self = @This();

    replicas: std.AutoHashMap(std.posix.socket_t, Replica),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .replicas = std.AutoHashMap(std.posix.socket_t, Replica).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn broadcast(self: *@This(), cmd: Command) !void {
        // TODO sync with lock
        var it = self.replicas.valueIterator();
        while (it.next()) |r| {
            try r.write(cmd);
        }
    }

    pub fn countUpToDate(self: *@This(), offset: u64) usize {
        var count: usize = 0;
        var it = self.replicas.valueIterator();
        while (it.next()) |r| {
            if (r.*.offset >= offset)
                count += 1;
        }
        return count;
    }

    pub fn addReplica(self: *Self, stream: std.net.Stream) !void {
        try self.replicas.put(stream.handle, Replica.init(stream, self.allocator));
    }

    pub fn getReplica(self: *Self, stream: std.net.Stream) ?*Replica {
        return self.replicas.getPtr(stream.handle);
    }

    pub fn removeReplica(self: *Self, stream: std.net.Stream) bool {
        return self.replicas.remove(stream.handle);
    }
};

pub const ServerState = union(enum) {
    Master: MasterState,
    Slave: SlaveState,

    const MasterState = struct {
        offset: u64 = 0,
        id: []const u8,
        replicationState: ReplicationState,
        options: Options,
    };

    const SlaveState = struct {
        offset: u64 = 0,
    };

    // connected_slaves: u16,
    // second_repl_offset: i16 = -1,
    // repl_backlog_active: u16 = 0,
    // repl_backlog_size: usize = 1048576,
    // repl_backlog_first_byte_offset: usize = 0,
    // repl_backlog_histlen: ?usize,

};

pub const Role = enum {
    master,
    slave,
};

pub const Options = struct {
    port: u16,
    master: ?Host = null,
    dir: ?[]const u8 = null,
    dbfilename: ?[]const u8 = null,
};

pub const Host = struct {
    host: []const u8,
    port: u16,
};
