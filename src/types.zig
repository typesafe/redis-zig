const std = @import("std");
const Replica = @import("./Replica.zig");
const Command = @import("./command.zig").Command;

pub const ServerState = struct {
    role: Role = Role.master,
    master_replid: ?[]const u8 = null,
    master_repl_offset: ?u16 = null,
    replicas: [5]Replica = undefined,
    replica_count: u8 = 0,
    offset: u64 = 0,
    // connected_slaves: u16,
    // second_repl_offset: i16 = -1,
    // repl_backlog_active: u16 = 0,
    // repl_backlog_size: usize = 1048576,
    // repl_backlog_first_byte_offset: usize = 0,
    // repl_backlog_histlen: ?usize,

    pub fn add_replica(self: *@This(), stream: std.net.Stream, allocator: std.mem.Allocator) !void {
        const r = Replica.init(stream, allocator);
        self.replicas[self.replica_count] = r;
        self.replica_count += 1;

        // TODO: this should be recurring...
        //try r.getAck();
    }

    pub fn forward(self: *@This(), cmd: Command) !void {
        // TODO sync with lock
        for (0..self.replica_count) |i| {
            try self.replicas[i].forward(cmd);
        }
    }
};

pub const Role = enum {
    master,
    slave,
};

pub const Options = struct { port: u16, master: ?Host = null };

pub const Host = struct {
    host: []const u8,
    port: u16,
};
