const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();

const Parser = @import("./protocol/Parser.zig");
const Store = @import("./protocol/stores.zig").Store;

const Self = @This();

allocator: std.mem.Allocator,
address: net.Address,

pub fn init(name: []const u8, port: u16, allocator: std.mem.Allocator) !Self {
    const address = try net.Address.resolveIp(name, port);

    return .{
        .allocator = allocator,
        .address = address,
    };
}

pub fn run(self: Self) !void {
    try stdout.print("Logs from your program will appear here!", .{});

    var store = Store.init(self.allocator);
    defer store.deinit();

    var state = ServerState{};

    var listener = try self.address.listen(.{ .reuse_address = true });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        _ = try std.Thread.spawn(.{}, handle_client, .{ connection, self.allocator, &store, &state });
    }
}

fn handle_client(connection: net.Server.Connection, allocator: std.mem.Allocator, s: *Store, state: *ServerState) !void {
    defer connection.stream.close();

    var store = s;

    try stdout.print("accepted new connection", .{});

    const reader = connection.stream.reader();
    var buffer: [1024]u8 = undefined;
    while (try reader.read(&buffer) > 0) {
        try stdout.print("RAW: {s}", .{buffer});

        const msg = try Parser.parse(&buffer, allocator);

        try stdout.print("RESP: {any}", .{msg});

        if (msg.to_command()) |cmd| {
            try stdout.print("CMD: {any}", .{cmd});

            switch (cmd) {
                .ping => {
                    _ = try connection.stream.writer().write("+PONG\r\n");
                },
                .echo => |v| {
                    try std.fmt.format(connection.stream.writer(), "${}\r\n{s}\r\n", .{ v.len, v });
                },
                .set => |v| {
                    try store.kv.set(v.key, v.value, v.exp);
                    try std.fmt.format(connection.stream.writer(), "+OK\r\n", .{});
                },
                .get => |k| {
                    const v = try store.kv.get(k);
                    if (v) |value| {
                        try std.fmt.format(connection.stream.writer(), "{}", .{value});
                    } else {
                        try std.fmt.format(connection.stream.writer(), "$-1\r\n", .{});
                    }
                },
                .info => |_| {
                    const role = @tagName(state.role);
                    try stdout.print("ROLE {s}", .{role});
                    try std.fmt.format(connection.stream.writer(), "${}\r\n#Replication\nrole:{s}\r\n", .{ 12 + 1 + 5 + role.len, role });
                },
            }
        }
    }
}

pub const ServerState = struct {
    role: Role = Role.master,
    // connected_slaves: u16,
    // master_replid: []const u8, //8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
    // master_repl_offset: u16 = 0,
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
