const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();

const Parser = @import("./protocol/Parser.zig");
const Store = @import("./protocol/stores.zig").Store;

const Self = @This();

allocator: std.mem.Allocator,
address: net.Address,
options: Options,

pub fn init(name: []const u8, options: Options, allocator: std.mem.Allocator) !Self {
    const address = try net.Address.resolveIp(name, options.port);

    return .{
        .allocator = allocator,
        .address = address,
        .options = options,
    };
}

pub fn run(self: Self) !void {
    try stdout.print("Logs from your program will appear here!", .{});

    var store = Store.init(self.allocator);
    defer store.deinit();

    var state = if (self.options.master) |_| ServerState{
        .role = Role.slave,
    } else ServerState{
        .role = Role.master,
        .master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", // TODO: generate
        .master_repl_offset = 0,
    };

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
                    var buf: [1024]u8 = undefined;
                    const info = try get_replication_info(&buf, state);
                    try connection.stream.writer().print("${}\r\n{s}\r\n", .{ info.len, info });
                },
            }
        }
    }
}

fn get_replication_info(buffer: []u8, state: *ServerState) ![]u8 {
    var stream = std.io.fixedBufferStream(buffer);
    var w = stream.writer();
    try w.print("#Replication\nrole:{s}", .{@tagName(state.role)});
    if (state.role == Role.master) {
        try w.print("\nmaster_replid:{s}\nmaster_repl_offset:{}", .{ state.master_replid.?, state.master_repl_offset.? });
    }

    return stream.getWritten();
}

pub const ServerState = struct {
    role: Role = Role.master,
    master_replid: ?[]const u8 = null,
    master_repl_offset: ?u16 = null,

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

pub const Options = struct { port: u16, master: ?Host = null };

pub const Host = struct {
    host: []const u8,
    port: u16,
};
