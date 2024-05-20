const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();

const Parser = @import("./protocol/Parser.zig");
const Store = @import("./protocol/stores.zig").Store;
const Types = @import("./types.zig");
const Options = Types.Options;
const ServerState = Types.ServerState;
const Host = Types.Host;
const Role = Types.Role;

const Client = @import("./Client.zig");

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

    if (self.options.master) |master| {
        try Client.replication_handshake(master, self.options.port, self.allocator);
    }

    var listener = try self.address.listen(.{ .reuse_address = true });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        _ = try std.Thread.spawn(.{}, handle_client, .{ connection, self.allocator, &store, &state });
    }
}

fn handle_client(connection: net.Server.Connection, allocator: std.mem.Allocator, s: *Store, state: *ServerState) !void {
    defer {
        connection.stream.close();
    }
    var store = s;

    try stdout.print("accepted new connection", .{});

    var iter = Parser.get_commands(connection.stream.reader().any(), allocator);
    defer iter.deinit();

    while (try iter.next()) |msg| {
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
                .replconf => |_| {
                    try std.fmt.format(connection.stream.writer(), "+OK\r\n", .{});
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
