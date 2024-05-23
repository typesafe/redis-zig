const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();

const CommandIterator = @import("./CommandIterator.zig");

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
        const stream = try Client.replication_handshake(master, self.options.port, self.allocator);
        _ = try std.Thread.spawn(.{}, handle_client, .{ stream, self.allocator, &store, &state });
    }

    var listener = try self.address.listen(.{ .reuse_address = true });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        _ = try std.Thread.spawn(.{}, handle_client, .{ connection.stream, self.allocator, &store, &state });
    }
}

fn handle_client(stream: net.Stream, allocator: std.mem.Allocator, s: *Store, state: *ServerState) !void {
    defer stream.close();

    var store = s;

    try stdout.print("accepted new connection", .{});

    var iter = CommandIterator.init(stream.reader().any(), allocator);
    defer iter.deinit();

    while (try iter.next()) |cmd| {
        try stdout.print("CMD: {any}", .{cmd});

        switch (cmd) {
            .Ping => {
                _ = try stream.writer().write("+PONG\r\n");
            },
            .Echo => |v| {
                try std.fmt.format(stream.writer(), "${}\r\n{s}\r\n", .{ v.len, v });
            },
            .Set => |v| {
                try store.kv.set(v.key, v.value, v.exp);
                if (state.role == Role.master) {
                    try std.fmt.format(stream.writer(), "+OK\r\n", .{});
                    try state.forward(cmd);
                }
            },
            .Get => |k| {
                const v = try store.kv.get(k);
                if (v) |value| {
                    try std.fmt.format(stream.writer(), "{}", .{value});
                } else {
                    try std.fmt.format(stream.writer(), "$-1\r\n", .{});
                }
            },
            .Info => |_| {
                const role = @tagName(state.role);
                try stdout.print("ROLE {s}", .{role});
                var buf: [1024]u8 = undefined;
                const info = try get_replication_info(&buf, state);
                try stream.writer().print("${}\r\n{s}\r\n", .{ info.len, info });
            },
            .ReplConf => |_| {
                try std.fmt.format(stream.writer(), "+OK\r\n", .{});
            },
            .PSync => |_| {
                try std.fmt.format(stream.writer(), "+FULLRESYNC {s} 0\r\n", .{state.master_replid.?});
                var buffer: [88]u8 = undefined;
                const content = try std.fmt.hexToBytes(&buffer, empty_rdb);
                try stream.writer().print("${}\r\n{s}", .{ content.len, content });
                state.add_replica(stream);
            },
        }
    }
}

const empty_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

fn get_replication_info(buffer: []u8, state: *ServerState) ![]u8 {
    var stream = std.io.fixedBufferStream(buffer);
    var w = stream.writer();
    try w.print("#Replication\nrole:{s}", .{@tagName(state.role)});
    if (state.role == Role.master) {
        try w.print("\nmaster_replid:{s}\nmaster_repl_offset:{}", .{ state.master_replid.?, state.master_repl_offset.? });
    }

    return stream.getWritten();
}
