const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();

const CommandIterator = @import("./CommandIterator.zig");
const ParserUnmanaged = @import("./resp/ParserUnmanaged.zig");
const Value = @import("./resp/value.zig").Value;
const Command = @import("./command.zig").Command;
const Serializer = @import("./resp/Serializer.zig");
const Store = @import("./protocol/stores.zig").Store;
const Types = @import("./types.zig");
const Options = Types.Options;
const ServerState = Types.ServerState;
const Host = Types.Host;
const Role = Types.Role;

const Client = @import("./Client.zig");
const Database = @import("./persistence/Database.zig");

const Self = @This();

allocator: std.mem.Allocator,
address: net.Address,
options: Options,
listener: ?std.net.Server = null,

pub fn init(name: []const u8, options: Options, allocator: std.mem.Allocator) !Self {
    const address = try net.Address.resolveIp(name, options.port);

    return .{
        .allocator = allocator,
        .address = address,
        .options = options,
    };
}

pub fn deinit(self: Self) void {
    var l = self.listener.?;
    (&l).deinit();
}

fn loadFile(self: *Self, store: *Store) !void {
    if (self.options.dir == null or self.options.dbfilename == null) {
        return;
    }

    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const rel = try std.fmt.bufPrint(&buf, "{s}/{s}", .{ self.options.dir.?, self.options.dbfilename.? });

    const path = try std.fs.cwd().realpath(rel, &buf);
    var db = try Database.fromFile(path);

    var it = try db.getEntryIterator();

    while (it.next()) |kvp| {
        std.debug.print("{s}", .{kvp[0].String});
        try store.kv.set(kvp[0].String, Value{ .String = kvp[1].String }, kvp[2]);
    }
}

pub fn run(self: *Self) !void {
    var store = Store.init(self.allocator);
    defer store.deinit();

    loadFile(self, &store) catch {};

    var state = if (self.options.master) |_| ServerState{
        .Slave = .{},
    } else ServerState{
        .Master = .{
            .options = self.options,
            .id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", // TODO: generate
            .replicationState = Types.ReplicationState.init(self.allocator),
        },
    };

    if (self.options.master) |master| {
        const res = try Client.replication_handshake(master, self.options.port, self.allocator);
        _ = try std.Thread.spawn(.{}, handle_client, .{ res.stream, self.allocator, &store, &state });
        state.Slave.offset = res.offset;
        // TODO: is a slave supposed to listen for other client connections? Let's do that for now since it allows us to keep things up-and-running...
    }

    self.listener = try self.address.listen(.{ .reuse_address = true });
    errdefer self.listener.?.deinit();

    while (true) {
        const connection = self.listener.?.accept() catch {
            // TODO: this should be happening only because the main thread called deinit on the server.
            break;
        };
        try stdout.print("accepted new connection, spawning thread...", .{});
        _ = try std.Thread.spawn(.{}, handle_client, .{ connection.stream, self.allocator, &store, &state });
    }
}

fn handle_client(stream: net.Stream, allocator: std.mem.Allocator, s: *Store, state: *ServerState) !void {
    defer stream.close();

    var store = s;

    var iter = CommandIterator.init(stream.reader().any(), allocator);
    defer iter.deinit();

    while (try iter.next()) |value| {
        defer iter.free(value);

        if (Command.parse(value)) |cmd| {
            try stdout.print("CMD: {any}", .{cmd});

            switch (cmd) {
                .Ping => {
                    if (state.* == .Master) {
                        _ = try stream.writer().write("+PONG\r\n");
                    }
                },
                .Echo => |v| {
                    try std.fmt.format(stream.writer(), "${}\r\n{s}\r\n", .{ v.len, v });
                },
                .Keys => |_| {
                    try std.fmt.format(stream.writer(), "*{}\r\n", .{store.kv.map.count()});
                    var it = store.kv.map.keyIterator();

                    while (it.next()) |k| {
                        try std.fmt.format(stream.writer(), "${}\r\n{s}\r\n", .{ k.len, k.* });
                    }
                },
                .Set => |v| {
                    try store.kv.set(v.key, v.value, v.exp);
                    if (state.* == .Master) {
                        try std.fmt.format(stream.writer(), "+OK\r\n", .{});
                        try state.Master.replicationState.broadcast(cmd);
                        state.Master.offset += iter.lastCommandBytes;
                    }
                },
                .Get => |k| {
                    const v = try store.kv.get(k);
                    if (v) |val| {
                        try std.fmt.format(stream.writer(), "{}", .{val});
                    } else {
                        try std.fmt.format(stream.writer(), "$-1\r\n", .{});
                    }
                },
                .Info => |_| {
                    try stdout.print("ROLE {s}", .{if (state.* == .Master) "master" else "slave"});
                    var buf: [1024]u8 = undefined;
                    const info = try get_replication_info(&buf, state);
                    try stream.writer().print("${}\r\n{s}\r\n", .{ info.len, info });
                },
                .ReplConf => |rc| {
                    switch (rc) {
                        .GetAck => {
                            var buf: [8]u8 = undefined;
                            const offset = try std.fmt.bufPrint(&buf, "{}", .{state.Slave.offset});

                            try Serializer.write(stream.writer().any(), .{ "REPLCONF", "ACK", offset });
                        },
                        .Ack => |ack| {
                            if (state.Master.replicationState.getReplica(stream)) |r| {
                                r.*.offset = try std.fmt.parseInt(isize, ack, 10);
                            }
                        },
                        else => try std.fmt.format(stream.writer(), "+OK\r\n", .{}),
                    }
                },
                .GetConfig => |gc| {
                    if (std.ascii.eqlIgnoreCase(gc.key, "dir")) {
                        try Serializer.write(stream.writer().any(), .{ gc.key, state.Master.options.dir.? });
                    } else if (std.ascii.eqlIgnoreCase(gc.key, "dbfilename")) {
                        try Serializer.write(stream.writer().any(), .{ gc.key, state.Master.options.dbfilename.? });
                    } else {
                        try std.fmt.format(stream.writer().any(), "+OK\r\n", .{});
                    }
                },
                .PSync => |_| {
                    try std.fmt.format(stream.writer(), "+FULLRESYNC {s} {}\r\n", .{ state.Master.id, state.Master.offset });
                    var buffer: [88]u8 = undefined;
                    const content = try std.fmt.hexToBytes(&buffer, empty_rdb);
                    try stream.writer().print("${}\r\n{s}", .{ content.len, content });
                    try state.Master.replicationState.addReplica(stream);
                    // this is a client now, we no longer need to listen to its commands

                },
                .FullResync => |fr| {
                    const os: u64 = @intCast(fr.offset);
                    state.Slave.offset = os - iter.lastCommandBytes; // to compensate for the += below...
                },
                .Wait => |_| {
                    try handleWait(state, cmd, stream, allocator);
                },
            }
        }

        if (state.* == .Slave) {
            state.Slave.offset += iter.lastCommandBytes;
        }
    }

    if (state.* == .Master) {
        if (state.*.Master.replicationState.removeReplica(stream)) {
            try stdout.print("Removed replica...", .{});
        }
    }
    try stdout.print("Exiting read loop...", .{});
}

const empty_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

fn get_replication_info(buffer: []u8, state: *ServerState) ![]u8 {
    var stream = std.io.fixedBufferStream(buffer);
    var w = stream.writer();
    try w.print("#Replication\nrole:{s}", .{if (state.* == .Master) "master" else "slave"});
    if (state.* == .Master) {
        try w.print("\nmaster_replid:{s}\nmaster_repl_offset:{}", .{ state.Master.id, state.Master.offset });
    }

    return stream.getWritten();
}

fn handleWait(state: *ServerState, wait: Command, stream: net.Stream, _: std.mem.Allocator) !void {
    const expectedOffset = state.Master.offset;
    const expectedAcknowledgements = wait.Wait.numReplicas;
    const deadline = std.time.milliTimestamp() + wait.Wait.timeout;

    try state.Master.replicationState.broadcast(Command{ .ReplConf = .{ .GetAck = "*" } });

    while (deadline > std.time.milliTimestamp()) {
        const acks = state.Master.replicationState.countUpToDate(expectedOffset);

        if (acks >= expectedAcknowledgements) {
            _ = try stream.writer().print(":{}\r\n", .{acks});
            return;
        }
    }

    _ = try stream.writer().print(":{}\r\n", .{state.Master.replicationState.countUpToDate(expectedOffset)});
}
