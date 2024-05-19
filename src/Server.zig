const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();

const Parser = @import("./protocol/Parser.zig");

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

    var listener = try self.address.listen(.{ .reuse_address = true });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        _ = try std.Thread.spawn(.{}, handle_client, .{ connection, self.allocator });
    }
}

fn handle_client(connection: net.Server.Connection, allocator: std.mem.Allocator) !void {
    defer connection.stream.close();

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
            }
        }
    }
}
