const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();

const Parser = @import("./protocol/Parser.zig");

const RESP = @import("./protocol/types.zig").RESP;

const Types = @import("./types.zig");
const Options = Types.Options;
const ServerState = Types.ServerState;
const Host = Types.Host;
const Role = Types.Role;

const Self = @This();

pub fn replication_handshake(master: Host, listening_port: u16, allocator: std.mem.Allocator) !void {
    var client = try init(master, allocator);
    defer client.close();

    try client.write(.{"PING"});
    try stdout.print("\nRESPONSE {any}", .{try client.receive()});

    var buf: [8]u8 = undefined;
    try client.write(.{ "REPLCONF", "listening-port", try std.fmt.bufPrint(&buf, "{}", .{listening_port}) });
    try stdout.print("\nRESPONSE {any}", .{try client.receive()});

    try client.write(.{ "REPLCONF", "capa", "eof", "capa", "psync2" });
    try stdout.print("\nRESPONSE {any}", .{try client.receive()});

    try client.write(.{ "PSYNC", "?", "-1" });
    try stdout.print("\nRESPONSE {any}", .{try client.receive()});
}

allocator: std.mem.Allocator,
socket: std.net.Stream,

pub fn init(host: Host, allocator: std.mem.Allocator) !Self {
    const socket = try net.tcpConnectToHost(allocator, host.host, host.port);
    return Self{
        .allocator = allocator,
        .socket = socket,
    };
}

pub fn write(self: *Self, items: anytype) !void {
    const w = self.socket.writer();
    try w.print("*{}\r\n", .{items.len});

    inline for (items) |item| {
        try w.print("${}\r\n{s}\r\n", .{ item.len, item });
    }
}

pub fn print(self: *Self, comptime format: []const u8, args: anytype) !void {
    return self.socket.writer().print(format, args);
}

pub fn receive(self: *Self) !?RESP {
    var it = Parser.get_commands(self.socket.reader().any(), self.allocator);

    return it.next();
}

pub fn close(self: *Self) void {
    self.socket.close();
}
