const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();

const ParserUnmanaged = @import("./resp/ParserUnmanaged.zig");
const Serializer = @import("./resp/Serializer.zig");
const Value = @import("./resp/value.zig").Value;

const Types = @import("./types.zig");
const Options = Types.Options;
const ServerState = Types.ServerState;
const Host = Types.Host;
const Role = Types.Role;

const Self = @This();

pub fn replication_handshake(master: Host, listening_port: u16, allocator: std.mem.Allocator) !net.Stream {
    var client = try init(master, allocator);

    try client.write(.{"PING"});
    try stdout.print("\nRESPONSE {any}", .{try client.receive()});

    var buf: [8]u8 = undefined;
    try client.write(.{ "REPLCONF", "listening-port", try std.fmt.bufPrint(&buf, "{}", .{listening_port}) });
    try stdout.print("\nRESPONSE {any}", .{try client.receive()});

    try client.write(.{ "REPLCONF", "capa", "eof", "capa", "psync2" });
    try stdout.print("\nRESPONSE {any}", .{try client.receive()});

    try client.write(.{ "PSYNC", "?", "-1" });
    try stdout.print("\nPSYNC RESPONSE {any}", .{try client.receive()});
    try stdout.print("\nPSYNC RESPONSE {any}", .{try client.receive_rdb()});

    return client.socket;
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
    return Serializer.write(self.socket.writer().any(), items);
}

pub fn print(self: *Self, comptime format: []const u8, args: anytype) !void {
    return self.socket.writer().print(format, args);
}

pub fn receive(self: *Self) !?Value {
    return ParserUnmanaged.parse(self.socket.reader().any(), self.allocator) catch |err| {
        if (err == error.EndOfStream) return null;

        return err;
    };
}

pub fn receive_rdb(self: *Self) ![]const u8 {
    return try ParserUnmanaged.parseRdb(self.socket.reader().any(), self.allocator);
}

pub fn close(self: *Self) void {
    self.socket.close();
}
