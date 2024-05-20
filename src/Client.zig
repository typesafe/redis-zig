const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();

const Types = @import("./types.zig");
const Options = Types.Options;
const ServerState = Types.ServerState;
const Host = Types.Host;
const Role = Types.Role;

const Self = @This();

pub fn replication_handshake(master: Host, listening_port: u16, allocator: std.mem.Allocator) !void {
    var client = try init(master, allocator);
    defer client.close();

    try client.write("*1\r\n$4\r\nPING\r\n");
    var buffer: [64]u8 = undefined;
    _ = try client.receive(&buffer);

    try client.print("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n", .{listening_port});
    _ = try client.receive(&buffer);
    try client.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
    _ = try client.receive(&buffer);
}

allocator: std.mem.Allocator,
socket: std.net.Stream,

pub fn init(host: Host, allocator: std.mem.Allocator) !Self {
    return Self{
        .allocator = allocator,
        .socket = try net.tcpConnectToHost(allocator, host.host, host.port),
    };
}

pub fn write(self: *Self, data: []const u8) !void {
    return self.socket.writer().writeAll(data);
}

pub fn print(self: *Self, comptime format: []const u8, args: anytype) !void {
    return self.socket.writer().print(format, args);
}

pub fn receive(self: *Self, buffer: []u8) !usize {
    return self.socket.read(buffer);
}

pub fn close(self: *Self) void {
    self.socket.close();
}
