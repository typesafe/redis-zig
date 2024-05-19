const std = @import("std");
const net = std.net;

const Server = @import("./Server.zig");

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    try stdout.print("Logs from your program will appear here!", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const server = try Server.init("127.0.0.1", 6379, gpa.allocator());

    try server.run();
}
