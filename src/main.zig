const std = @import("std");
const net = std.net;

const Server = @import("./Server.zig");

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    try stdout.print("Logs from your program will appear here!", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const options = parse_argv();

    const server = try Server.init("127.0.0.1", options, gpa.allocator());

    try server.run();
}

// var to avoid cannot asign to comptime value issues
var DEFAULT_PORT: u16 = 6379;

fn parse_argv() Server.Options {
    var options = Server.Options{ .port = @as(u16, DEFAULT_PORT) };

    for (std.os.argv, 0..) |arg, i| {
        if (std.mem.eql(u8, arg[0..std.mem.len(arg)], "--port") and i + 1 < std.os.argv.len) {
            const portArg = std.os.argv[i + 1];

            options.port = (std.fmt.parseInt(u16, portArg[0..std.mem.len(portArg)], 10) catch DEFAULT_PORT);
        } else if (std.mem.eql(u8, arg[0..std.mem.len(arg)], "--replicaof") and i + 1 < std.os.argv.len) {
            const a = std.os.argv[i + 1];
            var replicaOf = std.mem.splitScalar(u8, a[0..std.mem.len(a)], ' ');
            const host = replicaOf.next().?;
            const port = replicaOf.next();

            options.master = .{
                .host = host,
                .port = if (port) |p| (std.fmt.parseInt(u16, p, 10) catch DEFAULT_PORT) else DEFAULT_PORT,
            };
        }
    }

    return options;
}
