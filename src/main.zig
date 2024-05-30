const std = @import("std");
const net = std.net;
const signal = @import("signal");

const Server = @import("./Server.zig");
const Types = @import("./types.zig");

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    try stdout.print("Parsing options...\n", .{});
    const options = parse_argv();

    try stdout.print("Starting {s}...\n", .{if (options.master) |_| "slave" else "master"});

    var server = try Server.init("127.0.0.1", options, gpa.allocator());

    const t = try std.Thread.spawn(.{}, listen, .{&server});

    try stdout.print("Accepting connections. Press any key to exit.\n", .{});

    _ = try std.io.getStdIn().reader().readByte();
    server.deinit();

    try stdout.print("Exiting...\n", .{});

    t.join();
}

fn listen(server: *Server) !void {
    try server.run();
}

// var to avoid cannot asign to comptime value issues
var DEFAULT_PORT: u16 = 6379;

fn parse_argv() Types.Options {
    var options = Types.Options{ .port = @as(u16, DEFAULT_PORT) };

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
