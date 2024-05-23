const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();

const Command = @import("./command.zig").Command;

const Self = @This();

stream: net.Stream,

pub fn init(stream: net.Stream) Self {
    return .{ .stream = stream };
}

pub fn forward(self: *const Self, command: Command) !void {
    try self.stream.writer().print("{}", .{command});
}
