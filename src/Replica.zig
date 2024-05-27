const std = @import("std");
const net = std.net;
const stdout = std.io.getStdOut().writer();

const Command = @import("./command.zig").Command;
const ParserUnmanaged = @import("./resp/ParserUnmanaged.zig");
const Serializer = @import("./resp/Serializer.zig");

const Self = @This();

stream: net.Stream,
allocator: std.mem.Allocator,

pub fn init(stream: net.Stream, allocator: std.mem.Allocator) Self {
    return .{ .stream = stream, .allocator = allocator };
}

pub fn forward(self: *const Self, command: Command) !void {
    try self.stream.writer().print("{}", .{command});
}

pub fn getAck(self: *const Self) !void {
    try Serializer.write(self.stream.writer().any(), .{ "REPLCONF", "GETACK", "*" });

    const res = try ParserUnmanaged.parse(self.stream.reader().any(), self.allocator);
    if (std.mem.eql(u8, res.List[0].String, "REPLCONF")) {
        // OK
    } else {
        @panic("Un expected reply");
    }

    ParserUnmanaged.free(res, self.allocator);
}
