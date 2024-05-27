const std = @import("std");

pub fn write(writer: std.io.AnyWriter, items: anytype) !void {
    try writer.print("*{}\r\n", .{items.len});

    inline for (items) |item| {
        switch (@typeInfo(@TypeOf(item))) {
            .Int => try writer.print(":{}\r\n", .{item}),
            else => try writer.print("${}\r\n{s}\r\n", .{ item.len, item }),
        }
    }
}
