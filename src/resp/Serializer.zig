const std = @import("std");
const Value = @import("./value.zig").Value;

pub fn write(writer: std.io.AnyWriter, items: anytype) !void {
    try writer.print("*{}\r\n", .{items.len});

    inline for (items) |item| {
        switch (@typeInfo(@TypeOf(item))) {
            .Int => try writer.print(":{}\r\n", .{item}),
            .Pointer => |ptr| {
                switch (ptr.child) {
                    Value => try writer.print("{}", .{Value{ .List = item }}),
                    u8 => try writer.print("${}\r\n{s}\r\n", .{ item.len, item }),
                    else => try writer.print("${}\r\n{s}\r\n", .{ item.len, item }),
                }
            },
            else => try writer.print("${}\r\n{s}\r\n", .{ item.len, item }),
        }
    }
}
