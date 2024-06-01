const std = @import("std");
const testing = std.testing;

/// Represents a generic RESP value.
pub const Value = union(enum) {
    Nil: void,
    List: []const Value,
    String: []const u8,
    Bool: bool,
    Integer: i64,
    Double: f64,
    //Bignum: std.math.big.int.Managed,
    // Set: []Self ,
    // Map: [][2]*DynamicReply,

    pub fn format(value: @This(), comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        return switch (value) {
            .List => |items| {
                try writer.print("*{}\r\n", .{items.len});

                for (items) |item| {
                    try writer.print("{}", .{item});
                }
            },
            .String => |v| try writer.print("${}\r\n{s}\r\n", .{ v.len, v }),
            .Integer => |v| try writer.print(":{}\r\n", .{v}),
            else => unreachable,
        };
    }

    pub fn getRedisTypeName(value: @This()) []const u8 {
        //string, list, set, zset, hash and stream
        return switch (value) {
            .List => "list",
            .String => "string",

            else => unreachable,
        };
    }

    pub fn copy(value: Value, allocator: std.mem.Allocator) !Value {
        return switch (value) {
            .List => |list| {
                const cpy = try allocator.alloc(Value, list.len);

                for (list, 0..) |item, i| {
                    cpy[i] = try item.copy(allocator);
                }
                return Value{ .List = cpy };
            },
            .String => |v| {
                const cpy = try allocator.alloc(u8, v.len);
                @memcpy(cpy, value.String);
                return Value{ .String = cpy };
            },

            else => return value,
        };
    }
};

test "format Integer" {
    try testFormat(":123\r\n", Value{ .Integer = 123 });
    try testFormat(":-123\r\n", Value{ .Integer = -123 });
    try testFormat(":0\r\n", Value{ .Integer = 0 });
}

test "format String" {
    try testFormat("$3\r\nfoo\r\n", Value{ .String = "foo" });
}

test "format List of string" {
    try testFormat(
        "*2\r\n$3\r\nfoo\r\n$3\r\nfoo\r\n",
        Value{ .List = &[_]Value{ Value{ .String = "foo" }, Value{ .String = "foo" } } },
    );
}

test "format List of Lists" {
    try testFormat(
        "*2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nfoo\r\n*2\r\n$3\r\nfoo\r\n$3\r\nfoo\r\n",
        Value{ .List = &[_]Value{
            Value{ .List = &[_]Value{ Value{ .String = "foo" }, Value{ .String = "foo" } } },
            Value{ .List = &[_]Value{ Value{ .String = "foo" }, Value{ .String = "foo" } } },
        } },
    );
}

fn testFormat(comptime expected: []const u8, value: Value) !void {
    var buffer = std.ArrayList(u8).init(testing.allocator);
    defer buffer.deinit();

    try buffer.writer().print("{}", .{value});

    try testing.expectEqualStrings(expected, buffer.items);
}
