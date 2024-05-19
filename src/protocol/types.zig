const std = @import("std");
const testing = std.testing;

pub const RESP = union(enum) {
    array: struct { values: []RESP, allocator: std.mem.Allocator },
    string: []const u8,
    integer: i64,
    double: f64,
    big_number: []const u8,
    err: struct { kind: []const u8, message: []const u8 },

    pub fn to_command(self: RESP) ?Command {
        return switch (self) {
            .string => |v| {
                if (std.ascii.eqlIgnoreCase(v, "PING")) {
                    return Command{ .ping = {} };
                }

                return null;
            },
            .array => |v| {
                if (v.values.len == 2 and std.ascii.eqlIgnoreCase(v.values[0].string, "ECHO")) {
                    return Command{ .echo = v.values[1].string };
                }

                if (v.values.len == 1 and std.ascii.eqlIgnoreCase(v.values[0].string, "PING")) {
                    return Command{ .ping = {} };
                }

                if (std.ascii.eqlIgnoreCase(v.values[0].string, "SET")) {
                    return Command{ .set = .{
                        .key = v.values[1].string,
                        .value = v.values[2],
                        .exp = if (v.values.len == 5 and std.ascii.eqlIgnoreCase(v.values[3].string, "PX"))
                            (std.time.milliTimestamp() + (std.fmt.parseInt(i64, v.values[4].string, 10) catch 0))
                        else
                            null,
                    } };
                }

                if (std.ascii.eqlIgnoreCase(v.values[0].string, "GET")) {
                    return Command{ .get = v.values[1].string };
                }

                return null;
            },

            else => null,
        };
    }

    pub fn format(value: @This(), comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        return switch (value) {
            .string => |v| writer.print("${}\r\n{s}\r\n", .{ v.len, v }),
            .integer => |v| writer.print(":{}\r\n", .{v}),
            else => writer.print("<{s}>", .{@tagName(value)}),
        };
    }

    pub fn deinit(self: RESP) void {
        switch (self) {
            .string => {
                //self.string.allocator.
            },
            .array => {
                self.array.allocator.free(self.array.values);
            },
            else => {},
        }
    }
};

pub const Command = union(enum) {
    ping: void,
    echo: []const u8,
    get: []const u8,
    set: struct { key: []const u8, value: RESP, exp: ?i64 },

    pub fn format(value: @This(), comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        return switch (value) {
            .ping => writer.print("PING", .{}),
            .echo => |v| writer.print("ECHO {s}", .{v}),
            else => writer.print("<{s}>", .{@tagName(value)}),
        };
    }
};
