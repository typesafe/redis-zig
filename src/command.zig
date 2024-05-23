const std = @import("std");
const testing = std.testing;

const Value = @import("./resp/value.zig").Value;

pub const Command = union(enum) {
    Ping: void,
    Echo: []const u8,
    Info: struct { arg: []const u8 },
    Get: []const u8,
    Set: struct { key: []const u8, value: Value, exp: ?i64 },
    ReplConf: ReplConf,
    PSync: PSync,

    pub const ReplConf = union(enum) {
        listenting_port: []const u8,
        capa: []const u8,
    };

    pub const PSync = struct {
        repl_id: []const u8,
        offset: isize,
    };

    /// Initializes a Command from a value.
    /// Returns null if the value does not represent a command.
    pub fn parse(value: Value) ?Command {
        return switch (value) {
            .String => |v| {
                if (std.ascii.eqlIgnoreCase(v, "PING")) {
                    return Command{ .Ping = {} };
                }

                return null;
            },
            .List => |v| {
                if (v.len == 2 and std.ascii.eqlIgnoreCase(v[0].String, "ECHO")) {
                    return Command{ .Echo = v[1].String };
                }

                if (v.len == 1 and std.ascii.eqlIgnoreCase(v[0].String, "PING")) {
                    return Command{ .Ping = {} };
                }

                if (std.ascii.eqlIgnoreCase(v[0].String, "SET")) {
                    return Command{ .Set = .{
                        .key = v[1].String,
                        .value = v[2],
                        .exp = if (v.len == 5 and std.ascii.eqlIgnoreCase(v[3].String, "PX"))
                            (std.time.milliTimestamp() + (std.fmt.parseInt(i64, v[4].String, 10) catch 0))
                        else
                            null,
                    } };
                }

                if (std.ascii.eqlIgnoreCase(v[0].String, "INFO")) {
                    return Command{ .Info = .{ .arg = v[1].String } };
                }

                if (std.ascii.eqlIgnoreCase(v[0].String, "GET")) {
                    return Command{ .Get = v[1].String };
                }

                if (std.ascii.eqlIgnoreCase(v[0].String, "REPLCONF")) {
                    // TODO: other cases
                    return Command{ .ReplConf = ReplConf{ .listenting_port = "" } };
                }

                if (std.ascii.eqlIgnoreCase(v[0].String, "PSYNC")) {
                    // TODO: other cases
                    return Command{ .PSync = PSync{ .repl_id = v[1].String, .offset = std.fmt.parseInt(i64, v[2].String, 10) catch -1 } };
                }

                return null;
            },

            else => null,
        };
    }

    pub fn format(value: @This(), comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        return switch (value) {
            .Ping => write(writer, .{"PING"}),
            .Echo => |v| write(writer, .{ "ECHO", v }),
            .Set => |v| write(writer, .{ "SET", v.key, v.value.String }),
            .Get => |v| write(writer, .{ "GET", v }),
            else => writer.print("<{s}>", .{@tagName(value)}),
        };
    }

    fn write(writer: anytype, items: anytype) !void {
        try writer.print("*{}\r\n", .{items.len});

        inline for (items) |item| {
            try writer.print("${}\r\n{s}\r\n", .{ item.len, item });
        }
    }
};
