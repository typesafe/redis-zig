const std = @import("std");
const testing = std.testing;

const Value = @import("./resp/value.zig").Value;

pub const Command = union(enum) {
    Ping: void,
    Echo: []const u8,
    Wait: struct { numReplicas: usize, timeout: i64 },
    Info: struct { arg: []const u8 },
    Get: []const u8,
    Set: struct { key: []const u8, value: Value, exp: ?i64 },
    ReplConf: ReplConf,
    PSync: PSync,
    FullResync: FullResync,
    GetConfig: GetConfig,

    pub const GetConfig = struct {
        key: []const u8,
    };

    pub const FullResync = struct {
        offset: isize,
    };

    pub const ReplConf = union(enum) {
        ListeningPort: []const u8,
        Capa: []const u8,
        GetAck: []const u8,
        Ack: []const u8,
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

                if (std.ascii.startsWithIgnoreCase(v, "FULLRESYNC")) {
                    var it = std.mem.splitScalar(u8, v, ' ');
                    _ = it.next();

                    return Command{ .FullResync = .{ .offset = std.fmt.parseInt(isize, it.next().?, 10) catch 0 } };
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

                if (v.len == 3 and std.ascii.eqlIgnoreCase(v[0].String, "WAIT")) {
                    return Command{ .Wait = .{
                        .numReplicas = (std.fmt.parseInt(usize, v[1].String, 10) catch 0),
                        .timeout = (std.fmt.parseInt(i64, v[2].String, 10) catch @as(i64, 0)),
                    } };
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
                    if (std.ascii.eqlIgnoreCase(v[1].String, "GETACK")) {
                        return Command{ .ReplConf = ReplConf{ .GetAck = v[2].String } };
                    }

                    if (std.ascii.eqlIgnoreCase(v[1].String, "CAPA")) {
                        // TODO: this is a list of tuples
                        return Command{ .ReplConf = ReplConf{ .Capa = v[2].String } };
                    }

                    if (std.ascii.eqlIgnoreCase(v[1].String, "ACK")) {
                        // TODO: this is a list of tuples
                        return Command{ .ReplConf = ReplConf{ .Ack = v[2].String } };
                    }

                    // TODO: other cases
                    return Command{ .ReplConf = ReplConf{ .ListeningPort = v[2].String } };
                }

                if (std.ascii.eqlIgnoreCase(v[0].String, "CONFIG")) {
                    if (std.ascii.eqlIgnoreCase(v[1].String, "GET")) {
                        return Command{ .GetConfig = .{ .key = v[2].String } };
                    }

                    return null;
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
            .ReplConf => |v| switch (v) {
                .ListeningPort => |p| write(writer, .{ "REPLCONF", "listening-port", p }),
                .GetAck => |a| write(writer, .{ "REPLCONF", "GETACK", a }),
                .Ack => |a| write(writer, .{ "REPLCONF", "ACK", a }),
                else => write(writer, .{ "REPLCONF", "?" }),
            },
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
