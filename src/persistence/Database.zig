const std = @import("std");
const testing = std.testing;

const Self = @This();

offset: usize = 0,
buffer: []const u8,
file: ?std.fs.File = null,

pub fn fromFile(absolutePath: []const u8) !Self {
    const file = try std.fs.openFileAbsolute(absolutePath, .{ .mode = .read_write });

    const db = try std.posix.mmap(
        null,
        try file.getEndPos(),
        std.posix.PROT.READ,
        std.posix.MAP{ .TYPE = .SHARED },
        file.handle,
        0,
    );

    std.debug.print("{X}", .{db});

    return .{
        .file = file,
        .buffer = db,
    };
}

pub fn fromBuffer(buffer: []const u8) Self {
    return .{
        .buffer = buffer,
    };
}

pub fn deinit(self: *Self) void {
    if (self.file) |f| {
        std.posix.munmap(@as([]align(std.mem.page_size) u8, @constCast(@alignCast(self.buffer))));
        f.close();
    }
}

pub fn getEntryIterator(self: *Self) !EntryIterator {
    const it = .{ .database = self.* };

    try self.seekDatabaseOffset();

    return it;
}

test "get entries from empty rdb" {
    const empty_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    var buffer: [88]u8 = undefined;
    const content = try std.fmt.hexToBytes(&buffer, empty_rdb);
    var db = fromBuffer(content);

    var it = try db.getEntryIterator();

    while (it.next()) |k| {
        std.debug.print("{b}\n", .{k});
    }
}

pub const EntryIterator = struct {
    database: Self,

    pub fn next(self: *@This()) ?[]const u8 {
        if (self.database.offset >= self.database.buffer.len) {
            return null;
        }

        const op = self.database.peekOpCode();
        return switch (op) {
            // TODO: read key value pairs
            OpCode.EOF => null,
            else => null,
        };
    }
};

/// Sets the offset to the first database in the file.
fn seekDatabaseOffset(self: *Self) !void {
    self.offset = 0;
    const data = self.buffer;

    // TODO: add readHeader to return info, for now let's just skip it

    _ = data[0..5]; // magic string "REDIS"
    _ = data[5..9]; // version

    self.offset = 9;

    while (self.offset < data.len) {
        switch (self.readOpCode()) {
            OpCode.Auxiliary => {
                _ = self.readField(); // key
                _ = self.readField(); // value
            },
            OpCode.DatabaseSelector => break, // we've reached the DB
            else => {},
        }
    }
}

/// Expects the current offset to point to the next op code.
/// Increments the offset.
fn readOpCode(self: *Self) OpCode {
    const op = OpCode.parse(self.buffer[self.offset]);
    self.offset += 1;
    return op;
}

fn peekOpCode(self: *Self) OpCode {
    return OpCode.parse(self.buffer[self.offset]);
}

fn readByte(self: *Self) u8 {
    const b = self.buffer[self.offset];
    self.offset += 1;
    return b;
}

fn readBytes(self: *Self, len: usize) []const u8 {
    const b = self.buffer[self.offset .. self.offset + len];
    self.offset += len;
    return b;
}

const StringEncodedValue = union(enum) {
    String: []const u8,
    U8: u8,
    U16: u16,
    U32: u32,
};

fn readField(self: *Self) StringEncodedValue {
    const byte = self.readByte();
    const flags = byte & 0b11000000;
    const payload = byte & 0b00111111;

    var len: usize = undefined;

    switch (flags) {
        0b00000000 => len = std.mem.readInt(u32, &.{ 0, 0, 0, payload }, .big),
        0b01000000 => len = std.mem.readInt(u32, &.{ 0, 0, payload, self.readByte() }, .big),
        0b10000000 => len = std.mem.readInt(u32, &.{ self.readByte(), self.readByte(), self.readByte(), self.readByte() }, .big),
        0b11000000 => {
            switch (payload) {
                0 => return .{ .U8 = self.readByte() },
                1 => return .{ .U16 = std.mem.readInt(u16, &.{ self.readByte(), self.readByte() }, .big) },
                2 => return .{ .U32 = std.mem.readInt(u32, &.{ self.readByte(), self.readByte(), self.readByte(), self.readByte() }, .big) },
                else => @panic("LZF not supported (yet)"), // TODO
            }
        },
        else => unreachable,
    }

    return .{ .String = self.readBytes(len) };
}

const OpCode = enum {
    Auxiliary,
    DatabaseSelector,
    EOF,
    ExpireTimeMs,
    ExpireTimeSeconds,
    ResizeDB,
    Unknown,

    fn parse(b: u8) OpCode {
        return switch (b) {
            0xFA => OpCode.Auxiliary,

            0xFE => OpCode.DatabaseSelector,
            0xFF => OpCode.EOF,
            0xFC => OpCode.ExpireTimeMs,
            0xFD => OpCode.ExpireTimeSeconds,
            0xFB => OpCode.ResizeDB,
            else => OpCode.Unknown,
        };
    }
};

const ValueType = enum {
    String,
    List,
    Set,
    SortedSet,
    Hash,
    Zipmap,
    Ziplist,
    IntSet,
    ZipListSortedSet,
    ZipListHash,
    QuicklistList,
};
