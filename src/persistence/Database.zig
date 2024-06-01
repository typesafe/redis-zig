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
    try self.seekDatabaseOffset();

    const it = .{ .database = self.* };
    return it;
}

test "get entries from rdb with single key" {
    const rdb = "524544495330303033FA0972656469732D76657205372E322E30FA0A72656469732D62697473C040FE00FB010000097261737062657272790A73747261776265727279FF8BEB06989DD158B60A";
    var buffer: [88]u8 = undefined;
    const content = try std.fmt.hexToBytes(&buffer, rdb);
    var db = fromBuffer(content);

    var it = try db.getEntryIterator();

    while (it.next()) |k| {
        std.debug.print("{s}\n", .{k[0].String});
    }
}

test "get entries from rdb with exp key" {
    const rdb = "524544495330303033FA0972656469732D76657205372E322E30FA0A72656469732D62697473C040FE00FB0505FC000C288AC70100000004706561720470656172FC009CEF127E0100000009726173706265727279056170706C65FC000C288AC701000000066F72616E6765056D616E676FFC000C288AC701000000056170706C650970696E656170706C65FC000C288AC7010000000A7374726177626572727909626C75656265727279FFE77A32A0300EE3DC0A";
    var buffer: [362]u8 = undefined;
    const content = try std.fmt.hexToBytes(&buffer, rdb);
    var db = fromBuffer(content);

    var it = try db.getEntryIterator();

    while (it.next()) |k| {
        std.debug.print("{s}\n", .{k[0].String});
    }
}

pub const KeyValuePair = struct { StringEncodedValue, StringEncodedValue, ?i64 };

pub const EntryIterator = struct {
    database: Self,

    pub fn next(self: *@This()) ?KeyValuePair {
        if (self.database.offset >= self.database.buffer.len) {
            return null;
        }

        const op = self.database.peekOpCode();
        return switch (op) {
            OpCode.Unknown => self.database.readKeyValuePair(null),
            OpCode.ExpireTimeSeconds => self.database.readKeyValuePair(self.database.readExpiration(i32)),
            OpCode.ExpireTimeMs => self.database.readKeyValuePair(self.database.readExpiration(i64)),
            OpCode.EOF => null,
            else => null,
        };
    }
};

fn readKeyValuePair(self: *Self, expiry: ?i64) KeyValuePair {
    const vtype = self.readValueType();
    const key = self.readField();
    const val = switch (vtype) {
        .String => self.readField(),
        // TODO: other types
        else => self.readField(),
    };

    return .{ key, val, expiry };
}

fn readExpiration(self: *Self, comptime T: type) T {
    // when this is called the opcode is not yet consumed...
    _ = self.readOpCode();
    const v: T = std.mem.readVarInt(T, self.buffer[self.offset .. self.offset + @sizeOf(T)], .little);
    self.offset += @sizeOf(T);
    return v;
}

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
            OpCode.DatabaseSelector => {
                // we've reached the DB
                _ = self.readField(); // number
                if (self.peekOpCode() == OpCode.ResizeDB) {
                    _ = self.readOpCode();
                    _ = self.readLength();
                    _ = self.readLength();
                }
                break;
            },
            else => {},
        }
    }
}

/// Expects the current offset to point to the next op code.
/// Increments the offset.
fn readOpCode(self: *Self) OpCode {
    return OpCode.parse(self.readByte());
}

fn peekOpCode(self: *Self) OpCode {
    return OpCode.parse(self.buffer[self.offset]);
}

fn readValueType(self: *Self) ValueType {
    return ValueType.parse(self.readByte());
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

fn readLength(self: *Self) usize {
    const byte = self.readByte();
    const flags = byte & 0b11000000;
    const payload = byte & 0b00111111;

    switch (flags) {
        0b00000000 => return std.mem.readInt(u32, &.{ 0, 0, 0, payload }, .big),
        0b01000000 => return std.mem.readInt(u32, &.{ 0, 0, payload, self.readByte() }, .big),
        0b10000000 => return std.mem.readInt(u32, &.{ self.readByte(), self.readByte(), self.readByte(), self.readByte() }, .big),

        else => unreachable,
    }
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
            0xFA => .Auxiliary,
            0xFE => .DatabaseSelector,
            0xFF => .EOF,
            0xFC => .ExpireTimeMs,
            0xFD => .ExpireTimeSeconds,
            0xFB => .ResizeDB,
            else => .Unknown,
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
    Unknown,

    fn parse(b: u8) ValueType {
        return switch (b) {
            0x00 => .String,
            0x01 => .List,
            0x02 => .Set,
            0x03 => .SortedSet,
            0x04 => .Hash,
            0x09 => .Zipmap,
            0x0A => .Ziplist,
            0x0B => .IntSet,
            0x0C => .ZipListSortedSet,
            0x0D => .ZipListHash,
            0x0E => .QuicklistList,
            else => .Unknown,
        };
    }
};
