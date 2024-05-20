pub const ServerState = struct {
    role: Role = Role.master,
    master_replid: ?[]const u8 = null,
    master_repl_offset: ?u16 = null,

    // connected_slaves: u16,
    // second_repl_offset: i16 = -1,
    // repl_backlog_active: u16 = 0,
    // repl_backlog_size: usize = 1048576,
    // repl_backlog_first_byte_offset: usize = 0,
    // repl_backlog_histlen: ?usize,
};

pub const Role = enum {
    master,
    slave,
};

pub const Options = struct { port: u16, master: ?Host = null };

pub const Host = struct {
    host: []const u8,
    port: u16,
};
