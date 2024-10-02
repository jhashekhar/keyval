// Key concepts:
// Key: a unique identifier for each data entry
// Value: the data associated with the key. This can be simple (like a string) or complex (like a JSON object)
// Put: Operation to add or update a key-value pair
// Get: Operation to retrieve a value by its key
// Delete: Operation to remove a key-value pair

// Additional concepts:
// Partioning: Splitting data across multiple nodes
// Replication: Creating copies of data for fault tolerance
// Consistent hashing: A technique to distribute data across nodes in a way that minimizes the number of key-value pairs that need to be moved when a node is added or removed
// Load balancing: Distributing incoming requests across multiple nodes to prevent any single node from becoming a bottleneck
// Caching: Storing frequently accessed data in memory to improve performance
// Indexing: Creating additional data structures to speed up data retrieval
// Sharding: Dividing a dataset into smaller chunks and distributing them across multiple nodes

const std = @import("std");

const DB_FILE = "key_value_store.db";

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var map = std.StringHashMap([]const u8).init(allocator);
    defer {
        var it = map.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.*);
        }
        map.deinit();
    }

    // Load existing data from file
    loadFromFile(&map, allocator) catch |err| {
        std.debug.print("Error loading from file: {}\n", .{err});
        std.debug.print("Starting with an empty database.\n", .{});
    };

    // Get command-line arguments
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 3) {
        std.debug.print("Usage: {s} <command> <key> <value>\n", .{args[0]});
        std.debug.print("   or: {s} <command> - <value>\n", .{args[0]});
        std.debug.print("Example: {s} put myKey myValue\n", .{args[0]});
        std.debug.print("     or: {s} put - myValue\n", .{args[0]});
        return;
    }

    const command = args[1];
    const key = args[2];

    if (std.mem.eql(u8, command, "put")) {
        if (std.mem.eql(u8, key, "-")) {
            if (args.len < 4) {
                std.debug.print("Error: Value is required when using '-' as key.\n", .{});
                return;
            }
            const value = args[3];
            const uuid = try generateUUIDv4(allocator);
            defer allocator.free(uuid);
            try put(&map, uuid, value);
            std.debug.print("Added key-value pair with generated UUID: {s} -> {s}\n", .{ uuid, value });
        } else {
            if (args.len < 4) {
                std.debug.print("Error: Value is required.\n", .{});
                return;
            }
            const value = args[3];
            try put(&map, key, value);
            std.debug.print("Added key-value pair: {s} -> {s}\n", .{ key, value });
        }
    } else {
        std.debug.print("Unknown command: {s}\n", .{command});
    }

    // Print all key-value pairs
    var it = map.iterator();
    while (it.next()) |entry| {
        std.debug.print("{s}: {s}\n", .{ entry.key_ptr.*, entry.value_ptr.* });
    }

    // Save data to file
    saveToFile(&map) catch |err| {
        std.debug.print("Error saving to file: {}\n", .{err});
    };
}

fn generateUUIDv4(allocator: std.mem.Allocator) ![]u8 {
    var uuid: [36]u8 = undefined;
    std.crypto.random.bytes(&uuid);
    return allocator.dupe(u8, &uuid);
}

fn put(map: *std.StringHashMap([]const u8), key: []const u8, value: []const u8) !void {
    const duped_key = try map.allocator.dupe(u8, key);
    const duped_value = try map.allocator.dupe(u8, value);
    try map.put(duped_key, duped_value);
}

fn saveToFile(map: *std.StringHashMap([]const u8)) !void {
    const file = try std.fs.cwd().createFile(DB_FILE, .{});
    defer file.close();

    var writer = file.writer();

    var it = map.iterator();
    while (it.next()) |entry| {
        try writer.print("{s}\n{s}\n", .{ entry.key_ptr.*, entry.value_ptr.* });
    }
}

fn loadFromFile(map: *std.StringHashMap([]const u8), allocator: std.mem.Allocator) !void {
    const file = try std.fs.cwd().openFile(DB_FILE, .{});
    defer file.close();

    var reader = file.reader();

    var it = std.mem.tokenize(u8, try reader.readAllAlloc(allocator, 1024 * 1024), "\n");
    while (it.next()) |line| {
        if (line.len == 0) continue;
        const parts = std.mem.tokenize(u8, line, " ");
        var iterator = parts;
        const key = iterator.next() orelse {
            std.debug.print("Error: Missing key\n", .{});
            return;
        };
        var part_count: usize = 0;
        var parts_iter = parts;
        while (parts_iter.next()) |_| {
            part_count += 1;
        }
        if (part_count != 2) continue;
        const value = parts_iter.next() orelse {
            std.debug.print("Error: Missing value\n", .{});
            return;
        };
        try map.put(try map.allocator.dupe(u8, key), try map.allocator.dupe(u8, value));
    }
}
