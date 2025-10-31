# PikoDB

**A reliable, simple, fast, no-frills key-value database for Node.js**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

---

- **Ultra-minimal footprint** at only ~2.5KB gzipped
- **Immediate disk persistence** with atomic writes - no data loss
- **Zero configuration** - works out of the box
- **Multiple named dictionaries** - flexible compression for different data types
- **Dynamic dictionary management** - add/remove dictionaries at runtime
- **Full data type support** - store any JSON-serializable data
- **Item versioning** - automatic version tracking
- **Item expiration** - automatic cleanup of expired records
- **Concurrent operation safety** - handles parallel reads/writes
- **Simple architecture** - memory + disk, that's it
- **High test coverage** - lots of tests ensuring reliability
- **Zero dependencies** - pure Node.js implementation

## Installation

```bash
npm install pikodb
```

## Quick Start

```typescript
import { PikoDB } from 'pikodb';

// Create database instance
const db = new PikoDB({ databaseDirectory: './data' });

// Start the database
await db.start();

// Write data (immediately persisted to disk)
await db.write('users', 'user1', {
  name: 'Alice',
  email: 'alice@example.com',
  age: 30
});

// Read a specific value
const user = await db.get('users', 'user1');
console.log(user); // { name: 'Alice', email: 'alice@example.com', age: 30 }

// Read all values from a table
const allUsers = await db.get('users');
console.log(allUsers); // [['user1', {...}]]

// Delete a value
await db.delete('users', 'user1');

// Always close when done
await db.close();
```

## Design

### Design Goals

PikoDB is built on three architectural pillars: **reliability**, **simplicity**, and **performance**.

- **Reliability**: Immediate disk persistence with atomic writes means zero data loss. Every write operation is guaranteed to be safely stored before returning.
- **Simplicity**: No WAL files, no checkpoint complexity, no buffer races. The architecture is straightforward: memory + disk.
- **Performance**: Fast in-memory reads with efficient disk I/O. Optional dictionary compression reduces storage by 10-25%.

### Why PikoDB?

Traditional databases often use Write-Ahead Logs (WAL) and complex checkpointing mechanisms. This adds:

- Buffer race conditions
- Checkpoint timing issues
- Data loss scenarios if crashes occur between writes
- Complexity in recovery procedures

PikoDB eliminates these by using **immediate atomic writes**. Every write operation:

1. Updates the in-memory data structure
2. Atomically writes to disk using a temp file + rename strategy
3. Returns only after data is safely persisted

This means:

- ✅ No corruption possible - atomic rename guarantees consistency
- ✅ No recovery needed - database is always in valid state
- ✅ Simpler codebase - easier to understand and maintain
- ✅ Predictable behavior - writes complete before returning

**Note**: By default, writes are atomic but may remain in OS cache briefly. For guaranteed durability against power loss, enable `durableWrites` (see Durability & Crash Safety below).

### Implementation Details

- A **database** is just a directory containing files (tables).
- Each **table** is a binary file stored on disk.
- Tables are loaded into memory as JavaScript `Map` objects for fast access.
- All write operations use **atomic writes** (temp file → rename).
- Records include automatic versioning and optional expiration timestamps.
- Optional **dictionary compression** reduces storage size by mapping long keys to short keys.

### Characteristics

PikoDB is optimized for:

- **Small to medium datasets** per table (up to 100k records).
- **Read-heavy workloads** (dictionary compression speeds up reads).
- **Multi-tenant applications** with many small tables.
- **Serverless environments** where simplicity and reliability matter.
- **Development and testing** where zero configuration is valuable.

Performance considerations:

- Simple key lookups are extremely fast (in-memory Map access).
- Table loading incurs some latency (disk I/O + deserialization).
- Write operations are synchronous by design (reliability over speed).
- Dictionary compression adds ~30-80% overhead on writes, but speeds up reads.

## Dictionary Compression

PikoDB includes optional dictionary-based compression to reduce storage size. This feature:

- **Always compresses metadata** (value, version, timestamp, expiration, dictionaryName) - saves ~27 bytes per record.
- **Optionally compresses user data** if you provide a dictionary.
- **Supports multiple named dictionaries** for different data types.
- **Works recursively** on nested objects and arrays.
- **Transparent** - you always read/write with original keys.
- **Auto-generates inverse mapping** - provide deflate OR inflate, not both.

### Compression Savings

Real-world examples:

| Data Type | Records | Without Compression | With Compression | Savings |
|-----------|---------|---------------------|------------------|---------|
| Large objects (component specs) | 1 | 1,942 bytes | 1,742 bytes | 10.30% |
| Small metrics | 1 | 112 bytes | 104 bytes | 7.14% |
| Batch metrics | 1,000 | 110,781 bytes | 102,781 bytes | 7.22% |

**Note**: Everyone gets metadata compression for free (~27 bytes per record). Dictionary compression adds additional savings on user data.

### Compression Best Practices

```typescript
// ✅ Good: Simple, logical abbreviations
{
  deflate: {
    sensor: 's',
    temperature: 't',
    humidity: 'h',
    timestamp: 'ts'
  }
}

// ✅ Good: Nested keys compressed recursively
{
  deflate: {
    metadata: 'm',
    location: 'l',
    building: 'b'
  }
}
// Input:  { metadata: { location: { building: 'A' } } }
// On disk: { m: { l: { b: 'A' } } }

// ⚠️ Avoid: Keys that exist in your data
// If your data has an "s" field, don't use "s" as compression target
```

### Performance Impact

| Operation | Impact | Notes |
|-----------|--------|-------|
| **Writes** | 30-80% slower | Recursive key transformation overhead |
| **Reads (individual)** | 20-70% **faster** | Smaller files = faster disk I/O |
| **Reads (bulk)** | Similar or faster | I/O savings offset decompression |
| **Storage** | 10-25% smaller | Varies by data structure |

**✅ When to use compression:**

- Read-heavy workloads (you get faster reads!)
- Storage-constrained environments
- Metrics/telemetry data (repetitive structures)
- Archival data (write once, read many)

**⚠️ When to skip compression:**

- Write-heavy workloads (50-80% slower writes...)
- Real-time systems where write latency matters
- Small datasets where storage isn't a concern

## Durability & Crash Safety

### Default Behavior (Fast Writes)

PikoDB uses atomic file operations to ensure **no corruption**, but writes may remain in the OS page cache briefly:

```typescript
const db = new PikoDB({ databaseDirectory: './data' });
// durableWrites: false (default)
```

**Safety guarantees:**

- ✅ **No corruption** - atomic rename ensures consistency
- ✅ **Crash safe** - process/VM crashes won't corrupt data
- ✅ **Fast writes** - no durability overhead
- ⚠️ **Power loss window** - last 5-30 seconds of writes may be lost on power failure

**When to use:**

- ✅ Development and testing
- ✅ Batch jobs and VM workloads (jobs can retry)
- ✅ Caches and non-critical data
- ✅ Applications with external backups

### Maximum Durability (Durable Writes)

For guaranteed persistence against power loss:

```typescript
const db = new PikoDB({
  databaseDirectory: './data',
  durableWrites: true  // Force writes to physical storage
});
```

**How it works:**

1. Writes data to temp file
2. Forces data to physical disk (fsync)
3. Atomically renames temp file to final file
4. Forces directory entry to disk (when supported)

**Safety guarantees:**

- ✅ **Maximum durability** - survives power loss
- ✅ **Zero data loss** - every completed write is on disk
- ✅ **No corruption** - atomic rename still ensures consistency
- ❌ **Slower writes** - 10-100x slower depending on storage

**When to use:**

- ✅ Financial transactions
- ✅ Critical audit logs
- ✅ Medical records
- ✅ Any data that cannot be recreated

**Trade-off comparison:**

| Feature | Default (fast) | Durable writes |
|---------|----------------|----------------|
| Corruption protection | ✅ Yes | ✅ Yes |
| Process crash safety | ✅ Yes | ✅ Yes |
| VM termination safety | ✅ Yes | ✅ Yes |
| Power loss safety | ⚠️ Partial (OS dependent) | ✅ Yes |
| Write performance | ✅ Fast | ❌ 10-100x slower |
| Suitable for | Jobs, caches, dev/test | Financial, audit logs |

### Crash Scenarios

**Process crash (kill -9, exception):**

- Both modes: ✅ No corruption, completed writes safe
- Data loss: Only in-flight writes (microseconds)

**VM shutdown (docker stop, orchestrator kill):**

- Both modes: ✅ No corruption
- Default mode: ⚠️ Last 5-30 seconds may be lost (OS dependent)
- Durable writes: ✅ All writes safe

**Power loss / kernel panic:**

- Both modes: ✅ No corruption
- Default mode: ⚠️ Last 5-30 seconds may be lost
- Durable writes: ✅ All writes safe (filesystem dependent)

**Recommendation**: Use default mode unless you absolutely cannot lose data or re-run failed operations.

## API Reference

### Constructor

```typescript
new PikoDB(options: DatabaseOptions)
```

**Options:**

- `databaseDirectory`: Path to database directory (required)
- `dictionaries`: Optional object containing named dictionaries for compression (optional)
- `durableWrites`: Enable durable writes for maximum durability (optional, default: false)

**Examples:**

```typescript
// Basic usage
const db = new PikoDB({
  databaseDirectory: './data'
});

// With multiple named dictionaries
const db = new PikoDB({
  databaseDirectory: './data',
  dictionaries: {
    sensors: { deflate: { sensor: 's', temperature: 't' } },
    users: { deflate: { username: 'u', email: 'e' } }
  }
});

// With durable writes for maximum durability
const db = new PikoDB({
  databaseDirectory: './data',
  durableWrites: true  // Slower writes, but survives power loss
});

// All options combined
const db = new PikoDB({
  databaseDirectory: './data',
  dictionaries: {
    metrics: { deflate: { sensor: 's', temperature: 't' } }
  },
  durableWrites: true
});
```

### start()

Initialize the database by loading existing tables from disk.

```typescript
await db.start(): Promise<void>
```

**Example:**

```typescript
await db.start(); // Loads existing tables from disk
```

### write()

Write a key-value pair to a table with immediate disk persistence.

```typescript
await db.write(
  tableName: string,
  key: string,
  value: any,
  expirationTimestamp?: number,
  dictionaryName?: string
): Promise<boolean>
```

**Parameters:**

- `tableName`: The table to write to
- `key`: The key to store the value under
- `value`: The value to store (any JSON-serializable data)
- `expirationTimestamp`: Optional expiration timestamp in milliseconds
- `dictionaryName`: Optional dictionary name to use for compression

**Returns:** `true` if write succeeded, `false` otherwise

**Examples:**

```typescript
// Simple write
await db.write('users', 'user1', { name: 'Alice', age: 30 });

// Write with expiration (1 hour from now)
const oneHour = Date.now() + (60 * 60 * 1000);
await db.write('sessions', 'session123', { userId: 'user1' }, oneHour);

// Write with specific dictionary
await db.write('sensors', 'sensor1', {
  sensor: 'DHT22',
  temperature: 23.5
}, undefined, 'sensorDict');

// Write nested objects (compression applies recursively if dictionary configured)
await db.write('sensors', 'sensor1', {
  sensor: 'DHT22',
  metadata: {
    location: 'warehouse',
    building: 'A'
  }
}, undefined, 'sensorDict');
```

### get()

Read a value by key, or all values if no key specified.

```typescript
await db.get(tableName: string, key?: string): Promise<any>
```

**Parameters:**

- `tableName`: The table to read from
- `key`: Optional key to retrieve a specific value

**Returns:** The value for the key, or array of `[key, value]` pairs if no key specified

**Examples:**

```typescript
// Get a specific value
const user = await db.get('users', 'user1');
console.log(user); // { name: 'Alice', age: 30 }

// Get all values from a table
const allUsers = await db.get('users');
console.log(allUsers); // [['user1', {...}], ['user2', {...}]]

// Returns undefined if key doesn't exist
const missing = await db.get('users', 'nonexistent'); // undefined

// Expired records return undefined and are auto-cleaned
const expired = await db.get('sessions', 'old-session'); // undefined (if expired)
```

### delete()

Delete a key from a table with immediate disk persistence.

```typescript
await db.delete(tableName: string, key: string): Promise<boolean>
```

**Parameters:**

- `tableName`: The table to delete from
- `key`: The key to delete

**Returns:** `true` if deletion succeeded, `false` if key didn't exist

**Examples:**

```typescript
// Delete a key
const deleted = await db.delete('users', 'user1');
console.log(deleted); // true if existed, false otherwise

// Deleting an expired key returns false
await db.delete('sessions', 'expired-session'); // false
```

### getTableSize()

Get the number of keys in a table.

```typescript
await db.getTableSize(tableName: string): Promise<number>
```

**Parameters:**

- `tableName`: The table to get the size of

**Returns:** The number of non-expired keys in the table

**Example:**

```typescript
await db.write('users', 'user1', { name: 'Alice' });
await db.write('users', 'user2', { name: 'Bob' });
const size = await db.getTableSize('users'); // 2
```

### listTables()

List all table names currently in memory.

```typescript
db.listTables(): string[]
```

**Returns:** Array of table names

**Example:**

```typescript
await db.write('users', 'user1', { name: 'Alice' });
await db.write('products', 'prod1', { name: 'Widget' });
const tables = db.listTables(); // ['users', 'products']
```

### deleteTable()

Delete an entire table and its disk file.

```typescript
await db.deleteTable(tableName: string): Promise<boolean>
```

**Parameters:**

- `tableName`: The table to delete

**Returns:** `true` if deletion succeeded

**Example:**

```typescript
await db.deleteTable('old-sessions');
const tables = db.listTables(); // 'old-sessions' no longer in list
```

### cleanupExpired()

Clean up expired records from a table.

```typescript
await db.cleanupExpired(tableName: string): Promise<number>
```

**Parameters:**

- `tableName`: The table to clean up

**Returns:** The number of expired records removed

**Example:**

```typescript
const removed = await db.cleanupExpired('sessions');
console.log(`Removed ${removed} expired sessions`);
```

### cleanupAllExpired()

Clean up expired records from all tables.

```typescript
await db.cleanupAllExpired(): Promise<number>
```

**Returns:** The total number of expired records removed across all tables

**Example:**

```typescript
const totalRemoved = await db.cleanupAllExpired();
console.log(`Removed ${totalRemoved} expired records total`);
```

### flush()

Force persistence of all in-memory tables to disk.

```typescript
await db.flush(): Promise<void>
```

**Note:** Write operations already persist immediately. Use this only if needed.

**Example:**

```typescript
await db.flush(); // Manually flush all tables to disk
```

### close()

Close the database by flushing all data to disk.

```typescript
await db.close(): Promise<void>
```

**Always call this before your application exits to ensure data persistence.**

**Example:**

```typescript
const db = new PikoDB({ databaseDirectory: './data' });
await db.start();
// ... use the database ...
await db.close(); // Always close when done
```

### addDictionary()

Add a new dictionary for compression after database instantiation.

```typescript
db.addDictionary(name: string, dictionary: Dictionary): void
```

**Parameters:**

- `name`: The name to identify this dictionary
- `dictionary`: The dictionary configuration (provide either deflate or inflate)

**Throws:** If a dictionary with the same name already exists

**Example:**

```typescript
const db = new PikoDB({ databaseDirectory: './data' });
await db.start();

// Add a dictionary for sensor data
db.addDictionary('sensors', {
  deflate: {
    sensor: 's',
    temperature: 't',
    humidity: 'h'
  }
});

// Use the dictionary when writing
await db.write('readings', 'r1', { sensor: 'DHT22', temperature: 23.5 }, undefined, 'sensors');
```

### removeDictionary()

Remove a dictionary by name.

```typescript
db.removeDictionary(name: string): boolean
```

**Parameters:**

- `name`: The name of the dictionary to remove

**Returns:** `true` if dictionary was removed, `false` if it didn't exist

**Example:**

```typescript
db.removeDictionary('sensors');
```

### listDictionaries()

List all available dictionary names.

```typescript
db.listDictionaries(): string[]
```

**Returns:** Array of dictionary names

**Example:**

```typescript
const dictionaries = db.listDictionaries();
console.log(dictionaries); // ['sensors', 'users', 'metrics']
```

## Item Expiration

Setting an expiration timestamp is easy:

```typescript
// Expire in 1 minute
const oneMinute = Date.now() + (60 * 1000);
await db.write('sessions', 'session123', { userId: 'user1' }, oneMinute);

// Expire in 1 hour
const oneHour = Date.now() + (60 * 60 * 1000);
await db.write('cache', 'temp-data', { value: 'cached' }, oneHour);

// Expire in 1 day
const oneDay = Date.now() + (24 * 60 * 60 * 1000);
await db.write('tokens', 'token123', { token: 'xyz' }, oneDay);
```

Expired items are automatically cleaned up when:

- Reading (returns `undefined` and removes expired item)
- Calling `cleanupExpired()` or `cleanupAllExpired()`
- Calling `getTableSize()` (counts only non-expired items)

## Item Versioning

PikoDB automatically tracks item versions:

```typescript
const tableName = 'users';
const key = 'user1';

// First write - version 1
await db.write(tableName, key, { name: 'John' });

// Second write - version 2
await db.write(tableName, key, { name: 'Jane' });

// Third write - version 3
await db.write(tableName, key, { name: 'Sam' });

// When retrieved, version will be 3
const user = await db.get(tableName, key);
// Returns the value, version is tracked internally
```

Versioning is automatic and handled internally. Every update increments the version number.

## TypeScript Support

PikoDB is written in TypeScript and includes full type definitions:

```typescript
import { PikoDB, DatabaseOptions, DatabaseRecord, Dictionary } from 'pikodb';

const options: DatabaseOptions = {
  databaseDirectory: './data',
  dictionary: {
    deflate: {
      sensor: 's',
      temperature: 't'
    }
  }
};

const db = new PikoDB(options);
```

**Available Types:**

- `PikoDB` - The main database class
- `DatabaseOptions` - Configuration options for the database
- `DatabaseRecord` - Internal record structure with value, version, timestamp, and expiration
- `Dictionary` - Dictionary configuration for compression (deflate or inflate mappings)

## Testing

```bash
npm test
```

PikoDB includes 120+ comprehensive tests covering:

- Basic operations (read, write, delete)
- Concurrent operations
- Expiration handling
- Dictionary compression
- Input validation and security
- Durable writes options
- Edge cases and error handling
- Performance benchmarks

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT. See `LICENSE` file for details.

## Related Projects

- [MikroDB](https://github.com/mikaelvesavuori/mikrodb)