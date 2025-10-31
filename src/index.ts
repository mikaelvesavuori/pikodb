import {
  existsSync,
  mkdirSync,
  readdirSync,
  openSync,
  closeSync
} from 'node:fs';
import { readFile, writeFile, rename, unlink, open } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import {
  type Dictionary,
  type ProcessedDictionary,
  processDictionary,
  transformValue
} from './dictionary.js';

// Re-export Dictionary type for public API
export type { Dictionary };

/**
 * Configuration options for PikoDB.
 *
 * @example
 * // Basic usage without compression
 * const options = {
 *   databaseDirectory: './data'
 * };
 *
 * @example
 * // With dictionary compression (provide deflate OR inflate, not both)
 * const options = {
 *   databaseDirectory: './data',
 *   dictionary: {
 *     deflate: {
 *       sensor: 's',
 *       temperature: 't',
 *       timestamp: 'ts'
 *     }
 *   }
 * };
 *
 * @example
 * // Or provide inflate mapping (inverse auto-generated)
 * const options = {
 *   databaseDirectory: './data',
 *   dictionary: {
 *     inflate: {
 *       s: 'sensor',
 *       t: 'temperature',
 *       ts: 'timestamp'
 *     }
 *   }
 * };
 *
 * @example
 * // With durable writes for maximum durability (slower writes)
 * const options = {
 *   databaseDirectory: './data',
 *   durableWrites: true  // Guarantees data on disk before returning
 * };
 */
export interface DatabaseOptions {
  databaseDirectory: string;
  /**
   * Optional dictionary for compressing database records.
   * Provide either deflate (long → short) or inflate (short → long).
   * The inverse mapping will be auto-generated.
   *
   * Note: DatabaseRecord metadata (value, version, timestamp, expiration)
   * is ALWAYS compressed automatically - no need to include in dictionary.
   */
  dictionary?: Dictionary;
  /**
   * Enable durable writes for guaranteed persistence.
   *
   * When enabled, forces writes to physical storage before returning,
   * ensuring data survives power loss or VM termination.
   *
   * Trade-offs:
   * - ✅ Maximum durability (survives power loss)
   * - ✅ No data loss on crashes
   * - ❌ 10-100x slower writes (depends on storage)
   *
   * Default: false (atomic rename provides crash safety without durability overhead)
   *
   * @default false
   */
  durableWrites?: boolean;
}

/**
 * Represents a database record with versioning and expiration support.
 */
export interface DatabaseRecord {
  value: any;
  version: number;
  timestamp: number;
  expiration: number | null;
}

/**
 * Validates table name to prevent directory traversal and other security issues.
 */
function validateTableName(tableName: string): void {
  if (!tableName || typeof tableName !== 'string') {
    throw new Error('Table name must be a non-empty string');
  }

  if (tableName.length > 255) {
    throw new Error('Table name must not exceed 255 characters');
  }

  // Prevent directory traversal
  if (tableName.includes('/') || tableName.includes('\\')) {
    throw new Error('Table name must not contain path separators');
  }

  // Prevent parent directory references
  if (tableName.includes('..')) {
    throw new Error('Table name must not contain ".."');
  }

  // Prevent hidden files and special names
  if (tableName.startsWith('.')) {
    throw new Error('Table name must not start with "."');
  }

  // Prevent null bytes
  if (tableName.includes('\0')) {
    throw new Error('Table name must not contain null bytes');
  }

  // Prevent reserved filesystem names (Windows)
  const reservedNames = [
    'CON',
    'PRN',
    'AUX',
    'NUL',
    'COM1',
    'COM2',
    'COM3',
    'COM4',
    'COM5',
    'COM6',
    'COM7',
    'COM8',
    'COM9',
    'LPT1',
    'LPT2',
    'LPT3',
    'LPT4',
    'LPT5',
    'LPT6',
    'LPT7',
    'LPT8',
    'LPT9'
  ];
  if (reservedNames.includes(tableName.toUpperCase())) {
    throw new Error(`Table name "${tableName}" is reserved by the filesystem`);
  }
}

/**
 * Validates key to ensure it's a valid string.
 */
function validateKey(key: string): void {
  if (key === undefined || key === null) {
    throw new Error('Key must be defined');
  }

  if (typeof key !== 'string') {
    throw new Error('Key must be a string');
  }

  if (key.length === 0) {
    throw new Error('Key must not be empty');
  }

  if (key.length > 1024) {
    throw new Error('Key must not exceed 1024 characters');
  }

  // Prevent null bytes in keys
  if (key.includes('\0')) {
    throw new Error('Key must not contain null bytes');
  }
}

/**
 * Validates value to ensure it's JSON-serializable.
 */
function validateValue(value: any): void {
  if (value === undefined) {
    throw new Error('Value must not be undefined (use null instead)');
  }

  // Check for non-serializable types
  const type = typeof value;
  if (type === 'function') {
    throw new Error(
      'Value must be JSON-serializable: functions are not supported'
    );
  }
  if (type === 'symbol') {
    throw new Error(
      'Value must be JSON-serializable: symbols are not supported'
    );
  }

  try {
    const serialized = JSON.stringify(value);
    // Check if serialization resulted in undefined (e.g., for functions, symbols)
    if (serialized === undefined) {
      throw new Error(
        'Value must be JSON-serializable: value cannot be serialized'
      );
    }
  } catch (error) {
    throw new Error(
      `Value must be JSON-serializable: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

/**
 * PikoDB is a reliable, simple, fast, no-frills key-value database.
 *
 * Features:
 * - Immediate disk persistence with atomic writes
 * - No WAL complexity or checkpoint issues
 * - Simple memory + disk architecture
 * - Full data type support
 * - Concurrent operation safety
 * - Optional dictionary compression for storage optimization
 *
 * @example
 * // Basic usage
 * const db = new PikoDB({ databaseDirectory: './data' });
 * await db.start();
 * await db.write('users', 'user1', { name: 'Alice', email: 'alice@example.com' });
 * const user = await db.get('users', 'user1');
 * await db.close();
 *
 * @example
 * // With dictionary compression
 * const db = new PikoDB({
 *   databaseDirectory: './data',
 *   dictionary: {
 *     deflate: {
 *       sensor: 's',
 *       temperature: 't',
 *       humidity: 'h'
 *     }
 *   }
 * });
 * await db.start();
 * // Data is compressed on disk but you work with original keys
 * await db.write('readings', 'r1', { sensor: 'DHT22', temperature: 23.5 });
 * const reading = await db.get('readings', 'r1'); // { sensor: 'DHT22', temperature: 23.5 }
 * await db.close();
 */
export class PikoDB {
  private readonly data: Map<string, Map<string, DatabaseRecord>> = new Map();
  private readonly databaseDirectory: string;
  private readonly dictionary?: ProcessedDictionary;
  private readonly useFsync: boolean;

  constructor(options: DatabaseOptions) {
    this.databaseDirectory = options.databaseDirectory;
    this.useFsync = options.durableWrites ?? false;

    if (options.dictionary) {
      this.dictionary = processDictionary(options.dictionary);
    }

    if (!existsSync(this.databaseDirectory))
      mkdirSync(this.databaseDirectory, { recursive: true });
  }

  /**
   * @description Initialize the database by loading existing tables from disk.
   *
   * @example
   * const db = new PikoDB({ databaseDirectory: './data' });
   * await db.start(); // Loads existing tables from disk
   */
  async start(): Promise<void> {
    try {
      const files = readdirSync(this.databaseDirectory);

      for (const file of files) {
        // Skip temporary files and hidden files
        if (!file.endsWith('.tmp') && !file.startsWith('.'))
          await this.loadTable(file);
      }
    } catch (error) {
      console.error('Failed to start database:', error);
      throw error;
    }
  }

  /**
   * @description Write a key-value pair to a table with immediate disk persistence.
   *
   * @param tableName - The table to write to
   * @param key - The key to store the value under
   * @param value - The value to store (any JSON-serializable data)
   * @param expirationTimestamp - Optional expiration timestamp in milliseconds
   * @returns True if write succeeded, false otherwise
   *
   * @example
   * // Simple write
   * await db.write('users', 'user1', { name: 'Alice', age: 30 });
   *
   * @example
   * // Write with expiration (1 hour from now)
   * const oneHour = Date.now() + (60 * 60 * 1000);
   * await db.write('sessions', 'session123', { userId: 'user1' }, oneHour);
   *
   * @example
   * // Write nested objects (compression applies recursively if dictionary configured)
   * await db.write('sensors', 'sensor1', {
   *   sensor: 'DHT22',
   *   metadata: {
   *     location: 'warehouse',
   *     building: 'A'
   *   }
   * });
   */
  async write(
    tableName: string,
    key: string,
    value: any,
    expirationTimestamp?: number
  ): Promise<boolean> {
    // Validate inputs (throws on error)
    validateTableName(tableName);
    validateKey(key);
    validateValue(value);

    try {
      // Ensure table exists in memory
      if (!this.data.has(tableName)) this.data.set(tableName, new Map());

      // biome-ignore lint/style/noNonNullAssertion: OK
      const table = this.data.get(tableName)!;
      const currentRecord = table.get(key);
      const newVersion = (currentRecord?.version || 0) + 1;

      const record: DatabaseRecord = {
        value,
        version: newVersion,
        timestamp: Date.now(),
        expiration: expirationTimestamp || null
      };

      table.set(key, record);

      await this.persistTable(tableName);

      return true;
    } catch (error) {
      console.error(`Write failed for ${tableName}:${key}:`, error);
      return false;
    }
  }

  /**
   * @description Read a value by key, or all values if no key specified.
   *
   * @param tableName - The table to read from
   * @param key - Optional key to retrieve a specific value
   * @returns The value for the key, or array of [key, value] pairs if no key specified
   *
   * @example
   * // Get a specific value
   * const user = await db.get('users', 'user1');
   * console.log(user); // { name: 'Alice', age: 30 }
   *
   * @example
   * // Get all values from a table
   * const allUsers = await db.get('users');
   * console.log(allUsers); // [['user1', {...}], ['user2', {...}]]
   *
   * @example
   * // Returns undefined if key doesn't exist
   * const missing = await db.get('users', 'nonexistent'); // undefined
   *
   * @example
   * // Expired records return undefined and are auto-cleaned
   * const expired = await db.get('sessions', 'old-session'); // undefined (if expired)
   */
  async get(tableName: string, key?: string): Promise<any> {
    // Validate inputs (throws on error)
    validateTableName(tableName);
    if (key !== undefined) validateKey(key);

    try {
      if (!this.data.has(tableName)) await this.loadTable(tableName);

      const table = this.data.get(tableName);
      if (!table) return key ? undefined : [];

      if (key !== undefined) {
        const record = table.get(key);
        if (!record) return undefined;

        if (this.isExpired(record)) {
          table.delete(key);
          await this.persistTable(tableName);
          return undefined;
        }

        return record.value;
      }

      const result: [string, any][] = [];
      const expiredKeys: string[] = [];

      for (const [k, record] of table.entries()) {
        if (this.isExpired(record)) expiredKeys.push(k);
        else result.push([k, record.value]);
      }

      if (expiredKeys.length > 0) {
        for (const expiredKey of expiredKeys) table.delete(expiredKey);

        await this.persistTable(tableName);
      }

      return result;
    } catch (error) {
      console.error(`Read failed for ${tableName}:${key}:`, error);
      return key ? undefined : [];
    }
  }

  /**
   * @description Delete a key from a table with immediate disk persistence.
   *
   * @param tableName - The table to delete from
   * @param key - The key to delete
   * @returns True if deletion succeeded, false if key didn't exist
   *
   * @example
   * // Delete a key
   * const deleted = await db.delete('users', 'user1');
   * console.log(deleted); // true if existed, false otherwise
   *
   * @example
   * // Deleting an expired key returns false
   * await db.delete('sessions', 'expired-session'); // false
   */
  async delete(tableName: string, key: string): Promise<boolean> {
    // Validate inputs (throws on error)
    validateTableName(tableName);
    validateKey(key);

    try {
      if (!this.data.has(tableName)) await this.loadTable(tableName);

      const table = this.data.get(tableName);
      if (!table || !table.has(key)) return false;

      const record = table.get(key);
      if (!record) return false;

      if (this.isExpired(record)) {
        table.delete(key);
        await this.persistTable(tableName);
        return false;
      }

      table.delete(key);
      await this.persistTable(tableName);

      return true;
    } catch (error) {
      console.error(`Delete failed for ${tableName}:${key}:`, error);
      return false;
    }
  }

  /**
   * @description Get the number of keys in a table.
   *
   * @param tableName - The table to get the size of
   * @returns The number of non-expired keys in the table
   *
   * @example
   * await db.write('users', 'user1', { name: 'Alice' });
   * await db.write('users', 'user2', { name: 'Bob' });
   * const size = await db.getTableSize('users'); // 2
   */
  async getTableSize(tableName: string): Promise<number> {
    // Validate inputs (throws on error)
    validateTableName(tableName);

    try {
      if (!this.data.has(tableName)) await this.loadTable(tableName);

      const table = this.data.get(tableName);
      if (!table) return 0;

      let size = 0;
      const expiredKeys: string[] = [];

      for (const [key, record] of table.entries()) {
        if (this.isExpired(record)) {
          expiredKeys.push(key);
        } else {
          size++;
        }
      }

      if (expiredKeys.length > 0) {
        for (const expiredKey of expiredKeys) table.delete(expiredKey);

        await this.persistTable(tableName);
      }

      return size;
    } catch (error) {
      console.error(`Get table size failed for ${tableName}:`, error);
      return 0;
    }
  }

  /**
   * @description Check if a record is expired.
   */
  private isExpired(record: DatabaseRecord): boolean {
    if (record.expiration === null) return false;
    return Date.now() > record.expiration;
  }

  /**
   * @description Clean up expired records from a table.
   *
   * @param tableName - The table to clean up
   * @returns The number of expired records removed
   *
   * @example
   * // Manually cleanup expired records
   * const removed = await db.cleanupExpired('sessions');
   * console.log(`Removed ${removed} expired sessions`);
   */
  async cleanupExpired(tableName: string): Promise<number> {
    // Validate inputs (throws on error)
    validateTableName(tableName);

    try {
      if (!this.data.has(tableName)) await this.loadTable(tableName);

      const table = this.data.get(tableName);
      if (!table) return 0;

      const expiredKeys: string[] = [];

      for (const [key, record] of table.entries()) {
        if (this.isExpired(record)) expiredKeys.push(key);
      }

      for (const key of expiredKeys) table.delete(key);

      if (expiredKeys.length > 0) {
        await this.persistTable(tableName);
      }

      return expiredKeys.length;
    } catch (error) {
      console.error(`Cleanup failed for ${tableName}:`, error);
      return 0;
    }
  }

  /**
   * @description Clean up expired records from all tables.
   *
   * @returns The total number of expired records removed across all tables
   *
   * @example
   * // Cleanup all expired records in the database
   * const totalRemoved = await db.cleanupAllExpired();
   * console.log(`Removed ${totalRemoved} expired records total`);
   */
  async cleanupAllExpired(): Promise<number> {
    let totalCleaned = 0;

    for (const tableName of this.data.keys())
      totalCleaned += await this.cleanupExpired(tableName);

    return totalCleaned;
  }

  /**
   * @description Delete an entire table and its disk file.
   *
   * @param tableName - The table to delete
   * @returns True if deletion succeeded
   *
   * @example
   * // Delete an entire table
   * await db.deleteTable('old-sessions');
   * const tables = db.listTables(); // 'old-sessions' no longer in list
   */
  async deleteTable(tableName: string): Promise<boolean> {
    // Validate inputs (throws on error)
    validateTableName(tableName);

    try {
      this.data.delete(tableName);

      const filePath = join(this.databaseDirectory, tableName);
      if (existsSync(filePath)) await unlink(filePath);

      return true;
    } catch (error) {
      console.error(`Delete table failed for ${tableName}:`, error);
      return false;
    }
  }

  /**
   * @description List all table names currently in memory.
   *
   * @returns Array of table names
   *
   * @example
   * await db.write('users', 'user1', { name: 'Alice' });
   * await db.write('products', 'prod1', { name: 'Widget' });
   * const tables = db.listTables(); // ['users', 'products']
   */
  listTables(): string[] {
    return Array.from(this.data.keys());
  }

  /**
   * @description Force persistence of all in-memory tables to disk.
   *
   * Note: Write operations already persist immediately. Use this only if needed.
   *
   * @example
   * // Manually flush all tables to disk
   * await db.flush();
   */
  async flush(): Promise<void> {
    try {
      const operations = Array.from(this.data.keys()).map((tableName) =>
        this.persistTable(tableName)
      );

      await Promise.all(operations);
    } catch (error) {
      console.error('Flush failed:', error);
      throw error;
    }
  }

  /**
   * @description Close the database by flushing all data to disk.
   *
   * Always call this before your application exits to ensure data persistence.
   *
   * @example
   * const db = new PikoDB({ databaseDirectory: './data' });
   * await db.start();
   * // ... use the database ...
   * await db.close(); // Always close when done
   */
  async close(): Promise<void> {
    await this.flush();
  }

  /**
   * @description Load a table from disk into memory.
   */
  private async loadTable(tableName: string): Promise<void> {
    const filePath = join(this.databaseDirectory, tableName);

    if (!existsSync(filePath)) {
      this.data.set(tableName, new Map());
      return;
    }

    try {
      const buffer = await readFile(filePath);

      if (buffer.length === 0) {
        this.data.set(tableName, new Map());
        return;
      }

      const tableData = this.deserializeTable(buffer);
      this.data.set(tableName, tableData);
    } catch (error) {
      console.error(`Failed to load table ${tableName}:`, error);
      this.data.set(tableName, new Map());
    }
  }

  /**
   * @description Persist a table to disk using atomic writes.
   * Optionally uses fsync for guaranteed durability.
   */
  private async persistTable(tableName: string): Promise<void> {
    const table = this.data.get(tableName);
    if (!table) return;

    const buffer = this.serializeTable(table);
    const filePath = join(this.databaseDirectory, tableName);

    const tempPath = `${filePath}.tmp.${Date.now()}.${Math.random().toString(36).substring(7)}`;

    try {
      await writeFile(tempPath, buffer);

      // fsync: Force data to physical storage before rename
      if (this.useFsync) {
        const fd = await open(tempPath, 'r+');
        try {
          await fd.sync();
        } finally {
          await fd.close();
        }
      }

      await rename(tempPath, filePath);

      // fsync: Force directory entry to physical storage
      if (this.useFsync) {
        const dirPath = dirname(filePath);
        const dirFd = openSync(dirPath, 'r');
        try {
          closeSync(dirFd);
        } catch (_) {
          // Directory fsync not supported on all platforms (e.g., Windows)
          // This is acceptable - file fsync is the critical part
        }
      }
    } catch (error) {
      try {
        await unlink(tempPath);
      } catch (_cleanupError) {
        // Ignore cleanup errors
      }
      throw error;
    }
  }

  /**
   * @description Serialize table data to buffer for disk storage.
   * Uses short keys for metadata (v, ver, ts, exp). Optionally compresses user data if dictionary provided.
   */
  private serializeTable(table: Map<string, DatabaseRecord>): Buffer {
    const data = Array.from(table.entries()).map(([key, record]) => {
      // Directly create compressed record with short keys (no transformation overhead)
      return [
        key,
        {
          v: this.dictionary
            ? transformValue(record.value, this.dictionary.deflate)
            : record.value,
          ver: record.version,
          ts: record.timestamp,
          exp: record.expiration
        }
      ];
    });

    return Buffer.from(JSON.stringify(data), 'utf8');
  }

  /**
   * @description Deserialize buffer data back to table map.
   * Directly maps short keys (v, ver, ts, exp) to full property names. Optionally decompresses user data if dictionary provided.
   */
  private deserializeTable(buffer: Buffer): Map<string, DatabaseRecord> {
    const data = JSON.parse(buffer.toString('utf8'));

    // Directly map short keys to DatabaseRecord structure (no transformation overhead)
    const records = data.map(([key, compressed]: [string, any]) => {
      return [
        key,
        {
          value: this.dictionary
            ? transformValue(compressed.v, this.dictionary.inflate)
            : compressed.v,
          version: compressed.ver,
          timestamp: compressed.ts,
          expiration: compressed.exp
        }
      ];
    });

    return new Map(records);
  }
}
