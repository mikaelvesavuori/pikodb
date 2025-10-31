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
  type ProcessedDictionary,
  type Dictionary,
  processDictionary,
  transformValue
} from '../interfaces/Dictionary';
import type { DatabaseOptions, DatabaseRecord } from '../interfaces';

import {
  validateKey,
  validateTableName,
  validateValue
} from '../utils/validation';

/**
 * @description PikoDB is a reliable, simple, fast, no-frills key-value database.
 *
 * Features:
 * - Immediate disk persistence with atomic writes
 * - Simple memory + disk architecture
 * - Full data type support
 * - Concurrent operation safety
 * - Multiple named dictionaries for flexible compression
 * - Dynamic dictionary management (add/remove at runtime)
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
 * // With multiple named dictionaries
 * const db = new PikoDB({
 *   databaseDirectory: './data',
 *   dictionaries: {
 *     sensors: {
 *       deflate: {
 *         sensor: 's',
 *         temperature: 't',
 *         humidity: 'h'
 *       }
 *     },
 *     users: {
 *       deflate: {
 *         username: 'u',
 *         email: 'e',
 *         created: 'c'
 *       }
 *     }
 *   }
 * });
 * await db.start();
 *
 * // Use different dictionaries for different data
 * await db.write('readings', 'r1', { sensor: 'DHT22', temperature: 23.5 }, undefined, 'sensors');
 * await db.write('users', 'user1', { username: 'alice', email: 'alice@example.com' }, undefined, 'users');
 *
 * const reading = await db.get('readings', 'r1'); // { sensor: 'DHT22', temperature: 23.5 }
 * await db.close();
 *
 * @example
 * // Add dictionaries dynamically
 * const db = new PikoDB({ databaseDirectory: './data' });
 * await db.start();
 *
 * db.addDictionary('metrics', {
 *   deflate: { timestamp: 'ts', value: 'v', unit: 'u' }
 * });
 *
 * await db.write('metrics', 'm1', { timestamp: Date.now(), value: 42, unit: 'celsius' }, undefined, 'metrics');
 * await db.close();
 */
export class PikoDB {
  private readonly data: Map<string, Map<string, DatabaseRecord>> = new Map();
  private readonly databaseDirectory: string;
  private readonly dictionaries: Map<string, ProcessedDictionary> = new Map();
  private readonly useFsync: boolean;

  constructor(options: DatabaseOptions) {
    this.databaseDirectory = options.databaseDirectory;
    this.useFsync = options.durableWrites ?? false;

    if (options.dictionaries) {
      Object.entries(options.dictionaries).forEach(([name, dict]) => {
        const processed = processDictionary(dict);
        this.dictionaries.set(name, processed);
      });
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
   * @param dictionaryName - Optional dictionary name to use for compression
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
   * // Write with specific dictionary
   * await db.write('sensors', 'sensor1', {
   *   sensor: 'DHT22',
   *   temperature: 23.5
   * }, undefined, 'sensorDict');
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
    expirationTimestamp?: number,
    dictionaryName?: string
  ): Promise<boolean> {
    // Validate inputs (throws on error)
    validateTableName(tableName);
    validateKey(key);
    validateValue(value);

    // Validate dictionary name if provided
    if (dictionaryName && !this.dictionaries.has(dictionaryName)) {
      throw new Error(
        `Dictionary "${dictionaryName}" not found. Available dictionaries: ${Array.from(this.dictionaries.keys()).join(', ') || 'none'}`
      );
    }

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
        expiration: expirationTimestamp || null,
        dictionaryName: dictionaryName || undefined
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
        if (this.isExpired(record)) expiredKeys.push(key);
        else size++;
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
   * @description Add or update a dictionary for compression after database instantiation.
   * If a dictionary with the same name already exists, it will be updated.
   *
   * @param name - The name to identify this dictionary
   * @param dictionary - The dictionary configuration (provide either deflate or inflate)
   * @throws If dictionary is invalid (neither deflate nor inflate, or both provided)
   *
   * @example
   * const db = new PikoDB({ databaseDirectory: './data' });
   * await db.start();
   *
   * // Add a dictionary for sensor data
   * db.addDictionary('sensors', {
   *   deflate: {
   *     sensor: 's',
   *     temperature: 't',
   *     humidity: 'h'
   *   }
   * });
   *
   * // Update the dictionary (no error will be thrown)
   * db.addDictionary('sensors', {
   *   deflate: {
   *     sensor: 's',
   *     temperature: 't',
   *     humidity: 'h',
   *     pressure: 'p'
   *   }
   * });
   *
   * // Use the dictionary when writing
   * await db.write('readings', 'r1', { sensor: 'DHT22', temperature: 23.5 }, undefined, 'sensors');
   */
  addDictionary(name: string, dictionary: Dictionary): void {
    const processed = processDictionary(dictionary);
    this.dictionaries.set(name, processed);
  }

  /**
   * @description Remove a dictionary by name.
   *
   * @param name - The name of the dictionary to remove
   * @returns True if dictionary was removed, false if it didn't exist
   *
   * @example
   * db.removeDictionary('sensors');
   */
  removeDictionary(name: string): boolean {
    return this.dictionaries.delete(name);
  }

  /**
   * @description List all available dictionary names.
   *
   * @returns Array of dictionary names
   *
   * @example
   * const dictionaries = db.listDictionaries();
   * console.log(dictionaries); // ['sensors', 'users', 'metrics']
   */
  listDictionaries(): string[] {
    return Array.from(this.dictionaries.keys());
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
   * Uses short keys for metadata (d, v, t, x, n).
   * Optionally compresses user data if dictionary provided.
   */
  private serializeTable(table: Map<string, DatabaseRecord>): Buffer {
    const data = Array.from(table.entries()).map(([key, record]) => {
      const dictionary = record.dictionaryName
        ? this.dictionaries.get(record.dictionaryName)
        : undefined;

      // Directly create compressed record with short keys
      const compressed: any = {
        d: dictionary
          ? transformValue(record.value, dictionary.deflate)
          : record.value,
        v: record.version,
        t: record.timestamp,
        x: record.expiration
      };

      // Only include dictionary name if it exists
      if (record.dictionaryName) compressed.n = record.dictionaryName;

      return [key, compressed];
    });

    return Buffer.from(JSON.stringify(data), 'utf8');
  }

  /**
   * @description Deserialize buffer data back to table map.
   * Directly maps short keys (d, v, t, x, n) to full property names. Optionally decompresses user data if dictionary provided.
   */
  private deserializeTable(buffer: Buffer): Map<string, DatabaseRecord> {
    const data = JSON.parse(buffer.toString('utf8'));

    // Directly map short keys to DatabaseRecord structure
    const records = data.map(([key, compressed]: [string, any]) => {
      const dictionaryName = compressed.n;
      const dictionary = dictionaryName
        ? this.dictionaries.get(dictionaryName)
        : undefined;

      return [
        key,
        {
          value: dictionary
            ? transformValue(compressed.d, dictionary.inflate)
            : compressed.d,
          version: compressed.v,
          timestamp: compressed.t,
          expiration: compressed.x,
          dictionaryName: dictionaryName || undefined
        }
      ];
    });

    return new Map(records);
  }
}
