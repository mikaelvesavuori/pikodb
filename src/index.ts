import { existsSync, mkdirSync, readdirSync } from 'node:fs';
import { readFile, writeFile, rename, unlink } from 'node:fs/promises';
import { join } from 'node:path';

export interface DatabaseOptions {
  databaseDirectory: string;
}

export interface DatabaseRecord {
  value: any;
  version: number;
  timestamp: number;
  expiration: number | null;
}

/**
 * AtomDB is a reliable, simple, fast, no-frills key-value database.
 *
 * Features:
 * - Immediate disk persistence with atomic writes
 * - No WAL complexity or checkpoint issues
 * - Simple memory + disk architecture
 * - Full data type support
 * - Concurrent operation safety
 */
export class AtomDB {
  private readonly data: Map<string, Map<string, DatabaseRecord>> = new Map();
  private readonly databaseDirectory: string;

  constructor(options: DatabaseOptions) {
    this.databaseDirectory = options.databaseDirectory;

    if (!existsSync(this.databaseDirectory))
      mkdirSync(this.databaseDirectory, { recursive: true });
  }

  /**
   * @description Initialize the database by loading existing tables from disk.
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
   */
  async write(
    tableName: string,
    key: string,
    value: any,
    expirationTimestamp?: number
  ): Promise<boolean> {
    if (!tableName || key === undefined) return false;

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
   */
  async get(tableName: string, key?: string): Promise<any> {
    if (!tableName) return key ? undefined : [];

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
   * @description Delete a key from a table with immediate disk persistence,
   */
  async delete(tableName: string, key: string): Promise<boolean> {
    if (!tableName || key === undefined) return false;

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
   */
  async getTableSize(tableName: string): Promise<number> {
    if (!tableName) return 0;

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
   */
  async cleanupExpired(tableName: string): Promise<number> {
    if (!tableName) return 0;

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
   */
  async cleanupAllExpired(): Promise<number> {
    let totalCleaned = 0;

    for (const tableName of this.data.keys())
      totalCleaned += await this.cleanupExpired(tableName);

    return totalCleaned;
  }

  /**
   * @description Delete an entire table and its disk file.
   */
  async deleteTable(tableName: string): Promise<boolean> {
    if (!tableName) return false;

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
   */
  listTables(): string[] {
    return Array.from(this.data.keys());
  }

  /**
   * @description Force persistence of all in-memory tables to disk.
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
   */
  private async persistTable(tableName: string): Promise<void> {
    const table = this.data.get(tableName);
    if (!table) return;

    const buffer = this.serializeTable(table);
    const filePath = join(this.databaseDirectory, tableName);

    const tempPath = `${filePath}.tmp.${Date.now()}.${Math.random().toString(36).substring(7)}`;

    try {
      await writeFile(tempPath, buffer);
      await rename(tempPath, filePath);
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
   */
  private serializeTable(table: Map<string, DatabaseRecord>): Buffer {
    const data = Array.from(table.entries());
    return Buffer.from(JSON.stringify(data), 'utf8');
  }

  /**
   * @description Deserialize buffer data back to table map.
   */
  private deserializeTable(buffer: Buffer): Map<string, DatabaseRecord> {
    const data = JSON.parse(buffer.toString('utf8'));
    return new Map(data);
  }
}
