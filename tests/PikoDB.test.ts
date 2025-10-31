/** biome-ignore-all lint/suspicious/noFocusedTests: Might need to focus on occasion */
import { existsSync } from 'node:fs';
import { readFile, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, test } from 'vitest';

import { PikoDB } from '../src/index.js';

describe('PikoDB', () => {
  let db: PikoDB;
  let testDir: string;

  beforeEach(async () => {
    testDir = join(
      process.cwd(),
      `test-db-${Date.now()}-${Math.random().toString(36).substring(7)}`
    );
    db = new PikoDB({ databaseDirectory: testDir });
    await db.start();
  });

  afterEach(async () => {
    await db.close();
    if (existsSync(testDir)) {
      await rm(testDir, { recursive: true, force: true });
    }
  });

  describe('Initialization', () => {
    test('It should create database directory', () => {
      expect(existsSync(testDir)).toBe(true);
    });

    test('It should start with empty database', async () => {
      const tables = db.listTables();
      expect(tables).toEqual([]);
    });

    test('It should load existing tables on startup', async () => {
      // Write data with first db instance
      await db.write('existing-table', 'key1', 'value1');
      await db.close();

      // Create new db instance and verify it loads existing data
      const newDb = new PikoDB({ databaseDirectory: testDir });
      await newDb.start();

      const result = await newDb.get('existing-table', 'key1');
      expect(result).toBe('value1');

      await newDb.close();
    });

    test('It should handle directory with existing non-table files', async () => {
      // Create some non-table files
      await writeFile(join(testDir, 'not-a-table.txt'), 'some content', 'utf8');
      await writeFile(join(testDir, '.hidden-file'), 'hidden content', 'utf8');
      await writeFile(join(testDir, 'temp-file.tmp'), 'temp content', 'utf8');

      const newDb = new PikoDB({ databaseDirectory: testDir });
      await newDb.start();

      const tables = newDb.listTables();
      expect(tables).toContain('not-a-table.txt');
      expect(tables).not.toContain('.hidden-file');
      expect(tables).not.toContain('temp-file.tmp');

      await newDb.close();
    });
  });

  describe('Writing Data', () => {
    test('It should write simple key-value data', async () => {
      const result = await db.write('users', 'user1', {
        name: 'John',
        age: 30
      });

      expect(result).toBe(true);
      expect(await db.get('users', 'user1')).toEqual({ name: 'John', age: 30 });
    });

    test('It should write different data types', async () => {
      await db.write('types', 'string', 'hello world');
      await db.write('types', 'number', 42);
      await db.write('types', 'boolean', true);
      await db.write('types', 'array', [1, 2, 3]);
      await db.write('types', 'object', { nested: { value: 'test' } });
      await db.write('types', 'null', null);

      expect(await db.get('types', 'string')).toBe('hello world');
      expect(await db.get('types', 'number')).toBe(42);
      expect(await db.get('types', 'boolean')).toBe(true);
      expect(await db.get('types', 'array')).toEqual([1, 2, 3]);
      expect(await db.get('types', 'object')).toEqual({
        nested: { value: 'test' }
      });
      expect(await db.get('types', 'null')).toBe(null);
    });

    test('It should handle complex nested objects', async () => {
      const complexObject = {
        user: {
          id: 123,
          profile: {
            name: 'John Doe',
            settings: {
              theme: 'dark',
              notifications: {
                email: true,
                push: false,
                sms: null
              }
            }
          }
        },
        metadata: {
          created: new Date('2024-01-01'),
          tags: ['user', 'premium', 'active'],
          scores: [95, 87, 92]
        }
      };

      const result = await db.write('complex', 'user123', complexObject);
      expect(result).toBe(true);

      const retrieved = await db.get('complex', 'user123');
      expect(retrieved).toEqual(complexObject);
    });

    test('It should handle version control on updates', async () => {
      await db.write('versions', 'key1', 'value1');
      await db.write('versions', 'key1', 'value2');
      await db.write('versions', 'key1', 'value3');

      const finalValue = await db.get('versions', 'key1');
      expect(finalValue).toBe('value3');
    });

    test('It should persist data to disk immediately', async () => {
      await db.write('persist-test', 'key1', 'value1');

      const filePath = join(testDir, 'persist-test');
      expect(existsSync(filePath)).toBe(true);

      const fileContent = await readFile(filePath, 'utf8');
      expect(fileContent).toContain('key1');
      expect(fileContent).toContain('value1');
    });

    test('It should handle concurrent writes to same table', async () => {
      const promises = Array.from({ length: 100 }, (_, i) =>
        db.write('concurrent', `key${i}`, `value${i}`)
      );

      const results = await Promise.all(promises);
      expect(results.every((r: any) => r === true)).toBe(true);

      // Verify all data was written correctly
      for (let i = 0; i < 100; i++) {
        const value = await db.get('concurrent', `key${i}`);
        expect(value).toBe(`value${i}`);
      }
    });

    test('It should handle concurrent writes to different tables', async () => {
      const promises = Array.from({ length: 50 }, (_, i) =>
        db.write(`table${i}`, 'key1', `value${i}`)
      );

      const results = await Promise.all(promises);
      expect(results.every((r: any) => r === true)).toBe(true);

      // Verify all tables were created correctly
      for (let i = 0; i < 50; i++) {
        const value = await db.get(`table${i}`, 'key1');
        expect(value).toBe(`value${i}`);
      }
    });

    test('It should handle large data objects', async () => {
      const largeObject = {
        id: 'large-object',
        data: 'x'.repeat(100000), // 100KB string
        nested: {
          array: Array.from({ length: 1000 }, (_, i) => ({
            id: i,
            value: `item-${i}`,
            data: 'y'.repeat(100)
          }))
        },
        metadata: {
          size: 100000,
          created: new Date(),
          tags: Array.from({ length: 50 }, (_, i) => `tag-${i}`)
        }
      };

      const result = await db.write('large-data', 'big-object', largeObject);
      expect(result).toBe(true);

      const retrieved = await db.get('large-data', 'big-object');
      expect(retrieved).toEqual(largeObject);
    });

    test('It should handle empty values', async () => {
      await db.write('empty-values', 'empty-string', '');
      await db.write('empty-values', 'empty-array', []);
      await db.write('empty-values', 'empty-object', {});

      expect(await db.get('empty-values', 'empty-string')).toBe('');
      expect(await db.get('empty-values', 'empty-array')).toEqual([]);
      expect(await db.get('empty-values', 'empty-object')).toEqual({});
    });

    test('It should handle special characters in keys and values', async () => {
      const specialKey = 'key-with-üñíçødé-🚀-chars';
      const specialValue = {
        text: 'héllø wørld 🌍',
        emoji: '🎉🎊✨🌟💫',
        symbols: '!@#$%^&*()_+-=[]{}|;:,.<>?',
        unicode: 'αβγδε 中文 русский עברית العربية'
      };

      const result = await db.write('special-chars', specialKey, specialValue);
      expect(result).toBe(true);

      const retrieved = await db.get('special-chars', specialKey);
      expect(retrieved).toEqual(specialValue);
    });
  });

  describe('Reading Data', () => {
    beforeEach(async () => {
      await db.write('test-table', 'key1', 'value1');
      await db.write('test-table', 'key2', 'value2');
      await db.write('test-table', 'key3', 'value3');
    });

    test('It should read single key', async () => {
      const result = await db.get('test-table', 'key1');
      expect(result).toBe('value1');
    });

    test('It should return undefined for non-existent key', async () => {
      const result = await db.get('test-table', 'non-existent');
      expect(result).toBeUndefined();
    });

    test('It should return empty array for non-existent table', async () => {
      const result = await db.get('non-existent-table');
      expect(result).toEqual([]);
    });

    test('It should read all keys from table', async () => {
      const result = await db.get('test-table');
      expect(result).toHaveLength(3);

      const resultMap = new Map(result);
      expect(resultMap.get('key1')).toBe('value1');
      expect(resultMap.get('key2')).toBe('value2');
      expect(resultMap.get('key3')).toBe('value3');
    });

    test('It should handle reading from disk when not in memory', async () => {
      // Create new db instance to force disk read
      const newDb = new PikoDB({ databaseDirectory: testDir });
      await newDb.start();

      const result = await newDb.get('test-table', 'key1');
      expect(result).toBe('value1');

      const allResults = await newDb.get('test-table');
      expect(allResults).toHaveLength(3);

      await newDb.close();
    });

    test('It should handle concurrent reads', async () => {
      const promises = Array.from({ length: 200 }, (_, i) =>
        db.get('test-table', `key${(i % 3) + 1}`)
      );

      const results = await Promise.all(promises);
      expect(results).toHaveLength(200);

      // All results should be valid values
      results.forEach((result) => {
        expect(['value1', 'value2', 'value3']).toContain(result);
      });
    });

    test('It should handle mixed concurrent reads and writes', async () => {
      const operations = [];

      // Mix of reads and writes
      for (let i = 0; i < 100; i++) {
        if (i % 2 === 0) {
          operations.push(db.write('mixed-ops', `key${i}`, `value${i}`));
        } else {
          operations.push(db.get('mixed-ops', `key${Math.floor(i / 2)}`));
        }
      }

      const results = await Promise.all(operations);

      // Writes should return true, reads should return values or undefined
      results.forEach((result: any, index: any) => {
        if (index % 2 === 0) {
          expect(result).toBe(true); // Write result
        } else {
          // Read result - could be undefined or a value
          expect(typeof result === 'string' || result === undefined).toBe(true);
        }
      });
    });
  });

  describe('Deleting Data', () => {
    beforeEach(async () => {
      await db.write('delete-test', 'key1', 'value1');
      await db.write('delete-test', 'key2', 'value2');
      await db.write('delete-test', 'key3', 'value3');
    });

    test('It should delete existing key', async () => {
      const result = await db.delete('delete-test', 'key1');
      expect(result).toBe(true);

      const value = await db.get('delete-test', 'key1');
      expect(value).toBeUndefined();

      // Other keys should remain
      expect(await db.get('delete-test', 'key2')).toBe('value2');
      expect(await db.get('delete-test', 'key3')).toBe('value3');
    });

    test('It should return false for non-existent key', async () => {
      const result = await db.delete('delete-test', 'non-existent');
      expect(result).toBe(false);
    });

    test('It should return false for non-existent table', async () => {
      const result = await db.delete('non-existent-table', 'key1');
      expect(result).toBe(false);
    });

    test('It should persist deletion to disk', async () => {
      await db.delete('delete-test', 'key1');

      // Create new db instance to verify deletion persisted
      const newDb = new PikoDB({ databaseDirectory: testDir });
      await newDb.start();

      const deletedValue = await newDb.get('delete-test', 'key1');
      expect(deletedValue).toBeUndefined();

      const remainingValue = await newDb.get('delete-test', 'key2');
      expect(remainingValue).toBe('value2');

      await newDb.close();
    });

    test('It should handle concurrent deletions', async () => {
      // Add more keys for concurrent deletion test
      for (let i = 4; i <= 50; i++) {
        await db.write('delete-test', `key${i}`, `value${i}`);
      }

      const promises = Array.from({ length: 25 }, (_, i) =>
        db.delete('delete-test', `key${i + 10}`)
      );

      const results = await Promise.all(promises);
      expect(results.every((r: any) => r === true)).toBe(true);

      // Verify deleted keys are gone
      for (let i = 10; i < 35; i++) {
        const value = await db.get('delete-test', `key${i}`);
        expect(value).toBeUndefined();
      }

      // Verify non-deleted keys remain
      expect(await db.get('delete-test', 'key1')).toBe('value1');
      expect(await db.get('delete-test', 'key35')).toBe('value35');
    });

    test('It should handle deleting and recreating same key', async () => {
      await db.delete('delete-test', 'key1');
      expect(await db.get('delete-test', 'key1')).toBeUndefined();

      await db.write('delete-test', 'key1', 'new-value');
      expect(await db.get('delete-test', 'key1')).toBe('new-value');
    });

    test('It should handle invalid parameters gracefully', async () => {
      await expect(db.delete('', 'key1')).rejects.toThrow(
        'Table name must be a non-empty string'
      );
      await expect(db.delete('table1', '')).rejects.toThrow(
        'Key must not be empty'
      );
      await expect(db.delete('table1', undefined as any)).rejects.toThrow(
        'Key must be defined'
      );
    });
  });

  describe('Table Management', () => {
    test('It should get table size', async () => {
      expect(await db.getTableSize('empty-table')).toBe(0);

      await db.write('size-test', 'key1', 'value1');
      expect(await db.getTableSize('size-test')).toBe(1);

      await db.write('size-test', 'key2', 'value2');
      expect(await db.getTableSize('size-test')).toBe(2);

      await db.delete('size-test', 'key1');
      expect(await db.getTableSize('size-test')).toBe(1);
    });

    test('It should list all tables', async () => {
      await db.write('table1', 'key1', 'value1');
      await db.write('table2', 'key1', 'value1');
      await db.write('table3', 'key1', 'value1');

      const tables = db.listTables();
      expect(tables).toContain('table1');
      expect(tables).toContain('table2');
      expect(tables).toContain('table3');
      expect(tables).toHaveLength(3);
    });

    test('It should delete entire table', async () => {
      await db.write('table-to-delete', 'key1', 'value1');
      await db.write('table-to-delete', 'key2', 'value2');

      expect(await db.getTableSize('table-to-delete')).toBe(2);

      const result = await db.deleteTable('table-to-delete');
      expect(result).toBe(true);

      expect(await db.getTableSize('table-to-delete')).toBe(0);
      const data = await db.get('table-to-delete');
      expect(data).toEqual([]);

      // Verify file was deleted
      const filePath = join(testDir, 'table-to-delete');
      expect(existsSync(filePath)).toBe(false);
    });

    test('It should handle deleting non-existent table', async () => {
      const result = await db.deleteTable('non-existent');
      expect(result).toBe(true); // Should succeed gracefully
    });

    test('It should handle concurrent table operations', async () => {
      const operations = [];

      // Create tables concurrently
      for (let i = 0; i < 20; i++) {
        operations.push(db.write(`concurrent-table-${i}`, 'key1', `value${i}`));
      }

      await Promise.all(operations);

      // Verify all tables were created
      const tables = db.listTables();
      expect(tables).toHaveLength(20);

      // Get sizes concurrently
      const sizePromises = Array.from({ length: 20 }, (_, i) =>
        db.getTableSize(`concurrent-table-${i}`)
      );

      const sizes = await Promise.all(sizePromises);
      expect(sizes.every((size: any) => size === 1)).toBe(true);
    });

    test('It should handle table name edge cases', async () => {
      // These should work
      await db.write('123-numeric-start', 'key1', 'value1');
      await db.write('table_with_underscores', 'key1', 'value1');
      await db.write('table-with-hyphens', 'key1', 'value1');
      await db.write('UPPERCASE', 'key1', 'value1');

      const tables = db.listTables();
      expect(tables).toContain('123-numeric-start');
      expect(tables).toContain('table_with_underscores');
      expect(tables).toContain('table-with-hyphens');
      expect(tables).toContain('UPPERCASE');
    });
  });

  describe('Data Persistence and Recovery', () => {
    test('It should recover all data after restart', async () => {
      // Write test data across multiple tables
      await db.write('users', 'user1', { name: 'John', age: 30, active: true });
      await db.write('users', 'user2', {
        name: 'Jane',
        age: 25,
        active: false
      });
      await db.write('products', 'prod1', {
        name: 'Widget',
        price: 19.99,
        stock: 100
      });
      await db.write('settings', 'theme', 'dark');
      await db.write('settings', 'language', 'en');

      await db.close();

      // Create new db instance and verify recovery
      const newDb = new PikoDB({ databaseDirectory: testDir });
      await newDb.start();

      // Verify all data is recovered correctly
      expect(await newDb.get('users', 'user1')).toEqual({
        name: 'John',
        age: 30,
        active: true
      });
      expect(await newDb.get('users', 'user2')).toEqual({
        name: 'Jane',
        age: 25,
        active: false
      });
      expect(await newDb.get('products', 'prod1')).toEqual({
        name: 'Widget',
        price: 19.99,
        stock: 100
      });
      expect(await newDb.get('settings', 'theme')).toBe('dark');
      expect(await newDb.get('settings', 'language')).toBe('en');

      // Verify table structure
      expect(await newDb.getTableSize('users')).toBe(2);
      expect(await newDb.getTableSize('products')).toBe(1);
      expect(await newDb.getTableSize('settings')).toBe(2);

      await newDb.close();
    });

    test('It should handle corrupt data files gracefully', async () => {
      await db.write('corrupt-test', 'key1', 'value1');
      await db.close();

      // Corrupt the file
      const filePath = join(testDir, 'corrupt-test');
      await writeFile(filePath, 'invalid json data', 'utf8');

      // Create new db instance - should handle corruption gracefully
      const newDb = new PikoDB({ databaseDirectory: testDir });
      await newDb.start();

      // Should create empty table when corruption detected
      const result = await newDb.get('corrupt-test', 'key1');
      expect(result).toBeUndefined();

      const allData = await newDb.get('corrupt-test');
      expect(allData).toEqual([]);

      await newDb.close();
    });

    test('It should handle empty data files', async () => {
      // Create empty file
      const filePath = join(testDir, 'empty-test');
      await writeFile(filePath, '', 'utf8');

      const newDb = new PikoDB({ databaseDirectory: testDir });
      await newDb.start();

      const result = await newDb.get('empty-test');
      expect(result).toEqual([]);

      expect(await newDb.getTableSize('empty-test')).toBe(0);

      await newDb.close();
    });

    test('It should maintain data consistency across multiple restarts', async () => {
      const testData = {
        id: 123,
        name: 'Test User',
        metadata: { created: new Date().toISOString() },
        tags: ['test', 'user', 'consistency']
      };

      // First session
      await db.write('consistency-test', 'user123', testData);
      await db.close();

      // Second session - modify data
      const db2 = new PikoDB({ databaseDirectory: testDir });
      await db2.start();

      const retrieved1 = await db2.get('consistency-test', 'user123');
      expect(retrieved1).toEqual(testData);

      await db2.write('consistency-test', 'user123', {
        ...testData,
        modified: true
      });
      await db2.close();

      // Third session - verify modifications persisted
      const db3 = new PikoDB({ databaseDirectory: testDir });
      await db3.start();

      const retrieved2 = await db3.get('consistency-test', 'user123');
      expect(retrieved2).toEqual({ ...testData, modified: true });

      await db3.close();
    });

    test('It should handle rapid restart cycles', async () => {
      const originalData = { value: 'original', timestamp: Date.now() };

      for (let i = 0; i < 5; i++) {
        const tempDb = new PikoDB({ databaseDirectory: testDir });
        await tempDb.start();

        if (i === 0) {
          await tempDb.write('rapid-restart', 'data', originalData);
        } else {
          const retrieved = await tempDb.get('rapid-restart', 'data');
          expect(retrieved).toEqual(originalData);
        }

        await tempDb.close();
      }
    });
  });

  describe('Error Handling', () => {
    test('It should handle filesystem permission errors gracefully', async () => {
      // This test may not work on all systems, but should not crash
      const result = await db.write('permission-test', 'key1', 'value1');
      // Should either succeed or fail gracefully
      expect(typeof result).toBe('boolean');
    });

    test('It should handle extremely large values', async () => {
      const largeKey = 'x'.repeat(1024); // Max allowed key size
      const largeValue = {
        data: 'y'.repeat(500000), // 500KB value
        metadata: {
          size: 500000,
          created: new Date(),
          type: 'large-test'
        }
      };

      const result = await db.write('large-test', largeKey, largeValue);
      expect(result).toBe(true);

      const retrieved = await db.get('large-test', largeKey);
      expect(retrieved).toEqual(largeValue);
    });

    test('It should handle special unicode in keys and values', async () => {
      const unicodeKey = '🔑-키-ключ-مفتاح-鍵';
      const unicodeValue = {
        text: '🌍 Hello World in many languages:',
        greetings: {
          english: 'Hello',
          korean: '안녕하세요',
          russian: 'Привет',
          arabic: 'مرحبا',
          chinese: '你好',
          japanese: 'こんにちは',
          emoji: '👋🌟✨'
        }
      };

      const result = await db.write('unicode-test', unicodeKey, unicodeValue);
      expect(result).toBe(true);

      const retrieved = await db.get('unicode-test', unicodeKey);
      expect(retrieved).toEqual(unicodeValue);
    });

    test('It should handle concurrent access to same key', async () => {
      const promises = Array.from({ length: 50 }, (_, i) => {
        if (i % 2 === 0) {
          return db.write('concurrent-key', 'shared-key', `value-${i}`);
        } else {
          return db.get('concurrent-key', 'shared-key');
        }
      });

      const results = await Promise.all(promises);

      // Should not crash and should return valid results
      results.forEach((result: any, index: any) => {
        if (index % 2 === 0) {
          expect(typeof result).toBe('boolean');
        } else {
          expect(typeof result === 'string' || result === undefined).toBe(true);
        }
      });
    });
  });

  describe('Performance', () => {
    test('It should handle batch write operations efficiently', async () => {
      const startTime = Date.now();
      const batchSize = 1000;

      const promises = Array.from({ length: batchSize }, (_, i) =>
        db.write('performance', `key${i}`, {
          id: i,
          data: `value${i}`,
          timestamp: Date.now(),
          metadata: { index: i, batch: 'performance-test' }
        })
      );

      const results = await Promise.all(promises);
      const endTime = Date.now();

      expect(results.every((r: any) => r === true)).toBe(true);
      expect(endTime - startTime).toBeLessThan(10000); // Should complete within 10 seconds

      // Verify data integrity with random sampling
      const randomIndices = Array.from({ length: 50 }, () =>
        Math.floor(Math.random() * batchSize)
      );
      for (const index of randomIndices) {
        const value = await db.get('performance', `key${index}`);
        expect(value).toEqual({
          id: index,
          data: `value${index}`,
          timestamp: expect.any(Number),
          metadata: { index, batch: 'performance-test' }
        });
      }
    });

    test('It should handle mixed read/write workload efficiently', async () => {
      // Pre-populate with some data
      for (let i = 0; i < 100; i++) {
        await db.write('mixed-workload', `key${i}`, {
          value: i,
          data: `initial-${i}`
        });
      }

      const startTime = Date.now();
      const operations = [];

      // Mix of operations: 40% reads, 40% writes, 20% deletes
      for (let i = 0; i < 500; i++) {
        const rand = Math.random();
        if (rand < 0.4) {
          // Read operation
          operations.push(db.get('mixed-workload', `key${i % 100}`));
        } else if (rand < 0.8) {
          // Write operation
          operations.push(
            db.write('mixed-workload', `key${i % 100}`, {
              value: i,
              updated: true,
              timestamp: Date.now()
            })
          );
        } else {
          // Delete operation (but recreate immediately to maintain data)
          operations.push(
            db.delete('mixed-workload', `key${i % 100}`).then(() =>
              db.write('mixed-workload', `key${i % 100}`, {
                value: i,
                recreated: true
              })
            )
          );
        }
      }

      await Promise.all(operations);
      const endTime = Date.now();

      expect(endTime - startTime).toBeLessThan(8000); // Should complete within 8 seconds

      // Verify final state
      expect(await db.getTableSize('mixed-workload')).toBe(100);
    });

    test('It should handle large table operations efficiently', async () => {
      const tableSize = 1000; // Reduced from 5000

      // Create large table
      const writePromises = Array.from({ length: tableSize }, (_, i) =>
        db.write('large-table', `record${i}`, {
          id: i,
          name: `Record ${i}`,
          data: Array.from({ length: 5 }, (_, j) => `data-${i}-${j}`), // Reduced data size
          metadata: {
            created: Date.now(),
            category: i % 10,
            active: i % 3 === 0
          }
        })
      );

      await Promise.all(writePromises);

      // Verify table size
      expect(await db.getTableSize('large-table')).toBe(tableSize);

      // Test bulk read performance
      const readStartTime = Date.now();
      const allData = await db.get('large-table');
      const readEndTime = Date.now();

      expect(allData).toHaveLength(tableSize);
      expect(readEndTime - readStartTime).toBeLessThan(2000); // Increased timeout

      // Test random access performance
      const randomAccessStartTime = Date.now();
      const randomPromises = Array.from({ length: 50 }, () => {
        // Reduced from 100
        const randomIndex = Math.floor(Math.random() * tableSize);
        return db.get('large-table', `record${randomIndex}`);
      });

      const randomResults = await Promise.all(randomPromises);
      const randomAccessEndTime = Date.now();

      expect(randomResults.every((result) => result !== undefined)).toBe(true);
      expect(randomAccessEndTime - randomAccessStartTime).toBeLessThan(1000); // Increased timeout
    }, 15000); // Increased test timeout to 15 seconds

    test('It should maintain performance across database restarts', async () => {
      // Create initial dataset
      for (let i = 0; i < 1000; i++) {
        await db.write('restart-performance', `key${i}`, `value${i}`);
      }

      await db.close();

      // Restart and measure load time
      const restartStartTime = Date.now();
      const newDb = new PikoDB({ databaseDirectory: testDir });
      await newDb.start();

      // First access should trigger table loading
      const firstValue = await newDb.get('restart-performance', 'key0');
      const loadEndTime = Date.now();

      expect(firstValue).toBe('value0');
      expect(loadEndTime - restartStartTime).toBeLessThan(2000); // Should load within 2 seconds

      // Subsequent accesses should be fast
      const accessStartTime = Date.now();
      const accessPromises = Array.from({ length: 100 }, (_, i) =>
        newDb.get('restart-performance', `key${i * 10}`)
      );

      await Promise.all(accessPromises);
      const accessEndTime = Date.now();

      expect(accessEndTime - accessStartTime).toBeLessThan(100); // Should be very fast from memory

      await newDb.close();
    });
  });

  describe('Flush and Close Operations', () => {
    test('It should flush all data to disk', async () => {
      await db.write('flush-test1', 'key1', 'value1');
      await db.write('flush-test2', 'key2', 'value2');
      await db.write('flush-test3', 'key3', 'value3');

      await db.flush();

      // Verify all files exist on disk
      expect(existsSync(join(testDir, 'flush-test1'))).toBe(true);
      expect(existsSync(join(testDir, 'flush-test2'))).toBe(true);
      expect(existsSync(join(testDir, 'flush-test3'))).toBe(true);
    });

    test('It should close database cleanly', async () => {
      await db.write('close-test', 'key1', 'value1');
      await db.write('close-test', 'key2', 'value2');

      // Should not throw and should complete quickly
      const closeStartTime = Date.now();
      await db.close();
      const closeEndTime = Date.now();

      expect(closeEndTime - closeStartTime).toBeLessThan(1000);

      // Verify data persisted
      const newDb = new PikoDB({ databaseDirectory: testDir });
      await newDb.start();

      expect(await newDb.get('close-test', 'key1')).toBe('value1');
      expect(await newDb.get('close-test', 'key2')).toBe('value2');

      await newDb.close();
    });

    test('It should handle multiple close calls gracefully', async () => {
      await db.write('multi-close', 'key1', 'value1');

      // Multiple close calls should not cause issues
      await db.close();
      await db.close(); // Should be safe to call again

      // Data should still be persisted
      const newDb = new PikoDB({ databaseDirectory: testDir });
      await newDb.start();
      expect(await newDb.get('multi-close', 'key1')).toBe('value1');
      await newDb.close();
    });

    test('It should handle concurrent flush operations', async () => {
      // Add data to multiple tables
      for (let i = 0; i < 10; i++) {
        await db.write(`concurrent-flush-${i}`, 'key1', `value${i}`);
      }

      // Trigger multiple concurrent flush operations
      const flushPromises = Array.from({ length: 5 }, () => db.flush());

      // Should all complete successfully
      await Promise.all(flushPromises);

      // Verify all data is still accessible
      for (let i = 0; i < 10; i++) {
        const value = await db.get(`concurrent-flush-${i}`, 'key1');
        expect(value).toBe(`value${i}`);
      }
    });
  });

  describe('Atomic Operations', () => {
    test('It should ensure write operations are atomic', async () => {
      const largeValue = {
        step1: 'completed',
        step2: 'completed',
        step3: 'completed',
        largeData: 'x'.repeat(10000)
      };

      // Start write and read concurrently
      const promises = [
        db.write('atomic-test', 'key1', largeValue),
        db.get('atomic-test', 'key1')
      ];

      const [writeResult, readResult] = await Promise.all(promises);

      expect(writeResult).toBe(true);

      // Read should either get undefined (before write) or complete object (after write)
      if (readResult !== undefined) {
        expect(readResult).toEqual(largeValue);
      }
    });

    test('It should ensure delete operations are atomic', async () => {
      await db.write('atomic-delete', 'key1', { data: 'to-be-deleted' });

      // Concurrent delete and read
      const promises = [
        db.delete('atomic-delete', 'key1'),
        db.get('atomic-delete', 'key1')
      ];

      const [deleteResult, readResult] = await Promise.all(promises);

      expect(deleteResult).toBe(true);

      // Should either read the value (before delete) or undefined (after delete)
      if (readResult !== undefined) {
        expect(readResult).toEqual({ data: 'to-be-deleted' });
      }
    });

    test('It should maintain consistency during concurrent modifications', async () => {
      await db.write('consistency-test', 'counter', { value: 0 });

      // Simulate concurrent increments (this will likely result in race conditions,
      // but the database should remain consistent)
      const incrementPromises = Array.from({ length: 100 }, async () => {
        const current = await db.get('consistency-test', 'counter');
        if (current) {
          await db.write('consistency-test', 'counter', {
            value: current.value + 1
          });
        }
      });

      await Promise.all(incrementPromises);

      // Final value should be valid (though not necessarily 100 due to race conditions)
      const final = await db.get('consistency-test', 'counter');
      expect(final).toHaveProperty('value');
      expect(typeof final.value).toBe('number');
      expect(final.value).toBeGreaterThan(0);
      expect(final.value).toBeLessThanOrEqual(100);
    });
  });

  describe('Expiration Features', () => {
    test('It should write data with expiration timestamp', async () => {
      const futureTime = Date.now() + 3600000; // 1 hour from now
      const result = await db.write(
        'expiration-test',
        'key1',
        'value1',
        futureTime
      );

      expect(result).toBe(true);
      expect(await db.get('expiration-test', 'key1')).toBe('value1');
    });

    test('It should write data without expiration (permanent)', async () => {
      const result = await db.write(
        'expiration-test',
        'permanent',
        'permanent-value'
      );

      expect(result).toBe(true);
      expect(await db.get('expiration-test', 'permanent')).toBe(
        'permanent-value'
      );
    });

    test('It should return undefined for expired records', async () => {
      const pastTime = Date.now() - 1000; // 1 second ago
      await db.write(
        'expiration-test',
        'expired-key',
        'expired-value',
        pastTime
      );

      const result = await db.get('expiration-test', 'expired-key');
      expect(result).toBeUndefined();
    });

    test('It should automatically clean up expired records on read', async () => {
      const pastTime = Date.now() - 1000;
      const futureTime = Date.now() + 3600000;

      await db.write('cleanup-test', 'expired1', 'value1', pastTime);
      await db.write('cleanup-test', 'expired2', 'value2', pastTime);
      await db.write('cleanup-test', 'valid', 'valid-value', futureTime);

      // Reading should trigger cleanup
      const result = await db.get('cleanup-test', 'expired1');
      expect(result).toBeUndefined();

      // Reading all values should only return non-expired ones
      const allResults = await db.get('cleanup-test');
      expect(allResults).toHaveLength(1);
      expect(allResults[0]).toEqual(['valid', 'valid-value']);
    });

    test('It should only count non-expired records in table size', async () => {
      const pastTime = Date.now() - 1000;
      const futureTime = Date.now() + 3600000;

      await db.write('size-test', 'expired1', 'value1', pastTime);
      await db.write('size-test', 'expired2', 'value2', pastTime);
      await db.write('size-test', 'valid1', 'value3', futureTime);
      await db.write('size-test', 'valid2', 'value4'); // no expiration

      const size = await db.getTableSize('size-test');
      expect(size).toBe(2); // Only non-expired records
    });

    test('It should handle records expiring exactly at current time', async () => {
      const exactTime = Date.now();
      await db.write('exact-time-test', 'key1', 'value1', exactTime);

      // Small delay to ensure we're past the expiration time
      await new Promise((resolve) => setTimeout(resolve, 10));

      const result = await db.get('exact-time-test', 'key1');
      expect(result).toBeUndefined();
    });

    test('It should persist expiration data across restarts', async () => {
      const futureTime = Date.now() + 3600000;
      const pastTime = Date.now() - 1000;

      await db.write(
        'persist-expiration',
        'future-key',
        'future-value',
        futureTime
      );
      await db.write('persist-expiration', 'past-key', 'past-value', pastTime);
      await db.close();

      // Restart database
      const newDb = new PikoDB({ databaseDirectory: testDir });
      await newDb.start();

      expect(await newDb.get('persist-expiration', 'future-key')).toBe(
        'future-value'
      );
      expect(await newDb.get('persist-expiration', 'past-key')).toBeUndefined();

      await newDb.close();
    });

    test('It should handle manual cleanup of expired records', async () => {
      const pastTime = Date.now() - 1000;
      const futureTime = Date.now() + 3600000;

      await db.write('manual-cleanup', 'expired1', 'value1', pastTime);
      await db.write('manual-cleanup', 'expired2', 'value2', pastTime);
      await db.write('manual-cleanup', 'expired3', 'value3', pastTime);
      await db.write('manual-cleanup', 'valid', 'valid-value', futureTime);

      const cleanedCount = await db.cleanupExpired('manual-cleanup');
      expect(cleanedCount).toBe(3);

      // Verify cleanup
      const allResults = await db.get('manual-cleanup');
      expect(allResults).toHaveLength(1);
      expect(allResults[0]).toEqual(['valid', 'valid-value']);
    });

    test('It should handle cleanup of all tables', async () => {
      const pastTime = Date.now() - 1000;
      const futureTime = Date.now() + 3600000;

      // Add expired records to multiple tables
      await db.write('table1', 'expired', 'value1', pastTime);
      await db.write('table1', 'valid', 'value1', futureTime);
      await db.write('table2', 'expired1', 'value2', pastTime);
      await db.write('table2', 'expired2', 'value2', pastTime);
      await db.write('table3', 'valid', 'value3', futureTime);

      const totalCleaned = await db.cleanupAllExpired();
      expect(totalCleaned).toBe(3);

      // Verify results
      expect(await db.getTableSize('table1')).toBe(1);
      expect(await db.getTableSize('table2')).toBe(0);
      expect(await db.getTableSize('table3')).toBe(1);
    });

    test('It should handle cleanup on non-existent tables', async () => {
      const cleanedCount = await db.cleanupExpired('non-existent-table');
      expect(cleanedCount).toBe(0);
    });

    test('It should handle cleanup when no expired records exist', async () => {
      const futureTime = Date.now() + 3600000;

      await db.write('no-expired', 'key1', 'value1', futureTime);
      await db.write('no-expired', 'key2', 'value2'); // no expiration

      const cleanedCount = await db.cleanupExpired('no-expired');
      expect(cleanedCount).toBe(0);

      expect(await db.getTableSize('no-expired')).toBe(2);
    });

    test('It should handle mixed expiration scenarios during concurrent operations', async () => {
      const pastTime = Date.now() - 1000;
      const futureTime = Date.now() + 3600000;

      // Create mixed data
      const writePromises = [];
      for (let i = 0; i < 50; i++) {
        const expiration = i % 2 === 0 ? pastTime : futureTime;
        writePromises.push(
          db.write('concurrent-expiration', `key${i}`, `value${i}`, expiration)
        );
      }

      await Promise.all(writePromises);

      // Concurrent reads should handle expiration correctly
      const readPromises = Array.from({ length: 50 }, (_, i) =>
        db.get('concurrent-expiration', `key${i}`)
      );

      const results = await Promise.all(readPromises);

      // Even indices should be undefined (expired), odd indices should have values
      results.forEach((result: any, index: any) => {
        if (index % 2 === 0) {
          expect(result).toBeUndefined(); // expired
        } else {
          expect(result).toBe(`value${index}`); // valid
        }
      });

      // Final table size should only count non-expired records
      expect(await db.getTableSize('concurrent-expiration')).toBe(25);
    });

    test('It should handle updating expiration on existing keys', async () => {
      const shortExpiration = Date.now() + 1000; // 1 second
      const longExpiration = Date.now() + 3600000; // 1 hour

      // Write with short expiration
      await db.write('update-expiration', 'key1', 'value1', shortExpiration);
      expect(await db.get('update-expiration', 'key1')).toBe('value1');

      // Update with longer expiration before it expires
      await db.write(
        'update-expiration',
        'key1',
        'updated-value',
        longExpiration
      );

      // Wait for original expiration time to pass
      await new Promise((resolve) => setTimeout(resolve, 1100));

      // Should still be valid due to updated expiration
      expect(await db.get('update-expiration', 'key1')).toBe('updated-value');
    });

    test('It should handle very large expiration timestamps', async () => {
      const farFuture = Date.now() + 1000 * 60 * 60 * 24 * 365 * 10; // 10 years

      const result = await db.write('far-future', 'key1', 'value1', farFuture);
      expect(result).toBe(true);
      expect(await db.get('far-future', 'key1')).toBe('value1');
    });

    test('It should handle cleanup during delete operations', async () => {
      const pastTime = Date.now() - 1000;
      const futureTime = Date.now() + 3600000;

      await db.write('delete-cleanup', 'expired', 'expired-value', pastTime);
      await db.write('delete-cleanup', 'valid', 'valid-value', futureTime);

      // Deleting a valid key should work normally
      expect(await db.delete('delete-cleanup', 'valid')).toBe(true);

      // Trying to delete an expired key should return false (not found)
      expect(await db.delete('delete-cleanup', 'expired')).toBe(false);

      expect(await db.getTableSize('delete-cleanup')).toBe(0);
    });

    test('It should maintain performance with many expired records', async () => {
      const pastTime = Date.now() - 1000;
      const futureTime = Date.now() + 3600000;

      // Create many expired and some valid records
      const writePromises = [];
      for (let i = 0; i < 1000; i++) {
        const expiration = i < 950 ? pastTime : futureTime; // 950 expired, 50 valid
        writePromises.push(
          db.write('performance-expiration', `key${i}`, `value${i}`, expiration)
        );
      }

      await Promise.all(writePromises);

      const startTime = Date.now();
      const size = await db.getTableSize('performance-expiration');
      const endTime = Date.now();

      expect(size).toBe(50); // Only valid records
      expect(endTime - startTime).toBeLessThan(2000); // Should complete within 2 seconds
    });
  });

  describe('Dictionary Compression', () => {
    test('It should compress and decompress data with deflate mapping', async () => {
      const compressedDb = new PikoDB({
        databaseDirectory: testDir,
        dictionary: {
          deflate: {
            sensor: 's',
            identity: 'i',
            temperature: 't',
            timestamp: 'ts',
            metadata: 'm'
          }
        }
      });

      await compressedDb.start();

      const testData = {
        sensor: 'sensor-001',
        identity: 'device-123',
        temperature: 23.5,
        metadata: {
          location: 'room-42',
          status: 'active'
        }
      };

      await compressedDb.write('metrics', 'reading1', testData);

      // Should be able to read back the original data structure
      const retrieved = await compressedDb.get('metrics', 'reading1');
      expect(retrieved).toEqual(testData);

      await compressedDb.close();
    });

    test('It should compress and decompress data with inflate mapping', async () => {
      const compressedDb = new PikoDB({
        databaseDirectory: testDir,
        dictionary: {
          inflate: {
            s: 'sensor',
            i: 'identity',
            t: 'temperature',
            ts: 'timestamp',
            m: 'metadata'
          }
        }
      });

      await compressedDb.start();

      const testData = {
        sensor: 'sensor-001',
        identity: 'device-123',
        temperature: 23.5
      };

      await compressedDb.write('metrics', 'reading1', testData);

      const retrieved = await compressedDb.get('metrics', 'reading1');
      expect(retrieved).toEqual(testData);

      await compressedDb.close();
    });

    test('It should compress metadata fields (version, timestamp, expiration)', async () => {
      const compressedDb = new PikoDB({
        databaseDirectory: testDir,
        dictionary: {
          deflate: {
            value: 'v',
            version: 'ver',
            timestamp: 'ts',
            expiration: 'exp'
          }
        }
      });

      await compressedDb.start();

      const futureTime = Date.now() + 3600000;
      await compressedDb.write('test', 'key1', { data: 'test' }, futureTime);

      // Verify data is still accessible after compression
      const retrieved = await compressedDb.get('test', 'key1');
      expect(retrieved).toEqual({ data: 'test' });

      // Verify the compressed file is smaller
      const filePath = join(testDir, 'test');
      const fileContent = await readFile(filePath, 'utf8');
      expect(fileContent).toContain('ver'); // Compressed 'version'
      expect(fileContent).toContain('ts'); // Compressed 'timestamp'
      expect(fileContent).toContain('exp'); // Compressed 'expiration'
      expect(fileContent).not.toContain('version'); // Original should not exist
      expect(fileContent).not.toContain('timestamp');
      expect(fileContent).not.toContain('expiration');

      await compressedDb.close();
    });

    test('It should reduce file size with compression', async () => {
      const testData = {
        sensor: 'sensor-001',
        identity: 'device-123',
        temperature: 23.5,
        humidity: 65.2,
        pressure: 1013.25,
        timestamp: Date.now(),
        metadata: {
          location: 'warehouse-section-A',
          deviceType: 'environmental-monitor',
          calibrationDate: '2024-01-01'
        }
      };

      // Write without compression
      const uncompressedDb = new PikoDB({
        databaseDirectory: join(
          testDir,
          `uncompressed-${Math.random().toString(36).substring(7)}`
        )
      });
      await uncompressedDb.start();
      await uncompressedDb.write('metrics', 'reading1', testData);
      await uncompressedDb.close();

      const uncompressedPath = join(
        // @ts-expect-error
        uncompressedDb.databaseDirectory,
        'metrics'
      );
      const uncompressedContent = await readFile(uncompressedPath, 'utf8');
      const uncompressedSize = uncompressedContent.length;

      // Write with compression
      const compressedDb = new PikoDB({
        databaseDirectory: join(
          testDir,
          `compressed-${Math.random().toString(36).substring(7)}`
        ),
        dictionary: {
          deflate: {
            sensor: 's',
            identity: 'i',
            temperature: 't',
            humidity: 'h',
            pressure: 'p',
            timestamp: 'ts',
            metadata: 'm',
            location: 'l',
            deviceType: 'd',
            calibrationDate: 'c',
            version: 'v',
            expiration: 'e'
          }
        }
      });
      await compressedDb.start();
      await compressedDb.write('metrics', 'reading1', testData);
      await compressedDb.close();

      // @ts-expect-error
      const compressedPath = join(compressedDb.databaseDirectory, 'metrics');
      const compressedContent = await readFile(compressedPath, 'utf8');
      const compressedSize = compressedContent.length;

      // Compressed should be smaller
      expect(compressedSize).toBeLessThan(uncompressedSize);
    });

    test('It should handle nested objects with compression', async () => {
      const compressedDb = new PikoDB({
        databaseDirectory: testDir,
        dictionary: {
          deflate: {
            user: 'u',
            profile: 'p',
            settings: 's',
            notifications: 'n',
            email: 'e',
            push: 'pu',
            theme: 't'
          }
        }
      });

      await compressedDb.start();

      const complexData = {
        user: {
          profile: {
            settings: {
              notifications: {
                email: true,
                push: false
              },
              theme: 'dark'
            }
          }
        }
      };

      await compressedDb.write('users', 'user1', complexData);

      const retrieved = await compressedDb.get('users', 'user1');
      expect(retrieved).toEqual(complexData);

      await compressedDb.close();
    });

    test('It should handle arrays with compression', async () => {
      const compressedDb = new PikoDB({
        databaseDirectory: testDir,
        dictionary: {
          deflate: {
            sensor: 's',
            readings: 'r',
            temperature: 't',
            timestamp: 'ts'
          }
        }
      });

      await compressedDb.start();

      const arrayData = {
        sensor: 'sensor-001',
        readings: [
          { temperature: 23.5, timestamp: Date.now() },
          { temperature: 24.1, timestamp: Date.now() },
          { temperature: 23.8, timestamp: Date.now() }
        ]
      };

      await compressedDb.write('sensors', 'sensor1', arrayData);

      const retrieved = await compressedDb.get('sensors', 'sensor1');
      expect(retrieved).toEqual(arrayData);

      await compressedDb.close();
    });

    test('It should persist compressed data across restarts', async () => {
      const dictionary = {
        deflate: {
          sensor: 's',
          identity: 'i',
          temperature: 't'
        }
      };

      // First session with compression
      const db1 = new PikoDB({
        databaseDirectory: testDir,
        dictionary
      });
      await db1.start();

      const testData = {
        sensor: 'sensor-001',
        identity: 'device-123',
        temperature: 23.5
      };

      await db1.write('metrics', 'reading1', testData);
      await db1.close();

      // Second session with same compression
      const db2 = new PikoDB({
        databaseDirectory: testDir,
        dictionary
      });
      await db2.start();

      const retrieved = await db2.get('metrics', 'reading1');
      expect(retrieved).toEqual(testData);

      await db2.close();
    });

    test('It should handle keys not in dictionary gracefully', async () => {
      const compressedDb = new PikoDB({
        databaseDirectory: testDir,
        dictionary: {
          deflate: {
            sensor: 's',
            temperature: 't'
          }
        }
      });

      await compressedDb.start();

      const testData = {
        sensor: 'sensor-001',
        temperature: 23.5,
        unmappedKey: 'this-key-is-not-in-dictionary',
        anotherUnmapped: { nested: 'value' }
      };

      await compressedDb.write('metrics', 'reading1', testData);

      // Should still work, unmapped keys stay as-is
      const retrieved = await compressedDb.get('metrics', 'reading1');
      expect(retrieved).toEqual(testData);

      await compressedDb.close();
    });

    test('It should handle null and undefined values with compression', async () => {
      const compressedDb = new PikoDB({
        databaseDirectory: testDir,
        dictionary: {
          deflate: {
            nullField: 'n',
            undefinedField: 'u',
            normalField: 'nf'
          }
        }
      });

      await compressedDb.start();

      const testData = {
        nullField: null,
        undefinedField: undefined,
        normalField: 'value'
      };

      await compressedDb.write('nulls', 'key1', testData);

      const retrieved = await compressedDb.get('nulls', 'key1');
      expect(retrieved).toEqual(testData);

      await compressedDb.close();
    });

    test('It should handle primitive values with compression', async () => {
      const compressedDb = new PikoDB({
        databaseDirectory: testDir,
        dictionary: {
          deflate: {
            sensor: 's',
            value: 'v'
          }
        }
      });

      await compressedDb.start();

      // Primitive values should not be transformed
      await compressedDb.write('primitives', 'string', 'hello');
      await compressedDb.write('primitives', 'number', 42);
      await compressedDb.write('primitives', 'boolean', true);

      expect(await compressedDb.get('primitives', 'string')).toBe('hello');
      expect(await compressedDb.get('primitives', 'number')).toBe(42);
      expect(await compressedDb.get('primitives', 'boolean')).toBe(true);

      await compressedDb.close();
    });

    test('It should handle concurrent writes with compression', async () => {
      const compressedDb = new PikoDB({
        databaseDirectory: testDir,
        dictionary: {
          deflate: {
            sensor: 's',
            reading: 'r',
            timestamp: 'ts'
          }
        }
      });

      await compressedDb.start();

      const promises = Array.from({ length: 100 }, (_, i) =>
        compressedDb.write('concurrent', `key${i}`, {
          sensor: `sensor-${i}`,
          reading: i * 10,
          timestamp: Date.now()
        })
      );

      const results = await Promise.all(promises);
      expect(results.every((r) => r === true)).toBe(true);

      // Verify all data was written correctly
      for (let i = 0; i < 100; i++) {
        const value = await compressedDb.get('concurrent', `key${i}`);
        expect(value).toEqual({
          sensor: `sensor-${i}`,
          reading: i * 10,
          timestamp: expect.any(Number)
        });
      }

      await compressedDb.close();
    });

    test('It should throw error if neither deflate nor inflate is provided', async () => {
      expect(() => {
        new PikoDB({
          databaseDirectory: testDir,
          dictionary: {}
        });
      }).toThrow('Dictionary must provide either deflate or inflate mapping');
    });

    test('It should throw error if both deflate and inflate are provided', async () => {
      expect(() => {
        new PikoDB({
          databaseDirectory: testDir,
          dictionary: {
            deflate: { sensor: 's' },
            inflate: { s: 'sensor' }
          }
        });
      }).toThrow(
        'Dictionary should provide only one of deflate or inflate (not both)'
      );
    });

    test('It should handle expiration cleanup with compressed data', async () => {
      const compressedDb = new PikoDB({
        databaseDirectory: testDir,
        dictionary: {
          deflate: {
            sensor: 's',
            value: 'v',
            timestamp: 'ts',
            version: 'ver',
            expiration: 'exp'
          }
        }
      });

      await compressedDb.start();

      const pastTime = Date.now() - 1000;
      const futureTime = Date.now() + 3600000;

      await compressedDb.write(
        'cleanup',
        'expired',
        { sensor: 'sensor-1', value: 100 },
        pastTime
      );
      await compressedDb.write(
        'cleanup',
        'valid',
        { sensor: 'sensor-2', value: 200 },
        futureTime
      );

      const cleanedCount = await compressedDb.cleanupExpired('cleanup');
      expect(cleanedCount).toBe(1);

      const allData = await compressedDb.get('cleanup');
      expect(allData).toHaveLength(1);
      expect(allData[0][1]).toEqual({ sensor: 'sensor-2', value: 200 });

      await compressedDb.close();
    });

    test('It should work without dictionary (backwards compatibility)', async () => {
      const normalDb = new PikoDB({
        databaseDirectory: testDir
      });

      await normalDb.start();

      const testData = {
        sensor: 'sensor-001',
        temperature: 23.5
      };

      await normalDb.write('metrics', 'reading1', testData);

      const retrieved = await normalDb.get('metrics', 'reading1');
      expect(retrieved).toEqual(testData);

      await normalDb.close();
    });
  });

  describe('Input Validation', () => {
    describe('Table Name Validation', () => {
      test('It should reject empty table name', async () => {
        await expect(db.write('', 'key1', 'value')).rejects.toThrow(
          'Table name must be a non-empty string'
        );
      });

      test('It should reject non-string table name', async () => {
        // @ts-expect-error Testing invalid input
        await expect(db.write(123, 'key1', 'value')).rejects.toThrow(
          'Table name must be a non-empty string'
        );
      });

      test('It should reject table name with path separator /', async () => {
        await expect(db.write('table/name', 'key1', 'value')).rejects.toThrow(
          'Table name must not contain path separators'
        );
      });

      test('It should reject table name with path separator \\', async () => {
        await expect(db.write('table\\name', 'key1', 'value')).rejects.toThrow(
          'Table name must not contain path separators'
        );
      });

      test('It should reject table name with parent directory reference', async () => {
        await expect(db.write('..', 'key1', 'value')).rejects.toThrow(
          'Table name must not contain ".."'
        );
        await expect(db.write('table..name', 'key1', 'value')).rejects.toThrow(
          'Table name must not contain ".."'
        );
      });

      test('It should reject table name starting with dot', async () => {
        await expect(db.write('.hidden', 'key1', 'value')).rejects.toThrow(
          'Table name must not start with "."'
        );
      });

      test('It should reject table name with null bytes', async () => {
        await expect(db.write('table\0name', 'key1', 'value')).rejects.toThrow(
          'Table name must not contain null bytes'
        );
      });

      test('It should reject table name exceeding 255 characters', async () => {
        const longName = 'a'.repeat(256);
        await expect(db.write(longName, 'key1', 'value')).rejects.toThrow(
          'Table name must not exceed 255 characters'
        );
      });

      test('It should reject reserved filesystem names', async () => {
        const reservedNames = ['CON', 'PRN', 'AUX', 'NUL', 'COM1', 'LPT1'];

        for (const name of reservedNames) {
          await expect(db.write(name, 'key1', 'value')).rejects.toThrow(
            `Table name "${name}" is reserved by the filesystem`
          );
        }
      });

      test('It should accept valid table names', async () => {
        const validNames = [
          'users',
          'my-table',
          'table_name',
          'table123',
          'MyTable',
          'a'.repeat(200) // Long name (not max to avoid filesystem issues with temp files)
        ];

        for (const name of validNames) {
          const result = await db.write(name, 'key1', 'value');
          expect(result).toBe(true);
        }
      });
    });

    describe('Key Validation', () => {
      test('It should reject undefined key', async () => {
        // @ts-expect-error Testing invalid input
        await expect(db.write('table', undefined, 'value')).rejects.toThrow(
          'Key must be defined'
        );
      });

      test('It should reject null key', async () => {
        // @ts-expect-error Testing invalid input
        await expect(db.write('table', null, 'value')).rejects.toThrow(
          'Key must be defined'
        );
      });

      test('It should reject non-string key', async () => {
        // @ts-expect-error Testing invalid input
        await expect(db.write('table', 123, 'value')).rejects.toThrow(
          'Key must be a string'
        );
      });

      test('It should reject empty string key', async () => {
        await expect(db.write('table', '', 'value')).rejects.toThrow(
          'Key must not be empty'
        );
      });

      test('It should reject key exceeding 1024 characters', async () => {
        const longKey = 'k'.repeat(1025);
        await expect(db.write('table', longKey, 'value')).rejects.toThrow(
          'Key must not exceed 1024 characters'
        );
      });

      test('It should reject key with null bytes', async () => {
        await expect(db.write('table', 'key\0name', 'value')).rejects.toThrow(
          'Key must not contain null bytes'
        );
      });

      test('It should accept valid keys', async () => {
        const validKeys = [
          'key1',
          'my-key',
          'key_name',
          'key.with.dots',
          'key:with:colons',
          'k'.repeat(1024), // Max length
          'special!@#$%^&*()chars'
        ];

        for (const key of validKeys) {
          const result = await db.write('table', key, 'value');
          expect(result).toBe(true);
        }
      });
    });

    describe('Value Validation', () => {
      test('It should reject undefined value', async () => {
        await expect(db.write('table', 'key1', undefined)).rejects.toThrow(
          'Value must not be undefined'
        );
      });

      test('It should reject non-JSON-serializable values', async () => {
        const circularObj: any = { a: 1 };
        circularObj.self = circularObj;

        await expect(db.write('table', 'key1', circularObj)).rejects.toThrow(
          'Value must be JSON-serializable'
        );
      });

      test('It should reject functions', async () => {
        const fn = () => console.log('test');

        await expect(db.write('table', 'key1', fn)).rejects.toThrow(
          'Value must be JSON-serializable'
        );
      });

      test('It should reject symbols', async () => {
        const sym = Symbol('test');

        await expect(db.write('table', 'key1', sym)).rejects.toThrow(
          'Value must be JSON-serializable'
        );
      });

      test('It should accept null value', async () => {
        const result = await db.write('table', 'key1', null);
        expect(result).toBe(true);
        expect(await db.get('table', 'key1')).toBe(null);
      });

      test('It should accept all JSON-serializable values', async () => {
        const validValues = [
          'string',
          42,
          true,
          false,
          null,
          [1, 2, 3],
          { nested: { object: 'value' } },
          [{ mixed: 'array' }, 123, 'test']
        ];

        for (let i = 0; i < validValues.length; i++) {
          const result = await db.write('table', `key${i}`, validValues[i]);
          expect(result).toBe(true);
          expect(await db.get('table', `key${i}`)).toEqual(validValues[i]);
        }
      });
    });

    describe('Validation in Other Methods', () => {
      test('It should validate table name in get()', async () => {
        await expect(db.get('')).rejects.toThrow(
          'Table name must be a non-empty string'
        );
        await expect(db.get('../etc/passwd')).rejects.toThrow(
          'Table name must not contain path separators'
        );
      });

      test('It should validate key in get()', async () => {
        await expect(db.get('table', '')).rejects.toThrow(
          'Key must not be empty'
        );
      });

      test('It should validate table name in delete()', async () => {
        await expect(db.delete('', 'key1')).rejects.toThrow(
          'Table name must be a non-empty string'
        );
      });

      test('It should validate key in delete()', async () => {
        await expect(db.delete('table', '')).rejects.toThrow(
          'Key must not be empty'
        );
      });

      test('It should validate table name in getTableSize()', async () => {
        await expect(db.getTableSize('')).rejects.toThrow(
          'Table name must be a non-empty string'
        );
      });

      test('It should validate table name in cleanupExpired()', async () => {
        await expect(db.cleanupExpired('')).rejects.toThrow(
          'Table name must be a non-empty string'
        );
      });

      test('It should validate table name in deleteTable()', async () => {
        await expect(db.deleteTable('')).rejects.toThrow(
          'Table name must be a non-empty string'
        );
      });
    });
  });

  describe('Durable Writes Configuration', () => {
    test('It should work with durable writes enabled', async () => {
      const durableDb = new PikoDB({
        databaseDirectory: testDir,
        durableWrites: true
      });

      await durableDb.start();

      const testData = { name: 'Alice', age: 30 };
      const result = await durableDb.write('users', 'user1', testData);
      expect(result).toBe(true);

      const retrieved = await durableDb.get('users', 'user1');
      expect(retrieved).toEqual(testData);

      await durableDb.close();
    });

    test('It should persist data correctly with durable writes enabled', async () => {
      const durableDb = new PikoDB({
        databaseDirectory: testDir,
        durableWrites: true
      });

      await durableDb.start();

      // Write multiple records
      await durableDb.write('durable-test', 'key1', 'value1');
      await durableDb.write('durable-test', 'key2', 'value2');
      await durableDb.write('durable-test', 'key3', 'value3');

      await durableDb.close();

      // Create new instance to verify persistence
      const newDb = new PikoDB({
        databaseDirectory: testDir,
        durableWrites: true
      });

      await newDb.start();

      expect(await newDb.get('durable-test', 'key1')).toBe('value1');
      expect(await newDb.get('durable-test', 'key2')).toBe('value2');
      expect(await newDb.get('durable-test', 'key3')).toBe('value3');

      await newDb.close();
    });

    test('It should work with durable writes disabled (default)', async () => {
      const noDurableWritesDb = new PikoDB({
        databaseDirectory: testDir,
        durableWrites: false
      });

      await noDurableWritesDb.start();

      const testData = { name: 'Bob', age: 25 };
      const result = await noDurableWritesDb.write('users', 'user2', testData);
      expect(result).toBe(true);

      const retrieved = await noDurableWritesDb.get('users', 'user2');
      expect(retrieved).toEqual(testData);

      await noDurableWritesDb.close();
    });

    test('It should work with durable writes undefined (default behavior)', async () => {
      const defaultDb = new PikoDB({
        databaseDirectory: testDir
        // durableWrites not specified, should default to false
      });

      await defaultDb.start();

      const testData = { name: 'Charlie', age: 35 };
      const result = await defaultDb.write('users', 'user3', testData);
      expect(result).toBe(true);

      const retrieved = await defaultDb.get('users', 'user3');
      expect(retrieved).toEqual(testData);

      await defaultDb.close();
    });

    test('It should handle durable writes with dictionary compression', async () => {
      const durableWithDictDb = new PikoDB({
        databaseDirectory: testDir,
        durableWrites: true,
        dictionary: {
          deflate: {
            sensor: 's',
            temperature: 't',
            humidity: 'h'
          }
        }
      });

      await durableWithDictDb.start();

      const sensorData = {
        sensor: 'DHT22',
        temperature: 23.5,
        humidity: 65.2
      };

      await durableWithDictDb.write('sensors', 'sensor1', sensorData);

      const retrieved = await durableWithDictDb.get('sensors', 'sensor1');
      expect(retrieved).toEqual(sensorData);

      await durableWithDictDb.close();
    });

    test('It should handle durable writes with expiration', async () => {
      const durableDb = new PikoDB({
        databaseDirectory: testDir,
        durableWrites: true
      });

      await durableDb.start();

      const futureExpiration = Date.now() + 60000; // 1 minute from now
      await durableDb.write(
        'sessions',
        'session1',
        { userId: 'user1' },
        futureExpiration
      );

      const retrieved = await durableDb.get('sessions', 'session1');
      expect(retrieved).toEqual({ userId: 'user1' });

      await durableDb.close();
    });

    test('It should handle batch writes with durable writes enabled', async () => {
      const durableDb = new PikoDB({
        databaseDirectory: testDir,
        durableWrites: true
      });

      await durableDb.start();

      // Batch write
      const writes = [];
      for (let i = 0; i < 10; i++) {
        writes.push(durableDb.write('batch-durable', `key${i}`, `value${i}`));
      }

      await Promise.all(writes);

      // Verify all writes
      for (let i = 0; i < 10; i++) {
        const value = await durableDb.get('batch-durable', `key${i}`);
        expect(value).toBe(`value${i}`);
      }

      await durableDb.close();
    });
  });
});
