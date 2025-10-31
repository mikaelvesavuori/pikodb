import type { Dictionary } from './Dictionary';

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
 * // With multiple named dictionaries for different data types
 * const options = {
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
 * };
 *
 * @example
 * // Or provide inflate mapping (inverse auto-generated)
 * const options = {
 *   databaseDirectory: './data',
 *   dictionaries: {
 *     myDict: {
 *       inflate: {
 *         s: 'sensor',
 *         t: 'temperature',
 *         ts: 'timestamp'
 *       }
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
   * Optional set of named dictionaries for compressing database records.
   * Each dictionary is identified by a name (key) and contains compression mappings.
   * Provide either deflate (long → short) or inflate (short → long) for each dictionary.
   * The inverse mapping will be auto-generated.
   *
   * When writing data, you can specify which dictionary to use via the dictionaryName parameter.
   * This allows different compression strategies for different types of data.
   *
   * Note: DatabaseRecord metadata (value, version, timestamp, expiration, dictionaryName)
   * is ALWAYS compressed automatically - no need to include in dictionary.
   *
   * @example
   * dictionaries: {
   *   sensors: { deflate: { sensor: 's', temperature: 't' } },
   *   users: { deflate: { username: 'u', email: 'e' } }
   * }
   */
  dictionaries?: { [key: string]: Dictionary };
  /**
   * Enable durable writes for guaranteed persistence.
   *
   * When enabled, forces writes to physical storage before returning,
   * ensuring data survives power loss or VM termination.
   *
   * Trade-offs:
   * - Maximum durability (survives power loss)
   * - No data loss on crashes
   * - 10-100x slower writes (depends on storage)
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
  dictionaryName?: string;
}
