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
}
