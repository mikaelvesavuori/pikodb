/** biome-ignore-all lint/style/noNonNullAssertion: OK */

/**
 * @description Dictionary for compressing/decompressing object keys in database records.
 * Provide either deflate (long → short) or inflate (short → long) mapping.
 * The inverse will be auto-generated.
 *
 * Important:
 * - Compression is RECURSIVE - works at all nesting levels
 * - Arrays are preserved, objects inside arrays are compressed
 * - Keys not in dictionary are preserved unchanged
 * - Choose compressed keys that don't exist in your data to avoid collisions
 *
 * @example
 * // Provide deflate mapping (long → short)
 * const dictionary: Dictionary = {
 *   deflate: {
 *     sensor: 's',
 *     temperature: 't',
 *     humidity: 'h',
 *     timestamp: 'ts',
 *     metadata: 'm'
 *   }
 * };
 * // Inverse inflate mapping is auto-generated!
 *
 * @example
 * // Or provide inflate mapping (short → long)
 * const dictionary: Dictionary = {
 *   inflate: {
 *     s: 'sensor',
 *     t: 'temperature',
 *     h: 'humidity',
 *     ts: 'timestamp',
 *     m: 'metadata'
 *   }
 * };
 * // Inverse deflate mapping is auto-generated!
 *
 * @example
 * Works recursively on nested objects
 * With dictionary: { metadata: 'm', location: 'l' }
 * Input:  { metadata: { location: 'warehouse' } }
 * On disk: { m: { l: 'warehouse' } }
 * Output: { metadata: { location: 'warehouse' } } // Transparent!
 */
export interface Dictionary {
  /**
   * Map of long keys to short keys for compression.
   * Example: { "sensor": "s", "identity": "i" }
   */
  deflate?: Record<string, string>;

  /**
   * Map of short keys to long keys for decompression.
   * Example: { "s": "sensor", "i": "identity" }
   */
  inflate?: Record<string, string>;
}

/**
 * @description Processed dictionary with both deflate and inflate mappings.
 */
export interface ProcessedDictionary {
  deflate: Record<string, string>;
  inflate: Record<string, string>;
}

/**
 * @description Process a dictionary by generating the missing inverse mapping.
 * Throws if neither deflate nor inflate is provided, or if both are provided.
 */
export function processDictionary(dict: Dictionary): ProcessedDictionary {
  if (!dict.deflate && !dict.inflate) {
    throw new Error(
      'Dictionary must provide either deflate or inflate mapping'
    );
  }

  if (dict.deflate && dict.inflate) {
    throw new Error(
      'Dictionary should provide only one of deflate or inflate (not both). The inverse will be auto-generated.'
    );
  }

  if (dict.deflate) {
    return {
      deflate: dict.deflate,
      inflate: invertMapping(dict.deflate)
    };
  }

  // dict.inflate exists
  return {
    deflate: invertMapping(dict.inflate!),
    inflate: dict.inflate!
  };
}

/**
 * @description Invert a mapping object (swap keys and values).
 */
function invertMapping(
  mapping: Record<string, string>
): Record<string, string> {
  const inverted: Record<string, string> = {};

  for (const [key, value] of Object.entries(mapping)) inverted[value] = key;

  return inverted;
}

/**
 * @description Transform a value by mapping its keys according to the dictionary.
 * Recursively handles nested objects and arrays.
 */
export function transformValue(
  value: any,
  mapping: Record<string, string>
): any {
  if (value === null || value === undefined) return value;

  if (typeof value !== 'object') return value;

  if (Array.isArray(value))
    return value.map((item) => transformValue(item, mapping));

  const transformed: any = {};
  for (const [key, val] of Object.entries(value)) {
    const newKey = mapping[key] || key;
    transformed[newKey] = transformValue(val, mapping);
  }

  return transformed;
}
