/**
 * @description Validates table name to prevent directory traversal and other security issues.
 */
export function validateTableName(tableName: string): void {
  if (!tableName || typeof tableName !== 'string')
    throw new Error('Table name must be a non-empty string');

  if (tableName.length > 255)
    throw new Error('Table name must not exceed 255 characters');

  // Prevent directory traversal
  if (tableName.includes('/') || tableName.includes('\\'))
    throw new Error('Table name must not contain path separators');

  // Prevent parent directory references
  if (tableName.includes('..'))
    throw new Error('Table name must not contain ".."');

  // Prevent hidden files and special names
  if (tableName.startsWith('.'))
    throw new Error('Table name must not start with "."');

  // Prevent null bytes
  if (tableName.includes('\0'))
    throw new Error('Table name must not contain null bytes');

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
  if (reservedNames.includes(tableName.toUpperCase()))
    throw new Error(`Table name "${tableName}" is reserved by the filesystem`);
}

/**
 * @description Validates key to ensure it's a valid string.
 */
export function validateKey(key: string): void {
  if (key === undefined || key === null) throw new Error('Key must be defined');

  if (typeof key !== 'string') throw new Error('Key must be a string');

  if (key.length === 0) throw new Error('Key must not be empty');

  if (key.length > 1024) throw new Error('Key must not exceed 1024 characters');

  // Prevent null bytes in keys
  if (key.includes('\0')) throw new Error('Key must not contain null bytes');
}

/**
 * @descriptionValidates value to ensure it's JSON-serializable.
 */
export function validateValue(value: any): void {
  if (value === undefined)
    throw new Error('Value must not be undefined (use null instead)');

  // Check for non-serializable types
  const type = typeof value;
  if (type === 'function')
    throw new Error(
      'Value must be JSON-serializable: functions are not supported'
    );

  if (type === 'symbol')
    throw new Error(
      'Value must be JSON-serializable: symbols are not supported'
    );

  try {
    const serialized = JSON.stringify(value);

    if (serialized === undefined)
      throw new Error(
        'Value must be JSON-serializable: value cannot be serialized'
      );
  } catch (error) {
    throw new Error(
      `Value must be JSON-serializable: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}
