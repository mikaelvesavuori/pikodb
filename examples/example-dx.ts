import { PikoDB } from '../src/index.js';

/**
 * Example showing improved Developer Experience with @example blocks
 *
 * Now when you hover over PikoDB or its methods in your IDE,
 * you'll see helpful inline examples!
 */

async function main() {
  // Hover over "PikoDB" - you'll see examples!
  const db = new PikoDB({
    databaseDirectory: './data',
    // Hover over "dictionary" - you'll see examples of both deflate and inflate!
    dictionary: {
      deflate: {
        sensor: 's',
        temperature: 't',
        humidity: 'h'
      }
    }
  });

  // Hover over "start" - you'll see example!
  await db.start();

  // Hover over "write" - you'll see multiple examples:
  // - Simple write
  // - Write with expiration
  // - Write nested objects
  await db.write('sensors', 'sensor1', {
    sensor: 'DHT22',
    temperature: 23.5,
    humidity: 65.2
  });

  // Hover over "get" - you'll see examples:
  // - Get specific value
  // - Get all values
  // - Handling undefined/expired
  const sensor = await db.get('sensors', 'sensor1');
  console.log('Retrieved:', sensor);

  // Hover over "getTableSize" - example shown
  const size = await db.getTableSize('sensors');
  console.log('Table size:', size);

  // Hover over "listTables" - example shown
  const tables = db.listTables();
  console.log('Tables:', tables);

  // Hover over "delete" - example shown
  await db.delete('sensors', 'sensor1');

  // Hover over "close" - example shown
  await db.close();

  console.log(
    '\n✅ Great DX! Hover over any method to see examples in your IDE.'
  );
}

main().catch(console.error);
