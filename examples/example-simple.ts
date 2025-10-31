import { PikoDB } from '../src/index.js';

/**
 * Simple example showing dictionary compression for sensor data
 */

async function main() {
  // Create database with dictionary compression
  const db = new PikoDB({
    databaseDirectory: './data',
    dictionary: {
      // Provide either 'deflate' (long → short) mapping
      deflate: {
        sensor: 's',
        temperature: 't',
        humidity: 'h',
        timestamp: 'ts',
        location: 'l'
      }
      // OR provide 'inflate' (short → long) - the inverse is auto-generated!
      // inflate: {
      //   s: 'sensor',
      //   t: 'temperature',
      //   h: 'humidity',
      //   ts: 'timestamp',
      //   l: 'location'
      // }
    }
  });

  await db.start();

  // Write data using ORIGINAL keys - compression is automatic!
  await db.write('sensors', 'sensor-1', {
    sensor: 'DHT22',
    temperature: 23.5,
    humidity: 65.2,
    timestamp: Date.now(),
    location: 'warehouse-A'
  });

  await db.write('sensors', 'sensor-2', {
    sensor: 'BME280',
    temperature: 22.8,
    humidity: 62.1,
    timestamp: Date.now(),
    location: 'warehouse-B'
  });

  // Read data using ORIGINAL keys - decompression is automatic!
  const sensor1 = await db.get('sensors', 'sensor-1');
  console.log('Sensor 1:', sensor1);
  // Output: { sensor: 'DHT22', temperature: 23.5, humidity: 65.2, ... }

  // Get all sensors
  const allSensors = await db.get('sensors');
  console.log('\nAll sensors:', allSensors);

  await db.close();

  console.log(
    '\n✅ Data is compressed on disk but you work with original keys!'
  );
  console.log('\n📦 On disk, keys are stored as:');
  console.log(
    '   sensor → s, temperature → t, humidity → h, timestamp → ts, location → l'
  );
  console.log('   But you always read/write with the ORIGINAL keys!');
}

main().catch(console.error);
