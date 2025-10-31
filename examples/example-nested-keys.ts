import { PikoDB } from '../src/index.js';
import { readFile, rm } from 'node:fs/promises';
import { existsSync } from 'node:fs';

/**
 * Demonstrating how nested keys are automatically compressed recursively
 */

async function main() {
  console.log('='.repeat(70));
  console.log('Nested Key Compression - Automatic Recursion');
  console.log('='.repeat(70));
  console.log();

  // Test 1: Simple nested object
  console.log('📦 Test 1: Simple nested object compression');
  console.log('-'.repeat(70));

  const db1 = new PikoDB({
    databaseDirectory: './test-nested',
    dictionary: {
      deflate: {
        sensor: 's',
        metadata: 'm',
        location: 'l',
        building: 'b',
        floor: 'f',
        room: 'r',
        temperature: 't',
        timestamp: 'ts'
      }
    }
  });

  await db1.start();

  const nestedData = {
    sensor: 'DHT22',
    temperature: 23.5,
    timestamp: Date.now(),
    metadata: {
      location: 'warehouse',
      building: 'A',
      floor: 2,
      room: '201'
    }
  };

  await db1.write('readings', 'r1', nestedData);

  console.log('Original data:');
  console.log(JSON.stringify(nestedData, null, 2));

  // Check on-disk format
  const diskData = JSON.parse(await readFile('./test-nested/readings', 'utf8'));
  console.log('\nOn-disk format (compressed):');
  console.log(JSON.stringify(diskData[0][1].v, null, 2));

  const retrieved = await db1.get('readings', 'r1');
  console.log('\nRetrieved data (decompressed):');
  console.log(JSON.stringify(retrieved, null, 2));

  console.log('\n✅ Nested keys are automatically compressed at ALL levels!');
  console.log('   - Top level: sensor → s, metadata → m, temperature → t');
  console.log('   - Nested: location → l, building → b, floor → f, room → r');

  await db1.close();

  console.log();

  // Test 2: Deeply nested object
  console.log('📦 Test 2: Deeply nested object (3+ levels)');
  console.log('-'.repeat(70));

  const db2 = new PikoDB({
    databaseDirectory: './test-deep-nested',
    dictionary: {
      deflate: {
        organization: 'o',
        department: 'd',
        team: 't',
        member: 'm',
        name: 'n',
        role: 'r',
        email: 'e'
      }
    }
  });

  await db2.start();

  const deeplyNested = {
    organization: {
      department: {
        team: {
          member: {
            name: 'Alice',
            role: 'Engineer',
            email: 'alice@example.com'
          }
        }
      }
    }
  };

  await db2.write('org', 'o1', deeplyNested);

  console.log('Original deeply nested data:');
  console.log(JSON.stringify(deeplyNested, null, 2));

  const diskData2 = JSON.parse(
    await readFile('./test-deep-nested/org', 'utf8')
  );
  console.log('\nOn-disk format (all levels compressed):');
  console.log(JSON.stringify(diskData2[0][1].v, null, 2));

  const retrieved2 = await db2.get('org', 'o1');
  console.log('\nRetrieved (fully restored):');
  console.log(JSON.stringify(retrieved2, null, 2));

  console.log('\n✅ Works at ANY depth! All levels compressed recursively.');

  await db2.close();

  console.log();

  // Test 3: Arrays of objects
  console.log('📦 Test 3: Arrays of objects');
  console.log('-'.repeat(70));

  const db3 = new PikoDB({
    databaseDirectory: './test-arrays',
    dictionary: {
      deflate: {
        readings: 'r',
        temperature: 't',
        humidity: 'h',
        timestamp: 'ts'
      }
    }
  });

  await db3.start();

  const arrayData = {
    readings: [
      { temperature: 23.5, humidity: 65.2, timestamp: 1000 },
      { temperature: 24.1, humidity: 63.8, timestamp: 2000 },
      { temperature: 23.9, humidity: 64.5, timestamp: 3000 }
    ]
  };

  await db3.write('sensor', 's1', arrayData);

  console.log('Original data (array of objects):');
  console.log(JSON.stringify(arrayData, null, 2));

  const diskData3 = JSON.parse(await readFile('./test-arrays/sensor', 'utf8'));
  console.log('\nOn-disk format (objects inside array compressed):');
  console.log(JSON.stringify(diskData3[0][1].v, null, 2));

  const retrieved3 = await db3.get('sensor', 's1');
  console.log('\nRetrieved (fully restored):');
  console.log(JSON.stringify(retrieved3, null, 2));

  console.log(
    '\n✅ Arrays are preserved, objects INSIDE arrays are compressed!'
  );

  await db3.close();

  console.log();

  // Test 4: Mixed - some keys at multiple levels
  console.log('📦 Test 4: Same key name at different levels');
  console.log('-'.repeat(70));

  const db4 = new PikoDB({
    databaseDirectory: './test-same-key',
    dictionary: {
      deflate: {
        name: 'n',
        metadata: 'm'
      }
    }
  });

  await db4.start();

  const sameKeyData = {
    name: 'Top Level Name',
    metadata: {
      name: 'Nested Name', // Same key "name" at different level
      other: 'data'
    }
  };

  await db4.write('test', 't1', sameKeyData);

  console.log('Original (same key at different levels):');
  console.log(JSON.stringify(sameKeyData, null, 2));

  const diskData4 = JSON.parse(await readFile('./test-same-key/test', 'utf8'));
  console.log('\nOn-disk format (both "name" keys compressed to "n"):');
  console.log(JSON.stringify(diskData4[0][1].v, null, 2));

  const retrieved4 = await db4.get('test', 't1');
  console.log('\nRetrieved (both restored):');
  console.log(JSON.stringify(retrieved4, null, 2));

  console.log('\n✅ Same key at different levels = compressed the same way!');
  console.log('   This is a feature: consistent compression everywhere.');

  await db4.close();

  console.log();

  // Cleanup
  for (const dir of [
    './test-nested',
    './test-deep-nested',
    './test-arrays',
    './test-same-key'
  ]) {
    if (existsSync(dir)) {
      await rm(dir, { recursive: true, force: true });
    }
  }

  console.log('='.repeat(70));
  console.log('Key Takeaways: Nested Compression');
  console.log('='.repeat(70));
  console.log();
  console.log('✅ Compression is RECURSIVE - works at all nesting levels');
  console.log('✅ Arrays are preserved, objects inside arrays are compressed');
  console.log('✅ Same key at different levels = compressed consistently');
  console.log('✅ No need to specify paths - just list the keys to compress');
  console.log();
  console.log('💡 Simple Dictionary Example:');
  console.log('   {');
  console.log('     sensor: "s",');
  console.log('     temperature: "t",');
  console.log('     metadata: "m"');
  console.log('   }');
  console.log();
  console.log(
    '   This compresses ALL occurrences of these keys, regardless of depth!'
  );
  console.log();
}

main().catch(console.error);
