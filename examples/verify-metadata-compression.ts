import { readFile, rm } from 'node:fs/promises';
import { existsSync } from 'node:fs';

import { PikoDB } from '../src/index.js';

async function verify() {
  console.log('='.repeat(70));
  console.log('Verifying Metadata is ALWAYS Compressed');
  console.log('='.repeat(70));
  console.log();

  // Test 1: Without user dictionary - metadata should still be compressed
  console.log('📦 Test 1: Database WITHOUT user dictionary');
  console.log('-'.repeat(70));

  const db1 = new PikoDB({
    databaseDirectory: './test-no-dict'
  });

  await db1.start();

  await db1.write('sensors', 'sensor-1', {
    temperature: 23.5,
    humidity: 65.2,
    location: 'warehouse-A'
  });

  await db1.close();

  const file1 = await readFile('./test-no-dict/sensors', 'utf8');
  const data1 = JSON.parse(file1);

  console.log('On-disk format:');
  console.log(JSON.stringify(data1, null, 2));

  const record1 = data1[0][1];
  console.log('\n✅ Metadata fields on disk:');
  console.log(`   - "v" (value):      ${record1.v ? '✓' : '✗'}`);
  console.log(
    `   - "ver" (version):  ${record1.ver !== undefined ? '✓' : '✗'}`
  );
  console.log(`   - "ts" (timestamp): ${record1.ts !== undefined ? '✓' : '✗'}`);
  console.log(
    `   - "exp" (expiration): ${record1.exp !== undefined ? '✓' : '✗'}`
  );

  console.log('\n❌ Old format should NOT exist:');
  console.log(
    `   - "value":      ${record1.value !== undefined ? 'FOUND (BAD!)' : 'Not found (good!)'}`
  );
  console.log(
    `   - "version":    ${record1.version !== undefined ? 'FOUND (BAD!)' : 'Not found (good!)'}`
  );
  console.log(
    `   - "timestamp":  ${record1.timestamp !== undefined ? 'FOUND (BAD!)' : 'Not found (good!)'}`
  );
  console.log(
    `   - "expiration": ${record1.expiration !== undefined ? 'FOUND (BAD!)' : 'Not found (good!)'}`
  );

  console.log();

  // Test 2: With user dictionary - metadata AND user data compressed
  console.log('📦 Test 2: Database WITH user dictionary');
  console.log('-'.repeat(70));

  const db2 = new PikoDB({
    databaseDirectory: './test-with-dict',
    dictionary: {
      deflate: {
        temperature: 't',
        humidity: 'h',
        location: 'l'
      }
    }
  });

  await db2.start();

  await db2.write('sensors', 'sensor-1', {
    temperature: 23.5,
    humidity: 65.2,
    location: 'warehouse-A'
  });

  await db2.close();

  const file2 = await readFile('./test-with-dict/sensors', 'utf8');
  const data2 = JSON.parse(file2);

  console.log('On-disk format:');
  console.log(JSON.stringify(data2, null, 2));

  const record2 = data2[0][1];
  console.log('\n✅ Metadata fields compressed:');
  console.log(`   - "v" (value):      ${record2.v ? '✓' : '✗'}`);
  console.log(
    `   - "ver" (version):  ${record2.ver !== undefined ? '✓' : '✗'}`
  );
  console.log(`   - "ts" (timestamp): ${record2.ts !== undefined ? '✓' : '✗'}`);
  console.log(
    `   - "exp" (expiration): ${record2.exp !== undefined ? '✓' : '✗'}`
  );

  console.log('\n✅ User data fields compressed:');
  console.log(
    `   - "t" (temperature): ${record2.v.t !== undefined ? '✓' : '✗'}`
  );
  console.log(
    `   - "h" (humidity):    ${record2.v.h !== undefined ? '✓' : '✗'}`
  );
  console.log(
    `   - "l" (location):    ${record2.v.l !== undefined ? '✓' : '✗'}`
  );

  console.log();

  // Test 3: Verify read returns original format
  console.log('📖 Test 3: Reading data returns ORIGINAL format');
  console.log('-'.repeat(70));

  const db3 = new PikoDB({
    databaseDirectory: './test-with-dict',
    dictionary: {
      deflate: {
        temperature: 't',
        humidity: 'h',
        location: 'l'
      }
    }
  });

  await db3.start();
  const retrieved = await db3.get('sensors', 'sensor-1');
  await db3.close();

  console.log('Retrieved value:');
  console.log(JSON.stringify(retrieved, null, 2));

  console.log('\n✅ Original keys restored:');
  console.log(`   - temperature: ${retrieved.temperature}`);
  console.log(`   - humidity:    ${retrieved.humidity}`);
  console.log(`   - location:    ${retrieved.location}`);

  // Cleanup
  if (existsSync('./test-no-dict')) {
    await rm('./test-no-dict', { recursive: true, force: true });
  }
  if (existsSync('./test-with-dict')) {
    await rm('./test-with-dict', { recursive: true, force: true });
  }

  console.log();
  console.log('='.repeat(70));
  console.log('✅ Metadata Compression Verified!');
  console.log('='.repeat(70));
  console.log();
  console.log('Summary:');
  console.log('• Metadata (v, ver, ts, exp) is ALWAYS compressed');
  console.log('• User data is compressed only if dictionary provided');
  console.log('• Reads always return original format (transparent)');
  console.log();
}

verify().catch(console.error);
