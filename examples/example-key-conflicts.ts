import { PikoDB } from '../src/index.js';
import { rm } from 'node:fs/promises';
import { existsSync } from 'node:fs';

/**
 * Demonstrating that simple single-letter keys work perfectly fine
 * and conflicts only happen if YOU create them (which is your responsibility)
 */

async function main() {
  console.log('='.repeat(70));
  console.log('Dictionary Key Conflicts - No Mental Overhead Required!');
  console.log('='.repeat(70));
  console.log();

  // Test 1: Simple single-letter keys work fine
  console.log('✅ Test 1: Simple single-letter mappings (RECOMMENDED)');
  console.log('-'.repeat(70));

  const db1 = new PikoDB({
    databaseDirectory: './test-simple-keys',
    dictionary: {
      deflate: {
        sensor: 's',
        temperature: 't',
        humidity: 'h',
        timestamp: 'ts' // Can use 2-letter too
      }
    }
  });

  await db1.start();

  // This data has NO conflicts
  await db1.write('readings', 'r1', {
    sensor: 'DHT22',
    temperature: 23.5,
    humidity: 65.2,
    timestamp: Date.now()
  });

  const result1 = await db1.get('readings', 'r1');
  console.log('Original data:');
  console.log(JSON.stringify(result1, null, 2));
  console.log('\n✅ Works perfectly! No special naming patterns needed.');

  await db1.close();

  console.log();

  // Test 2: What if user data already has the compressed key?
  console.log('⚠️  Test 2: When user creates a conflict');
  console.log('-'.repeat(70));

  const db2 = new PikoDB({
    databaseDirectory: './test-conflict',
    dictionary: {
      deflate: {
        sensor: 's'
      }
    }
  });

  await db2.start();

  // User has BOTH "sensor" and "s" fields - this creates a conflict
  const conflictData = {
    sensor: 'DHT22', // Will become "s": "DHT22"
    s: 'original-s-value', // Already has "s"
    other: 'data'
  };

  await db2.write('readings', 'r1', conflictData);
  const result2 = await db2.get('readings', 'r1');

  console.log('Input data (has both "sensor" and "s"):');
  console.log(JSON.stringify(conflictData, null, 2));
  console.log('\nRetrieved data:');
  console.log(JSON.stringify(result2, null, 2));
  console.log(
    '\n⚠️  Note: "sensor" → "s" collision! This is USER error, not a framework issue.'
  );
  console.log(
    '    Solution: Don\'t use "s" in your data if "sensor" → "s" in dictionary.'
  );

  await db2.close();

  console.log();

  // Test 3: User data with single-letter keys that AREN'T in dictionary
  console.log(
    '✅ Test 3: User data with single-letter keys (not in dictionary)'
  );
  console.log('-'.repeat(70));

  const db3 = new PikoDB({
    databaseDirectory: './test-no-conflict',
    dictionary: {
      deflate: {
        sensor: 'sn', // Using "sn" instead of "s"
        temperature: 'tmp'
      }
    }
  });

  await db3.start();

  // User data happens to have "a", "b", "c" keys
  const userData = {
    sensor: 'DHT22',
    temperature: 23.5,
    metadata: {
      a: 'value-a',
      b: 'value-b',
      c: 123
    }
  };

  await db3.write('readings', 'r1', userData);
  const result3 = await db3.get('readings', 'r1');

  console.log('User data with single-letter keys (a, b, c):');
  console.log(JSON.stringify(result3, null, 2));
  console.log(
    '\n✅ "a", "b", "c" are preserved because they\'re NOT in the dictionary!'
  );
  console.log('    Only "sensor" and "temperature" were compressed.');

  await db3.close();

  console.log();

  // Cleanup
  for (const dir of [
    './test-simple-keys',
    './test-conflict',
    './test-no-conflict'
  ]) {
    if (existsSync(dir)) {
      await rm(dir, { recursive: true, force: true });
    }
  }

  console.log('='.repeat(70));
  console.log('Key Takeaways:');
  console.log('='.repeat(70));
  console.log();
  console.log(
    '✅ Use simple single-letter keys: { sensor: "s", temperature: "t" }'
  );
  console.log('✅ Keys NOT in dictionary are preserved unchanged');
  console.log('✅ No special prefixes or patterns needed');
  console.log();
  console.log('⚠️  Conflicts only occur if YOUR data has the compressed key');
  console.log('   Example: dictionary has "sensor → s" AND your data has "s"');
  console.log(
    "   Solution: Choose compressed keys that don't exist in your data"
  );
  console.log();
  console.log('💡 Best Practice: Use 1-2 letter abbreviations that make sense');
  console.log('   sensor → s, temperature → t, humidity → h, etc.');
  console.log();
}

main().catch(console.error);
