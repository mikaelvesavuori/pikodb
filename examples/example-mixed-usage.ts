import { PikoDB } from '../src/index.js';

/**
 * Example showing mixed usage: compression optional, per database instance
 */

async function main() {
  console.log('='.repeat(70));
  console.log('Mixed Usage Example: Optional Dictionary Compression');
  console.log('='.repeat(70));
  console.log();

  // Database 1: NO compression (default behavior)
  console.log('📦 Database 1: Without compression');
  const db1 = new PikoDB({
    databaseDirectory: './data-uncompressed'
  });

  await db1.start();

  await db1.write('users', 'user-1', {
    name: 'Alice',
    email: 'alice@example.com',
    role: 'admin'
  });

  const user1 = await db1.get('users', 'user-1');
  console.log('   Retrieved:', user1);

  await db1.close();

  // Database 2: WITH compression
  console.log('\n📦 Database 2: With compression');
  const db2 = new PikoDB({
    databaseDirectory: './data-compressed',
    dictionary: {
      deflate: {
        name: 'n',
        email: 'e',
        role: 'r'
      }
    }
  });

  await db2.start();

  await db2.write('users', 'user-1', {
    name: 'Bob',
    email: 'bob@example.com',
    role: 'user'
  });

  const user2 = await db2.get('users', 'user-1');
  console.log('   Retrieved:', user2);

  await db2.close();

  console.log();
  console.log('✅ Both modes work perfectly!');
  console.log();
  console.log('Use cases:');
  console.log('• No compression: Fast writes, development, small datasets');
  console.log(
    '• With compression: Storage savings, read-heavy, metrics/telemetry'
  );
  console.log();
  console.log('💡 You can choose per database instance based on your needs!');
}

main().catch(console.error);
