import { existsSync, statSync } from 'node:fs';
import { rm } from 'node:fs/promises';
import { PikoDB } from '../src/index.js';

/**
 * Demo: Dictionary Compression Feature
 *
 * This demo shows how dictionary-based compression can significantly reduce
 * storage size for structured data, especially beneficial for:
 * - Large objects with many descriptive keys
 * - Small metric points where metadata overhead is significant
 */

async function demo() {
  console.log('='.repeat(70));
  console.log('PikoDB Dictionary Compression Demo');
  console.log('='.repeat(70));
  console.log();

  // Test Data 1: Large component specification with nested structures
  const componentData = {
    spec: {
      name: 'Test Component',
      repo: 'https://github.com/mikaelvesavuori/orbit-api',
      description:
        'Tonberries are small, usually no taller than three feet, although some games feature giant variants. They have green skin, round heads with small snouts, yellow eyes and a fish tail. They wear dark brown cloaks with hoods and carry an old-fashioned lantern and a small butcher knife. Tonberries usually reside within caves and attack alone, though in a handful of games they appear in small groups.',
      lifecycleStage: 'development',
      version: '1.0.0',
      kind: 'service',
      group: '5zsuQWC4',
      system: 'pVb2N137',
      domain: 'jx1UmEOT',
      dataSensitivity: 'internal',
      businessCriticality: 'high',
      deploymentModel: 'public_cloud',
      sourcingModel: 'custom'
    },
    baseline: {
      id: '87R_W78q'
    },
    contacts: [
      {
        email: 'test@example.com',
        relation: 'owner'
      }
    ],
    tags: ['test', 'example'],
    slo: [
      {
        title: 'Availability',
        description: 'Service availability',
        type: 'availability',
        target: '99.9%',
        period: 30
      }
    ],
    links: [
      {
        url: 'https://example.com',
        title: 'Example API',
        icon: 'api'
      },
      {
        url: 'https://example.com',
        title: 'Example Dashboard',
        icon: 'dashboard'
      },
      {
        url: 'https://example.com',
        title: 'Example Documentation',
        icon: 'documentation'
      },
      {
        url: 'https://example.com',
        title: 'Example Other',
        icon: 'other'
      },
      {
        url: 'https://example.com',
        title: 'Example Service',
        icon: 'service'
      },
      {
        url: 'https://example.com',
        title: 'Example Task',
        icon: 'task'
      },
      {
        url: 'https://example.com',
        title: 'Example Web',
        icon: 'web'
      },
      {
        url: 'https://example.com',
        title: 'Example (no specified icon)'
      }
    ],
    api: [
      {
        name: 'Test API',
        schemaPath:
          'https://github.com/mikaelvesavuori/orbit-api/schema/test.json'
      }
    ],
    dependencies: [
      {
        target: 'rW-gFkhxxC',
        description: 'A description here',
        criticality: 'medium'
      }
    ],
    metadata: {
      createdBy: 'test-user',
      timestamp: 1758362534,
      item: {
        a: 'asdf',
        b: false,
        c: 123,
        d: ['foo', 'bar']
      }
    }
  };

  // Test Data 2: Small metric point (timestamp, id, priority)
  const metricData = {
    id: 'gpQQCYD6w1',
    timestamp: '1761680522757',
    p: 1
  };

  console.log('📊 Test Data Overview');
  console.log('-'.repeat(70));
  console.log(
    `1. Component Spec: ${JSON.stringify(componentData).length} chars`
  );
  console.log(`2. Metric Point:   ${JSON.stringify(metricData).length} chars`);
  console.log();

  // Test 1: Component Data
  await testCompression(
    'Component Spec',
    componentData,
    'components',
    'component-1',
    {
      // Compress all the common keys (using _x prefix to avoid collisions)
      spec: '_sp',
      name: '_n',
      repo: '_rp',
      description: '_ds',
      lifecycleStage: '_ls',
      version: '_vr',
      kind: '_kd',
      group: '_gp',
      system: '_sy',
      domain: '_dm',
      dataSensitivity: '_dt',
      businessCriticality: '_bc',
      deploymentModel: '_dp',
      sourcingModel: '_sm',
      baseline: '_bl',
      id: '_id',
      contacts: '_ct',
      email: '_em',
      relation: '_rl',
      tags: '_tg',
      slo: '_sl',
      title: '_tt',
      type: '_ty',
      target: '_tr',
      period: '_pd',
      links: '_lk',
      url: '_ul',
      icon: '_ic',
      api: '_ap',
      schemaPath: '_sh',
      dependencies: '_de',
      criticality: '_cr',
      metadata: '_md',
      createdBy: '_cb',
      timestamp: '_ts',
      item: '_it'
      // Note: DatabaseRecord metadata (value, version, timestamp, expiration)
      // is automatically compressed - no need to include in dictionary!
    }
  );

  console.log();

  // Test 2: Metric Data (showing importance for small data with metadata)
  await testCompression('Metric Point', metricData, 'metrics', 'metric-1', {
    id: 'i',
    timestamp: 'ts',
    p: 'p'
    // Note: DatabaseRecord metadata is automatically compressed!
  });

  console.log();

  // Test 3: Batch metrics (realistic scenario)
  console.log('📈 Batch Metrics Test (1000 metric points)');
  console.log('-'.repeat(70));

  const batchCount = 1000;
  const batchMetrics = Array.from({ length: batchCount }, (_, i) => ({
    id: `metric-${i}`,
    timestamp: String(Date.now() + i),
    p: Math.floor(Math.random() * 5) + 1
  }));

  await testBatchCompression('Batch Metrics', batchMetrics, 'batch-metrics', {
    id: 'i',
    timestamp: 'ts',
    p: 'p'
    // Note: DatabaseRecord metadata is automatically compressed!
  });

  console.log();
  console.log('='.repeat(70));
  console.log('✅ Demo Complete!');
  console.log('='.repeat(70));
  console.log();
  console.log('Key Takeaways:');
  console.log(
    '• Dictionary compression works on ENTIRE records (data + metadata)'
  );
  console.log('• Most effective for repetitive data structures');
  console.log('• Small payloads benefit from metadata compression');
  console.log('• Transparent - read/write with original keys');
  console.log('• Auto-generates inverse mapping');
  console.log();
}

/**
 * Test compression for a single data item
 */
async function testCompression(
  testName: string,
  data: any,
  tableName: string,
  key: string,
  dictionary: Record<string, string>
) {
  console.log(`🔬 Testing: ${testName}`);
  console.log('-'.repeat(70));

  const uncompressedDir = `./demo-uncompressed-${Date.now()}`;
  const compressedDir = `./demo-compressed-${Date.now()}`;

  try {
    // Create databases
    const dbUncompressed = new PikoDB({ databaseDirectory: uncompressedDir });
    const dbCompressed = new PikoDB({
      databaseDirectory: compressedDir,
      dictionary: { deflate: dictionary }
    });

    await dbUncompressed.start();
    await dbCompressed.start();

    // Write data
    await dbUncompressed.write(tableName, key, data);
    await dbCompressed.write(tableName, key, data);

    await dbUncompressed.close();
    await dbCompressed.close();

    // Compare file sizes
    const uncompressedPath = `${uncompressedDir}/${tableName}`;
    const compressedPath = `${compressedDir}/${tableName}`;

    const uncompressedSize = statSync(uncompressedPath).size;
    const compressedSize = statSync(compressedPath).size;

    const savings = uncompressedSize - compressedSize;
    const savingsPercent = ((savings / uncompressedSize) * 100).toFixed(2);

    console.log(
      `Without compression: ${uncompressedSize.toLocaleString()} bytes`
    );
    console.log(
      `With compression:    ${compressedSize.toLocaleString()} bytes`
    );
    console.log(
      `Savings:             ${savings.toLocaleString()} bytes (${savingsPercent}%)`
    );

    // Verify data integrity
    const dbVerify = new PikoDB({
      databaseDirectory: compressedDir,
      dictionary: { deflate: dictionary }
    });
    await dbVerify.start();
    const retrieved = await dbVerify.get(tableName, key);
    await dbVerify.close();

    const isValid = JSON.stringify(retrieved) === JSON.stringify(data);
    console.log(
      `Data integrity:      ${isValid ? '✅ Verified' : '❌ Failed'}`
    );
  } finally {
    // Cleanup
    if (existsSync(uncompressedDir)) {
      await rm(uncompressedDir, { recursive: true, force: true });
    }
    if (existsSync(compressedDir)) {
      await rm(compressedDir, { recursive: true, force: true });
    }
  }
}

/**
 * Test compression for batch data (multiple items)
 */
async function testBatchCompression(
  _testName: string,
  dataArray: any[],
  tableName: string,
  dictionary: Record<string, string>
) {
  const uncompressedDir = `./demo-batch-uncompressed-${Date.now()}`;
  const compressedDir = `./demo-batch-compressed-${Date.now()}`;

  try {
    // Create databases
    const dbUncompressed = new PikoDB({ databaseDirectory: uncompressedDir });
    const dbCompressed = new PikoDB({
      databaseDirectory: compressedDir,
      dictionary: { deflate: dictionary }
    });

    await dbUncompressed.start();
    await dbCompressed.start();

    // Write batch data
    const writePromises = dataArray.map((data, index) => {
      const key = `item-${index}`;
      return Promise.all([
        dbUncompressed.write(tableName, key, data),
        dbCompressed.write(tableName, key, data)
      ]);
    });

    await Promise.all(writePromises);

    await dbUncompressed.close();
    await dbCompressed.close();

    // Compare file sizes
    const uncompressedPath = `${uncompressedDir}/${tableName}`;
    const compressedPath = `${compressedDir}/${tableName}`;

    const uncompressedSize = statSync(uncompressedPath).size;
    const compressedSize = statSync(compressedPath).size;

    const savings = uncompressedSize - compressedSize;
    const savingsPercent = ((savings / uncompressedSize) * 100).toFixed(2);

    console.log(`Items written:       ${dataArray.length.toLocaleString()}`);
    console.log(
      `Without compression: ${uncompressedSize.toLocaleString()} bytes`
    );
    console.log(
      `With compression:    ${compressedSize.toLocaleString()} bytes`
    );
    console.log(
      `Savings:             ${savings.toLocaleString()} bytes (${savingsPercent}%)`
    );
    console.log(
      `Avg per item:        ${(savings / dataArray.length).toFixed(2)} bytes saved`
    );

    // Verify random samples
    const dbVerify = new PikoDB({
      databaseDirectory: compressedDir,
      dictionary: { deflate: dictionary }
    });
    await dbVerify.start();

    const samples = 10;
    let verified = 0;
    for (let i = 0; i < samples; i++) {
      const index = Math.floor(Math.random() * dataArray.length);
      const retrieved = await dbVerify.get(tableName, `item-${index}`);
      if (JSON.stringify(retrieved) === JSON.stringify(dataArray[index])) {
        verified++;
      }
    }

    await dbVerify.close();

    console.log(
      `Data integrity:      ${verified}/${samples} samples verified ${verified === samples ? '✅' : '❌'}`
    );
  } finally {
    // Cleanup
    if (existsSync(uncompressedDir)) {
      await rm(uncompressedDir, { recursive: true, force: true });
    }
    if (existsSync(compressedDir)) {
      await rm(compressedDir, { recursive: true, force: true });
    }
  }
}

// Run the demo
demo().catch(console.error);
