// scripts/init_clickhouse.js - Initialize ClickHouse database with schema
const { createClient } = require('@clickhouse/client');
require('dotenv').config();

const client = createClient({
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
  database: process.env.CLICKHOUSE_DATABASE || 'zigchain',
});

const managementTables = [
  'index_state',
  'work_queue',
  'failed_blocks'
];

const tables = [
  'blocks',
  'txs',
  'tx_events',
  'tx_event_attrs_json',
  'block_events',
  'block_event_attrs',
  'type_wasm',
  'type_wasm_attrs',
  'type_message',
  'type_message_attrs'
];

const views = [
  'mv_daily_block_stats',
  'mv_daily_tx_stats',
  'mv_event_type_stats'
];

const dictionaries = ['dict_blocks'];

async function createDatabase() {
  console.log('📦 Creating database...');
  try {
    await client.exec({
      query: `CREATE DATABASE IF NOT EXISTS ${process.env.CLICKHOUSE_DATABASE || 'zigchain'}`
    });
    console.log('✅ Database created/verified');
  } catch (err) {
    console.error('❌ Database creation failed:', err.message);
    throw err;
  }
}

async function createManagementTables() {
  console.log('\n� Creating orchestrator management tables...');
  
  // Index state table
  console.log('  → Creating index_state table...');
  await client.exec({
    query: `
      CREATE TABLE IF NOT EXISTS index_state
      (
          index_name String,
          last_processed_height UInt64 DEFAULT 0,
          updated_at DateTime DEFAULT now()
      )
      ENGINE = ReplacingMergeTree(updated_at)
      ORDER BY index_name
      SETTINGS index_granularity = 8192;
    `
  });

  // Work queue table
  console.log('  → Creating work_queue table...');
  await client.exec({
    query: `
      CREATE TABLE IF NOT EXISTS work_queue
      (
          id UInt64,
          start_height UInt64,
          end_height UInt64,
          status String DEFAULT 'pending',
          created_at DateTime DEFAULT now(),
          updated_at DateTime DEFAULT now(),
          error_message String
      )
      ENGINE = ReplacingMergeTree(updated_at)
      ORDER BY id
      SETTINGS index_granularity = 8192;
    `
  });

  // Failed blocks table
  console.log('  → Creating failed_blocks table...');
  await client.exec({
    query: `
      CREATE TABLE IF NOT EXISTS failed_blocks
      (
          id UInt64,
          block_height UInt64,
          error_type String,
          error_message String,
          retry_count Int32 DEFAULT 0,
          max_retries Int32 DEFAULT 5,
          first_failed_at DateTime DEFAULT now(),
          last_retry_at DateTime DEFAULT now(),
          next_retry_at DateTime DEFAULT now(),
          worker_id String,
          status String DEFAULT 'pending'
      )
      ENGINE = ReplacingMergeTree(last_retry_at)
      ORDER BY (block_height, id)
      SETTINGS index_granularity = 8192;
    `
  });

  console.log('✅ Management tables created successfully');
}

async function createTables() {
  console.log('\n�📊 Creating blockchain data tables...');
  
  // 1. Blocks table
  console.log('  → Creating blocks table...');
  await client.exec({
    query: `
      CREATE TABLE IF NOT EXISTS blocks
      (
          height                  UInt64,
          app_hash                String,
          txs_results_count       UInt32 DEFAULT 0,
          finalize_events_count   UInt32 DEFAULT 0,
          created_at              DateTime DEFAULT now()
      )
      ENGINE = ReplacingMergeTree(created_at)
      PARTITION BY toYYYYMM(created_at)
      ORDER BY height
      SETTINGS index_granularity = 8192;
    `
  });

  // 2. Transactions table
  console.log('  → Creating txs table...');
  await client.exec({
    query: `
      CREATE TABLE IF NOT EXISTS txs
      (
          height          UInt64,
          tx_index        UInt32,
          code            Nullable(Int32),
          gas_wanted      Nullable(UInt64),
          gas_used        Nullable(UInt64),
          data            String,
          tx_hash         String,
          log             String
      )
      ENGINE = ReplacingMergeTree()
      PARTITION BY intDiv(height, 100000)
      ORDER BY (height, tx_index)
      SETTINGS index_granularity = 8192;
    `
  });

  // Add index for txs
  try {
    await client.exec({
      query: `ALTER TABLE txs ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 4;`
    });
  } catch (e) {
    if (!e.message.includes('already exists')) console.warn('  ⚠️ Index idx_tx_hash:', e.message);
  }

  // 3. TX Events table
  console.log('  → Creating tx_events table...');
  await client.exec({
    query: `
      CREATE TABLE IF NOT EXISTS tx_events
      (
          height          UInt64,
          tx_index        UInt32,
          event_index     UInt32,
          type            String
      )
      ENGINE = ReplacingMergeTree()
      PARTITION BY intDiv(height, 100000)
      ORDER BY (height, tx_index, event_index)
      SETTINGS index_granularity = 8192;
    `
  });

  // Add index for tx_events
  try {
    await client.exec({
      query: `ALTER TABLE tx_events ADD INDEX IF NOT EXISTS idx_type type TYPE set(100) GRANULARITY 4;`
    });
  } catch (e) {
    if (!e.message.includes('already exists')) console.warn('  ⚠️ Index idx_type:', e.message);
  }

  // 4. TX Event Attributes JSON table
  console.log('  → Creating tx_event_attrs_json table...');
  await client.exec({
    query: `
      CREATE TABLE IF NOT EXISTS tx_event_attrs_json
      (
          height          UInt64,
          tx_index        UInt32,
          event_index     UInt32,
          attrs_kv        String,
          attrs_map       String,
          attr_count      UInt32,
          created_at      DateTime DEFAULT now()
      )
      ENGINE = ReplacingMergeTree(created_at)
      PARTITION BY intDiv(height, 100000)
      ORDER BY (height, tx_index, event_index)
      SETTINGS index_granularity = 8192;
    `
  });

  // 5. Block Events table
  console.log('  → Creating block_events table...');
  await client.exec({
    query: `
      CREATE TABLE IF NOT EXISTS block_events
      (
          height          UInt64,
          event_index     UInt32,
          type            String
      )
      ENGINE = ReplacingMergeTree()
      PARTITION BY intDiv(height, 100000)
      ORDER BY (height, event_index)
      SETTINGS index_granularity = 8192;
    `
  });

  // 6. Block Event Attributes table
  console.log('  → Creating block_event_attrs table...');
  await client.exec({
    query: `
      CREATE TABLE IF NOT EXISTS block_event_attrs
      (
          height          UInt64,
          event_index     UInt32,
          attr_index      UInt32,
          key             String,
          value           String,
          indexed         UInt8
      )
      ENGINE = ReplacingMergeTree()
      PARTITION BY intDiv(height, 100000)
      ORDER BY (height, event_index, attr_index)
      SETTINGS index_granularity = 8192;
    `
  });

  // 7. WASM Events table
  console.log('  → Creating type_wasm table...');
  await client.exec({
    query: `
      CREATE TABLE IF NOT EXISTS type_wasm
      (
          height          UInt64,
          tx_index        UInt32,
          event_index     UInt32,
          type            String,
          tx_hash         String,
          created_at      DateTime DEFAULT now()
      )
      ENGINE = ReplacingMergeTree(created_at)
      PARTITION BY intDiv(height, 100000)
      ORDER BY (height, tx_index, event_index)
      SETTINGS index_granularity = 8192;
    `
  });

  // Add index for type_wasm
  try {
    await client.exec({
      query: `ALTER TABLE type_wasm ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 4;`
    });
  } catch (e) {
    if (!e.message.includes('already exists')) console.warn('  ⚠️ Index idx_tx_hash:', e.message);
  }

  // 8. WASM Event Attributes table
  console.log('  → Creating type_wasm_attrs table...');
  await client.exec({
    query: `
      CREATE TABLE IF NOT EXISTS type_wasm_attrs
      (
          height          UInt64,
          tx_index        UInt32,
          event_index     UInt32,
          attr_index      UInt32,
          key             String,
          value           String,
          indexed         UInt8
      )
      ENGINE = ReplacingMergeTree()
      PARTITION BY intDiv(height, 100000)
      ORDER BY (height, tx_index, event_index, attr_index)
      SETTINGS index_granularity = 8192;
    `
  });

  // Add index for type_wasm_attrs
  try {
    await client.exec({
      query: `ALTER TABLE type_wasm_attrs ADD INDEX IF NOT EXISTS idx_key key TYPE set(1000) GRANULARITY 4;`
    });
  } catch (e) {
    if (!e.message.includes('already exists')) console.warn('  ⚠️ Index idx_key:', e.message);
  }

  // 9. Message Events table
  console.log('  → Creating type_message table...');
  await client.exec({
    query: `
      CREATE TABLE IF NOT EXISTS type_message
      (
          height          UInt64,
          tx_index        UInt32,
          event_index     UInt32,
          type            String,
          tx_hash         String,
          created_at      DateTime DEFAULT now()
      )
      ENGINE = ReplacingMergeTree(created_at)
      PARTITION BY intDiv(height, 100000)
      ORDER BY (height, tx_index, event_index)
      SETTINGS index_granularity = 8192;
    `
  });

  // Add index for type_message
  try {
    await client.exec({
      query: `ALTER TABLE type_message ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 4;`
    });
  } catch (e) {
    if (!e.message.includes('already exists')) console.warn('  ⚠️ Index idx_tx_hash:', e.message);
  }

  // 10. Message Event Attributes table
  console.log('  → Creating type_message_attrs table...');
  await client.exec({
    query: `
      CREATE TABLE IF NOT EXISTS type_message_attrs
      (
          height          UInt64,
          tx_index        UInt32,
          event_index     UInt32,
          attr_index      UInt32,
          key             String,
          value           String,
          indexed         UInt8
      )
      ENGINE = ReplacingMergeTree()
      PARTITION BY intDiv(height, 100000)
      ORDER BY (height, tx_index, event_index, attr_index)
      SETTINGS index_granularity = 8192;
    `
  });

  // Add index for type_message_attrs
  try {
    await client.exec({
      query: `ALTER TABLE type_message_attrs ADD INDEX IF NOT EXISTS idx_key key TYPE set(1000) GRANULARITY 4;`
    });
  } catch (e) {
    if (!e.message.includes('already exists')) console.warn('  ⚠️ Index idx_key:', e.message);
  }

  console.log('✅ All tables created successfully');
}

async function createMaterializedViews() {
  console.log('\n📈 Creating materialized views...');

  // View 1: Daily block statistics
  console.log('  → Creating mv_daily_block_stats...');
  await client.exec({
    query: `
      CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_block_stats
      ENGINE = SummingMergeTree()
      PARTITION BY toYYYYMM(date)
      ORDER BY (date)
      AS SELECT
          toDate(created_at) AS date,
          count() AS block_count,
          sum(txs_results_count) AS total_txs,
          sum(finalize_events_count) AS total_events
      FROM blocks
      GROUP BY date;
    `
  });

  // View 2: Transaction stats by day
  console.log('  → Creating mv_daily_tx_stats...');
  await client.exec({
    query: `
      CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_tx_stats
      ENGINE = SummingMergeTree()
      PARTITION BY toYYYYMM(date)
      ORDER BY (date)
      AS SELECT
          toDate(b.created_at) AS date,
          count() AS tx_count,
          sum(t.gas_used) AS total_gas_used,
          avg(t.gas_used) AS avg_gas_used,
          countIf(t.code != 0) AS failed_txs
      FROM txs t
      JOIN blocks b ON t.height = b.height
      GROUP BY date;
    `
  });

  // View 3: Event type distribution
  console.log('  → Creating mv_event_type_stats...');
  await client.exec({
    query: `
      CREATE MATERIALIZED VIEW IF NOT EXISTS mv_event_type_stats
      ENGINE = SummingMergeTree()
      PARTITION BY toYYYYMM(date)
      ORDER BY (date, type)
      AS SELECT
          toDate(b.created_at) AS date,
          te.type,
          count() AS event_count
      FROM tx_events te
      JOIN blocks b ON te.height = b.height
      GROUP BY date, type;
    `
  });

  console.log('✅ All materialized views created successfully');
}

async function createDictionaries() {
  console.log('\n📚 Creating dictionaries...');
  
  console.log('  → Creating dict_blocks...');
  try {
    await client.exec({
      query: `
        CREATE DICTIONARY IF NOT EXISTS dict_blocks
        (
            height UInt64,
            created_at DateTime,
            txs_results_count UInt32
        )
        PRIMARY KEY height
        SOURCE(CLICKHOUSE(TABLE 'blocks'))
        LAYOUT(HASHED())
        LIFETIME(MIN 300 MAX 600);
      `
    });
    console.log('✅ Dictionary created successfully');
  } catch (err) {
    console.warn('⚠️ Dictionary creation skipped (may require blocks table to have data):', err.message);
  }
}

async function verifyTables() {
  console.log('\n🔍 Verifying tables...');
  
  const result = await client.query({
    query: `SELECT name, engine FROM system.tables WHERE database = '${process.env.CLICKHOUSE_DATABASE || 'zigchain'}' ORDER BY name`,
    format: 'JSONEachRow',
  });
  
  const existingTables = await result.json();
  
  console.log('\n📋 Existing tables:');
  for (const table of existingTables) {
    const emoji = managementTables.includes(table.name) ? '🔧' :
                  tables.includes(table.name) ? '✅' : 
                  views.includes(table.name) ? '📊' : 
                  dictionaries.includes(table.name) ? '📚' : '❓';
    console.log(`  ${emoji} ${table.name} (${table.engine})`);
  }
  
  // Check for missing core tables
  const existingTableNames = existingTables.map(t => t.name);
  const allRequiredTables = [...managementTables, ...tables];
  const missingTables = allRequiredTables.filter(t => !existingTableNames.includes(t));
  
  if (missingTables.length > 0) {
    console.log('\n⚠️ Missing tables:');
    missingTables.forEach(t => console.log(`  ❌ ${t}`));
    return false;
  }
  
  console.log('\n✅ All required tables exist!');
  return true;
}

async function getTableStats() {
  console.log('\n📊 Table statistics:');
  
  const result = await client.query({
    query: `
      SELECT 
          table,
          formatReadableSize(sum(bytes)) AS size,
          sum(rows) AS rows,
          count() AS parts
      FROM system.parts 
      WHERE database = '${process.env.CLICKHOUSE_DATABASE || 'zigchain'}' 
        AND active = 1
      GROUP BY table
      ORDER BY sum(bytes) DESC
    `,
    format: 'JSONEachRow',
  });
  
  const stats = await result.json();
  
  if (stats.length === 0) {
    console.log('  ℹ️ No data in tables yet (newly created)');
  } else {
    console.log('\n  Table              | Size      | Rows       | Parts');
    console.log('  -------------------|-----------|------------|------');
    for (const stat of stats) {
      const tableName = stat.table.padEnd(18);
      const size = stat.size.padEnd(9);
      const rows = stat.rows.toString().padStart(10);
      const parts = stat.parts.toString().padStart(5);
      console.log(`  ${tableName} | ${size} | ${rows} | ${parts}`);
    }
  }
}

async function main() {
  console.log('🚀 ClickHouse Database Initialization Script\n');
  console.log(`📍 Connecting to: ${process.env.CLICKHOUSE_URL || 'http://localhost:8123'}`);
  console.log(`📦 Database: ${process.env.CLICKHOUSE_DATABASE || 'zigchain'}\n`);
  
  try {
    // Test connection
    console.log('🔌 Testing connection...');
    const pingResult = await client.query({
      query: 'SELECT version() as version',
      format: 'JSONEachRow',
    });
    const versionData = await pingResult.json();
    console.log(`✅ Connected! ClickHouse version: ${versionData[0].version}\n`);
    
    // Create database
    await createDatabase();
    
    // Create management tables
    await createManagementTables();
    
    // Create blockchain data tables
    await createTables();
    
    // Create materialized views
    await createMaterializedViews();
    
    // Create dictionaries (optional)
    await createDictionaries();
    
    // Verify everything
    const allTablesExist = await verifyTables();
    
    // Show stats
    await getTableStats();
    
    console.log('\n' + '='.repeat(70));
    if (allTablesExist) {
      console.log('✅ Database initialization completed successfully!');
      console.log('🚀 You can now run the indexer with: npm start');
    } else {
      console.log('⚠️ Database initialization completed with warnings');
      console.log('   Please check the missing tables above');
    }
    console.log('='.repeat(70) + '\n');
    
  } catch (err) {
    console.error('\n❌ Initialization failed:', err.message);
    console.error('Stack:', err.stack);
    process.exit(1);
  } finally {
    await client.close();
  }
}

// Run the script
main();
