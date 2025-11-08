// src/database/models.js - Comprehensive database schema and table creation
const { clickhouseClient } = require('./db');

/**
 * Create orchestrator management tables (ClickHouse for job management)
 */
async function createOrchestratorTables(component = 'orchestrator', startHeight = 1) {
  // Index state table for tracking progress
  await clickhouseClient.query({
    query: `
      CREATE TABLE IF NOT EXISTS index_state (
        index_name String,
        last_processed_height UInt64 DEFAULT 0,
        updated_at DateTime DEFAULT now()
      )
      ENGINE = ReplacingMergeTree(updated_at)
      ORDER BY index_name
      SETTINGS index_granularity = 8192;
    `
  });

  // Initialize index state (insert if not exists)
  try {
    await clickhouseClient.insert({
      table: 'index_state',
      values: [{
        index_name: component,
        last_processed_height: startHeight,
        updated_at: Math.floor(Date.now() / 1000)
      }],
      format: 'JSONEachRow'
    });
  } catch (e) {
    // Ignore if already exists
  }

  // Work queue table for job distribution
  await clickhouseClient.query({
    query: `
      CREATE TABLE IF NOT EXISTS work_queue (
        id UInt64,
        start_height UInt64 NOT NULL,
        end_height UInt64 NOT NULL,
        status String DEFAULT 'pending',
        created_at DateTime DEFAULT now(),
        updated_at DateTime DEFAULT now(),
        error_message String
      )
      ENGINE = ReplacingMergeTree(updated_at)
      ORDER BY (id)
      SETTINGS index_granularity = 8192;
    `
  });

  // Failed blocks table for tracking individual block failures
  await clickhouseClient.query({
    query: `
      CREATE TABLE IF NOT EXISTS failed_blocks (
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

  console.log('🛠️ Orchestrator management tables ready (ClickHouse)');
}

/**
 * Create core blockchain tables in ClickHouse
 */
async function createCoreTables() {
  // 1) Blocks table - one row per height
  await clickhouseClient.query({
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
    `,
  });

  // 2) Transactions table - one row per tx within a block
  await clickhouseClient.query({
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
    `,
  });

  // Add index for tx_hash lookups
  await clickhouseClient.query({
    query: `ALTER TABLE txs ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 4;`,
  }).catch(() => {}); // Ignore if already exists

  // 3) Tx events table - one row per event per tx
  await clickhouseClient.query({
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
    `,
  });

  // Add index for event type filtering
  await clickhouseClient.query({
    query: `ALTER TABLE tx_events ADD INDEX IF NOT EXISTS idx_type type TYPE set(100) GRANULARITY 4;`,
  }).catch(() => {}); // Ignore if already exists

  // 4) Tx event attributes JSON table
  await clickhouseClient.query({
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
    `,
  });

  // 5) Block events table - events emitted at finalize_block stage
  await clickhouseClient.query({
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
    `,
  });

  // 6) Block event attributes table
  await clickhouseClient.query({
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
    `,
  });

  console.log('🛠️ Core blockchain tables ready (ClickHouse)');
}

/**
 * Create type-specific tables for wasm and message events in ClickHouse
 */
async function createTypeSpecificTables() {
  // WASM events table
  await clickhouseClient.query({
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
    `,
  });

  // Add index for tx_hash lookups
  await clickhouseClient.query({
    query: `ALTER TABLE type_wasm ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 4;`,
  }).catch(() => {});

  // WASM event attributes table
  await clickhouseClient.query({
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
    `,
  });

  // Add index for key lookups
  await clickhouseClient.query({
    query: `ALTER TABLE type_wasm_attrs ADD INDEX IF NOT EXISTS idx_key key TYPE set(1000) GRANULARITY 4;`,
  }).catch(() => {});

  // Message events table
  await clickhouseClient.query({
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
    `,
  });

  // Add index for tx_hash lookups
  await clickhouseClient.query({
    query: `ALTER TABLE type_message ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 4;`,
  }).catch(() => {});

  // Message event attributes table
  await clickhouseClient.query({
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
    `,
  });

  // Add index for key lookups
  await clickhouseClient.query({
    query: `ALTER TABLE type_message_attrs ADD INDEX IF NOT EXISTS idx_key key TYPE set(1000) GRANULARITY 4;`,
  }).catch(() => {});

  console.log('🛠️ Type-specific tables ready (ClickHouse)');
}

/**
 * Create ClickHouse functions - not applicable (ClickHouse uses direct inserts)
 */
async function createDatabaseFunctions() {
  // Note: ClickHouse doesn't support triggers or stored procedures
  // Type-specific tables will be populated directly in the worker
  console.log('🛠️ Database functions skipped (ClickHouse uses direct inserts)');
}

/**
 * Create triggers - not applicable (ClickHouse doesn't support triggers)
 */
async function createTriggers() {
  // Note: ClickHouse doesn't support triggers
  // Type-specific tables will be populated directly in the worker
  console.log('🛠️ Database triggers skipped (ClickHouse uses direct inserts)');
}

/**
 * Initialize all database tables and schema
 */
async function initializeDatabase(component = 'orchestrator', startHeight = 1) {
  console.log('⏳ Initializing comprehensive database schema...');
  
  // ClickHouse tables for orchestrator management
  console.log('📊 Creating ClickHouse orchestrator tables...');
  await createOrchestratorTables(component, startHeight);
  
  // ClickHouse tables for blockchain data
  console.log('📊 Creating ClickHouse blockchain tables...');
  await createCoreTables();
  await createTypeSpecificTables();
  await createDatabaseFunctions();
  await createTriggers();
  
  console.log('✅ Comprehensive database schema initialized (ClickHouse)');
}

module.exports = {
  // Core schema functions
  createOrchestratorTables,
  createCoreTables,
  createTypeSpecificTables,
  createDatabaseFunctions,
  createTriggers,
  
  // Main initialization
  initializeDatabase
};
