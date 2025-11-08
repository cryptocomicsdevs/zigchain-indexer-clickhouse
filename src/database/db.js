// src/database/db.js
const { createClient } = require('@clickhouse/client');

const isOrchestrator = process.argv[1]?.includes('orchestrator.js');

// ============================================================================
// CLICKHOUSE CLIENT CONFIGURATION
// ============================================================================

const clickhouseClient = createClient({
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
  database: process.env.CLICKHOUSE_DATABASE || 'test_db',
  application: isOrchestrator
    ? `zig-orchestrator-${process.pid}`
    : `zig-worker-${process.pid}`,
  clickhouse_settings: {
    async_insert: 1,
    wait_for_async_insert: 0,
    async_insert_max_data_size: 10485760,
    async_insert_busy_timeout_ms: 1000,
    enable_http_compression: 1,
    max_insert_block_size: 1048576,
    max_block_size: 65536,
  },
  request_timeout: 60000,
  max_open_connections: isOrchestrator ? 2 : 5,
});

// ClickHouse query runner with retry logic
async function runClickHouseQuery(query, values = {}, retries = 3) {
  let lastError;
  
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const resultSet = await clickhouseClient.query({
        query,
        query_params: values,
        format: 'JSONEachRow',
      });
      
      const result = await resultSet.json();
      return result;
    } catch (err) {
      lastError = err;
      
      if (err.message.includes('ECONNREFUSED') || err.message.includes('ENOTFOUND')) {
        console.warn(`🌐 ClickHouse connection refused (attempt ${attempt}/${retries})`);
        await new Promise(resolve => setTimeout(resolve, 2000 * attempt));
      } else if (err.message.includes('timeout')) {
        console.warn(`⏱️ ClickHouse timeout (attempt ${attempt}/${retries})`);
        await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
      } else {
        throw err;
      }
    }
  }
  
  throw lastError;
}

// ClickHouse batch insert
async function insertClickHouse(table, columns, values, retries = 3) {
  let lastError;
  
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await clickhouseClient.insert({
        table,
        values,
        format: 'JSONEachRow',
        clickhouse_settings: {
          async_insert: 1,
          wait_for_async_insert: 0,
        }
      });
      
      return { success: true, rowCount: values.length };
    } catch (err) {
      lastError = err;
      
      if (err.message.includes('ECONNREFUSED') || err.message.includes('ENOTFOUND')) {
        console.warn(`🌐 ClickHouse connection refused (attempt ${attempt}/${retries})`);
        await new Promise(resolve => setTimeout(resolve, 2000 * attempt));
      } else if (err.message.includes('timeout')) {
        console.warn(`⏱️ ClickHouse timeout (attempt ${attempt}/${retries})`);
        await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
      } else {
        throw err;
      }
    }
  }
  
  throw lastError;
}

// ClickHouse connection test
async function testClickHouseConnection() {
  try {
    const result = await clickhouseClient.query({
      query: 'SELECT version() as version',
      format: 'JSONEachRow',
    });
    const data = await result.json();
    console.log(`✅ ClickHouse connected: ${data[0].version}`);
    return true;
  } catch (err) {
    console.error(`❌ ClickHouse connection failed: ${err.message}`);
    return false;
  }
}

// Graceful shutdown
async function closeClickHouse() {
  try {
    await clickhouseClient.close();
    console.log('✅ ClickHouse connection closed');
  } catch (err) {
    console.error(`❌ Error closing ClickHouse: ${err.message}`);
  }
}

module.exports = {
  clickhouseClient,
  runClickHouseQuery,
  insertClickHouse,
  testClickHouseConnection,
  closeClickHouse,
};
