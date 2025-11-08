// src/database/clickhouse_queries.js - ClickHouse query helpers for orchestrator

const { clickhouseClient, runClickHouseQuery } = require('./db');

/**
 * Execute a SELECT query and return rows
 */
async function query(sql, params = []) {
  // Convert PostgreSQL-style $1, $2 placeholders to ClickHouse {param:Type} format
  let clickhouseSQL = sql.trim();
  
  // Handle DELETE statements - convert to ClickHouse ALTER TABLE DELETE syntax
  if (clickhouseSQL.toUpperCase().startsWith('DELETE FROM')) {
    const deleteMatch = clickhouseSQL.match(/DELETE FROM\s+(\w+)\s+WHERE\s+(.+)/i);
    if (deleteMatch) {
      const tableName = deleteMatch[1];
      let whereClause = deleteMatch[2];
      
      // Replace parameters in WHERE clause
      params.forEach((param, index) => {
        const placeholder = `$${index + 1}`;
        const value = typeof param === 'string' ? `'${param.replace(/'/g, "''")}'` : param;
        whereClause = whereClause.replace(new RegExp(`\\${placeholder}(?![0-9])`, 'g'), value);
      });
      
      clickhouseSQL = `ALTER TABLE ${tableName} DELETE WHERE ${whereClause}`;
      await clickhouseClient.exec({ query: clickhouseSQL });
      return { rows: [], rowCount: 0 };
    }
  }
  
  // Handle UPDATE statements - convert to ClickHouse ALTER TABLE UPDATE syntax
  if (clickhouseSQL.toUpperCase().startsWith('UPDATE')) {
    const updateMatch = clickhouseSQL.match(/UPDATE\s+(\w+)\s+SET\s+(.+?)\s+WHERE\s+(.+)/is);
    if (updateMatch) {
      const tableName = updateMatch[1];
      let setClause = updateMatch[2].trim();
      let whereClause = updateMatch[3].trim();
      
      // Replace parameters
      params.forEach((param, index) => {
        const placeholder = `$${index + 1}`;
        const value = typeof param === 'string' ? `'${param.replace(/'/g, "''")}'` : param;
        const regex = new RegExp(`\\${placeholder}(?![0-9])`, 'g');
        setClause = setClause.replace(regex, value);
        whereClause = whereClause.replace(regex, value);
      });
      
      // Replace NOW() with now()
      setClause = setClause.replace(/NOW\(\)/gi, 'now()');
      
      clickhouseSQL = `ALTER TABLE ${tableName} UPDATE ${setClause} WHERE ${whereClause}`;
      await clickhouseClient.exec({ query: clickhouseSQL });
      return { rows: [], rowCount: 0 };
    }
  }
  
  // For SELECT queries
  // Replace parameters
  params.forEach((param, index) => {
    const placeholder = `$${index + 1}`;
    const value = typeof param === 'string' ? `'${param.replace(/'/g, "''")}'` : param;
    clickhouseSQL = clickhouseSQL.replace(new RegExp(`\\${placeholder}(?![0-9])`, 'g'), value);
  });
  
  const result = await clickhouseClient.query({
    query: clickhouseSQL,
    format: 'JSONEachRow',
  });
  const rows = await result.json();
  return { rows, rowCount: rows.length };
}

/**
 * Execute INSERT/UPDATE/DELETE query
 */
async function exec(sql, params = []) {
  let clickhouseSQL = sql;
  
  // Convert PostgreSQL-style parameters to values
  if (params.length > 0) {
    params.forEach((param, index) => {
      const placeholder = `$${index + 1}`;
      const value = typeof param === 'string' ? `'${param.replace(/'/g, "''")}'` : param;
      clickhouseSQL = clickhouseSQL.replace(new RegExp(`\\${placeholder}(?![0-9])`, 'g'), value);
    });
  }
  
  await clickhouseClient.exec({ query: clickhouseSQL });
  return { rowCount: 1 }; // ClickHouse doesn't return affected rows easily
}

/**
 * Insert data into a table
 */
async function insert(table, data) {
  if (!Array.isArray(data)) {
    data = [data];
  }
  
  if (data.length === 0) return { rowCount: 0 };
  
  await clickhouseClient.insert({
    table,
    values: data,
    format: 'JSONEachRow',
  });
  
  return { rowCount: data.length };
}

/**
 * Get last processed height from index_state
 */
async function getLastIndexedHeight(indexName = 'decoded_indexer') {
  const result = await query(
    `SELECT last_processed_height FROM index_state WHERE index_name = '${indexName}' ORDER BY updated_at DESC LIMIT 1`
  );
  
  if (result.rows.length === 0) {
    return 0;
  }
  
  return parseInt(result.rows[0].last_processed_height, 10);
}

/**
 * Update last processed height
 */
async function updateLastIndexedHeight(indexName, height) {
  // In ClickHouse, we insert a new row (ReplacingMergeTree will handle deduplication)
  await insert('index_state', {
    index_name: indexName,
    last_processed_height: height,
    updated_at: Math.floor(Date.now() / 1000)
  });
}

/**
 * Get max height from blocks table
 */
async function getMaxBlockHeight() {
  const result = await query(`SELECT max(height) as max_h FROM blocks`);
  if (result.rows.length === 0 || result.rows[0].max_h === null) {
    return 0;
  }
  return parseInt(result.rows[0].max_h, 10);
}

/**
 * Count work queue items by status
 */
async function countWorkQueue(status) {
  const result = await query(
    `SELECT count() as count FROM work_queue FINAL WHERE status = '${status}'`
  );
  return parseInt(result.rows[0]?.count || 0, 10);
}

/**
 * Get pending work items
 */
async function getPendingWork(limit = 1) {
  const result = await query(
    `SELECT * FROM work_queue FINAL WHERE status = 'pending' ORDER BY id LIMIT ${limit}`
  );
  return result.rows;
}

/**
 * Update work queue status
 */
async function updateWorkQueueStatus(id, status, errorMessage = null) {
  const data = {
    id: parseInt(id, 10),
    status,
    updated_at: Math.floor(Date.now() / 1000)
  };
  
  if (errorMessage) {
    data.error_message = errorMessage;
  }
  
  await insert('work_queue', data);
}

/**
 * Delete work queue item
 */
async function deleteWorkQueueItem(id) {
  // ClickHouse doesn't support DELETE in the traditional sense for MergeTree engines
  // We use ALTER TABLE DELETE which is asynchronous
  await exec(`ALTER TABLE work_queue DELETE WHERE id = ${id}`);
}

/**
 * Insert work queue items
 */
async function insertWorkQueue(items) {
  if (!Array.isArray(items)) {
    items = [items];
  }
  
  const data = items.map(item => ({
    id: item.id || Date.now() + Math.random(),
    start_height: item.start_height,
    end_height: item.end_height,
    status: item.status || 'pending',
    created_at: Math.floor(Date.now() / 1000),
    updated_at: Math.floor(Date.now() / 1000),
    error_message: item.error_message || ''
  }));
  
  return await insert('work_queue', data);
}

/**
 * Get overlapping work queue ranges
 */
async function getOverlappingRanges(startHeight, endHeight) {
  const result = await query(`
    SELECT start_height as s, end_height as e
    FROM work_queue FINAL
    WHERE status IN ('pending','processing')
      AND NOT (${endHeight} < start_height OR ${startHeight} > end_height)
    ORDER BY start_height ASC
  `);
  return result.rows;
}

/**
 * Add failed block
 */
async function addFailedBlock(height, errorType, errorMessage, workerId = null) {
  const now = Math.floor(Date.now() / 1000);
  const nextRetry = now + (5 * 60); // 5 minutes from now
  
  await insert('failed_blocks', {
    id: Date.now() + Math.random(),
    block_height: height,
    error_type: errorType,
    error_message: errorMessage,
    retry_count: 1,
    max_retries: 5,
    first_failed_at: now,
    last_retry_at: now,
    next_retry_at: nextRetry,
    worker_id: workerId || `worker-${process.pid}`,
    status: 'pending'
  });
}

/**
 * Remove failed block
 */
async function removeFailedBlock(height) {
  await exec(`ALTER TABLE failed_blocks DELETE WHERE block_height = ${height}`);
}

module.exports = {
  query,
  exec,
  insert,
  getLastIndexedHeight,
  updateLastIndexedHeight,
  getMaxBlockHeight,
  countWorkQueue,
  getPendingWork,
  updateWorkQueueStatus,
  deleteWorkQueueItem,
  insertWorkQueue,
  getOverlappingRanges,
  addFailedBlock,
  removeFailedBlock,
};
