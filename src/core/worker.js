// worker.js - Comprehensive blockchain data extraction and storage
const axios = require('axios');
const crypto = require('crypto');
const { insertClickHouse, clickhouseClient, runClickHouseQuery } = require('../database/db');

// RPC endpoints from environment variable
const RPC_NODES = process.env.RPC_ENDPOINTS 
  ? process.env.RPC_ENDPOINTS.split(',').map(url => url.trim())
  : [
      'http://144.91.113.217:26657', // Primary node
      'http://5.189.171.155:26657',
      'http://5.189.162.146:26657',
      'http://167.86.79.37:26657',
      'http://82.208.20.12:26657'
    ];

let rpcIndex = 0;

console.log(`🔧 Worker using ${RPC_NODES.length} RPC endpoints`);

function getRPC() {
  return RPC_NODES[rpcIndex % RPC_NODES.length];
}
function rotateRPC() {
  rpcIndex = (rpcIndex + 1) % RPC_NODES.length;
  console.warn(`⚠️ Switching RPC to ${getRPC()}`);
}

async function rpcGet(path) {
  let attempts = 0;
  while (attempts < RPC_NODES.length) {
    try {
      const url = `${getRPC()}${path}`;
      const res = await axios.get(url, { timeout: 15000 });
      if (res.status === 200 && res.data) return res.data;
      throw new Error(`Invalid response status: ${res.status}`);
    } catch (err) {
      console.error(`RPC ${getRPC()} failed for path ${path}: ${err.message}`);
      rotateRPC();
      attempts++;
      await new Promise(r => setTimeout(r, 500));
    }
  }
  throw new Error(`All RPC endpoints failed for path: ${path}`);
}

// --------------------- Data Fetching ---------------------
async function fetchBlock(height) {
  return rpcGet(`/block?height=${height}`);
}
async function fetchBlockResults(height) {
  return rpcGet(`/block_results?height=${height}`);
}

// --------------------- Args ---------------------
const [, , startArg, endArg, workQueueIdArg] = process.argv;
const START = parseInt(startArg || '1300000', 10);
const END = parseInt(endArg || (START + 99), 10);
const WORK_QUEUE_ID = workQueueIdArg ? parseInt(workQueueIdArg, 10) : null;

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '5', 10); // Blocks per scheduling batch
const WORKER_CONCURRENCY = parseInt(process.env.WORKER_CONCURRENCY || '1', 10); // Parallel blocks per worker

// Performance tracking
let processedBlocks = 0;
let totalTransactions = 0;
let emptyBlocks = 0;
const startTime = Date.now();

// --------------------- Database Storage Functions (ClickHouse) ---------------------

async function storeBlockData(height, blockResponse, blockResultsResponse) {
  const block = blockResponse.result.block;
  const blockResults = blockResultsResponse.result;
  const blockTime = Math.floor(new Date(block.header.time).getTime() / 1000); // Unix timestamp
  
  // Extract block metadata
  const appHash = block.header.app_hash || '';
  const txsResultsCount = blockResults.txs_results ? blockResults.txs_results.length : 0;
  const finalizeEventsCount = blockResults.finalize_block_events ? blockResults.finalize_block_events.length : 0;

  // 1. Insert block data
  await insertClickHouse('blocks', ['height', 'app_hash', 'txs_results_count', 'finalize_events_count', 'created_at'], [{
    height: height,
    app_hash: appHash,
    txs_results_count: txsResultsCount,
    finalize_events_count: finalizeEventsCount,
    created_at: blockTime
  }]);

  // 2. Process transactions
  const rawTxs = block.data.txs || [];
  const txResults = blockResults.txs_results || [];
  
  if (rawTxs.length > 0) {
    try {
      await processTransactions(height, rawTxs, txResults);
      totalTransactions += rawTxs.length;
    } catch (txErr) {
      console.error(`🔥 Block ${height} transaction processing failed: ${txErr.message}`);
      throw new Error(`Transaction processing failed for block ${height}: ${txErr.message}`);
    }
  } else {
    emptyBlocks++;
    // Log empty blocks less frequently to reduce noise
    if (emptyBlocks % 100 === 0) {
      console.log(`⏭️ Processed ${emptyBlocks} empty blocks (no transactions) - latest: ${height}`);
    }
  }

  // 3. Process finalize block events
  if (blockResults.finalize_block_events && blockResults.finalize_block_events.length > 0) {
    try {
      await processBlockEvents(height, blockResults.finalize_block_events);
    } catch (eventErr) {
      console.error(`🔥 Block ${height} event processing failed: ${eventErr.message}`);
      throw new Error(`Event processing failed for block ${height}: ${eventErr.message}`);
    }
  }

  processedBlocks++;
}

async function processTransactions(height, rawTxs, txResults) {
  // Collect all data for batch insert
  const txsData = [];
  const eventsData = [];
  const attrsData = [];
  const wasmData = [];
  const wasmAttrsData = [];
  const messageData = [];
  const messageAttrsData = [];

  for (let txIndex = 0; txIndex < rawTxs.length; txIndex++) {
    const rawTx = rawTxs[txIndex];
    const txResult = txResults[txIndex] || {};
    const txHash = crypto.createHash('sha256').update(Buffer.from(rawTx, 'base64')).digest('hex').toUpperCase();
    const code = txResult.code || 0;
    const gasWanted = txResult.gas_wanted ? parseInt(txResult.gas_wanted) : null;
    const gasUsed = txResult.gas_used ? parseInt(txResult.gas_used) : null;
    const data = txResult.data ? Buffer.from(txResult.data, 'base64').toString('hex') : '';
    const log = txResult.log || '';

    // Add transaction data
    txsData.push({
      height: height,
      tx_index: txIndex,
      code: code,
      gas_wanted: gasWanted,
      gas_used: gasUsed,
      data: data,
      tx_hash: txHash,
      log: log
    });

    // Process events for this transaction
    const events = txResult.events || [];
    for (let eventIndex = 0; eventIndex < events.length; eventIndex++) {
      const event = events[eventIndex];
      const eventType = event.type || '';

      // Add event metadata
      eventsData.push({
        height: height,
        tx_index: txIndex,
        event_index: eventIndex,
        type: eventType
      });

      // Process event attributes
      const attributes = event.attributes || [];
      if (attributes.length > 0) {
        const attrsKv = [];
        const attrsMap = {};
        
        attributes.forEach((attr, attrIndex) => {
          const key = attr.key || '';
          const value = attr.value || '';
          const indexed = attr.index || false;
          
          attrsKv.push({ attr_index: attrIndex, key, value, indexed });
          if (!attrsMap[key]) attrsMap[key] = [];
          attrsMap[key].push(value);

          // Type-specific tables: wasm and message
          if (eventType === 'wasm') {
            wasmAttrsData.push({
              height: height,
              tx_index: txIndex,
              event_index: eventIndex,
              attr_index: attrIndex,
              key: key,
              value: value,
              indexed: indexed ? 1 : 0
            });
          } else if (eventType === 'message') {
            messageAttrsData.push({
              height: height,
              tx_index: txIndex,
              event_index: eventIndex,
              attr_index: attrIndex,
              key: key,
              value: value,
              indexed: indexed ? 1 : 0
            });
          }
        });

        // Add JSON attributes
        attrsData.push({
          height: height,
          tx_index: txIndex,
          event_index: eventIndex,
          attrs_kv: JSON.stringify(attrsKv),
          attrs_map: JSON.stringify(attrsMap),
          attr_count: attributes.length,
          created_at: Math.floor(Date.now() / 1000)
        });

        // Type-specific parent rows
        if (eventType === 'wasm') {
          wasmData.push({
            height: height,
            tx_index: txIndex,
            event_index: eventIndex,
            type: eventType,
            tx_hash: txHash,
            created_at: Math.floor(Date.now() / 1000)
          });
        } else if (eventType === 'message') {
          messageData.push({
            height: height,
            tx_index: txIndex,
            event_index: eventIndex,
            type: eventType,
            tx_hash: txHash,
            created_at: Math.floor(Date.now() / 1000)
          });
        }
      }
    }
  }

  // Batch insert all data
  if (txsData.length > 0) {
    await insertClickHouse('txs', Object.keys(txsData[0]), txsData);
  }
  
  if (eventsData.length > 0) {
    await insertClickHouse('tx_events', Object.keys(eventsData[0]), eventsData);
  }
  
  if (attrsData.length > 0) {
    await insertClickHouse('tx_event_attrs_json', Object.keys(attrsData[0]), attrsData);
  }
  
  if (wasmData.length > 0) {
    await insertClickHouse('type_wasm', Object.keys(wasmData[0]), wasmData);
  }
  
  if (wasmAttrsData.length > 0) {
    await insertClickHouse('type_wasm_attrs', Object.keys(wasmAttrsData[0]), wasmAttrsData);
  }
  
  if (messageData.length > 0) {
    await insertClickHouse('type_message', Object.keys(messageData[0]), messageData);
  }
  
  if (messageAttrsData.length > 0) {
    await insertClickHouse('type_message_attrs', Object.keys(messageAttrsData[0]), messageAttrsData);
  }
}

// No longer used: metadata is bulk-inserted and attributes are batched per block
async function processTransactionEvents() { /* intentionally no-op (kept for compatibility) */ }

// No longer used: handled in bulk above
async function processEventAttributes() { /* no-op; handled in bulk above */ }

async function processBlockEvents(height, blockEvents) {
  const blockEventsData = [];
  const blockEventAttrsData = [];

  for (let eventIndex = 0; eventIndex < blockEvents.length; eventIndex++) {
    const event = blockEvents[eventIndex];
    
    // Add block event
    blockEventsData.push({
      height: height,
      event_index: eventIndex,
      type: event.type || ''
    });

    // Process block event attributes
    if (event.attributes && event.attributes.length > 0) {
      for (let attrIndex = 0; attrIndex < event.attributes.length; attrIndex++) {
        const attr = event.attributes[attrIndex];
        
        blockEventAttrsData.push({
          height: height,
          event_index: eventIndex,
          attr_index: attrIndex,
          key: attr.key || '',
          value: attr.value || '',
          indexed: attr.index ? 1 : 0
        });
      }
    }
  }

  // Batch insert
  if (blockEventsData.length > 0) {
    await insertClickHouse('block_events', Object.keys(blockEventsData[0]), blockEventsData);
  }
  
  if (blockEventAttrsData.length > 0) {
    await insertClickHouse('block_event_attrs', Object.keys(blockEventAttrsData[0]), blockEventAttrsData);
  }
}

// Force connection cleanup
async function forceConnectionCleanup() {
  try {
    console.log('🧹 Forcing worker connection cleanup...');
    // ClickHouse client doesn't use connection pools like PostgreSQL
    // Just ensure client is still responsive
    await clickhouseClient.ping();
    console.log('✅ ClickHouse client is responsive');
  } catch (err) {
    console.error(`❌ Worker cleanup error: ${err.message}`);
  }
}

// Add failed block to tracking table
async function addFailedBlock(height, errorType, errorMessage, workerId = null) {
  try {
    // Calculate next retry time with exponential backoff
    const baseDelay = 5 * 60 * 1000; // 5 minutes base delay
    const nextRetryAt = new Date(Date.now() + baseDelay);
    
    // First, check if block already exists to calculate retry count
    const existingQuery = `
      SELECT retry_count FROM failed_blocks WHERE block_height = {height:UInt64} LIMIT 1
    `;
    const existingResult = await runClickHouseQuery(existingQuery, { height });
    
    let retryCount = 0;
    if (existingResult.length > 0) {
      retryCount = existingResult[0].retry_count + 1;
    }
    
    // Calculate next retry with exponential backoff
    const retryMultiplier = Math.pow(2, Math.min(retryCount, 5));
    const calculatedNextRetry = new Date(Date.now() + baseDelay * retryMultiplier);
    
    // ClickHouse doesn't support ON CONFLICT - ReplacingMergeTree handles deduplication
    const status = retryCount >= 10 ? 'failed_permanently' : 'pending'; // max_retries = 10
    
    await runClickHouseQuery(`
      INSERT INTO failed_blocks (
        block_height, error_type, error_message, worker_id, next_retry_at, retry_count, last_retry_at, status
      ) VALUES (
        {height:UInt64}, {errorType:String}, {errorMessage:String}, 
        {workerId:String}, {nextRetryAt:DateTime}, {retryCount:UInt32}, now(), {status:String}
      )
    `, { 
      height, 
      errorType, 
      errorMessage, 
      workerId: workerId || `worker-${process.pid}`, 
      nextRetryAt: calculatedNextRetry,
      retryCount,
      status
    });
    
    console.log(`📝 Added block ${height} to failed_blocks table (${errorType})`);
  } catch (err) {
    console.error(`❌ Failed to add block ${height} to failed_blocks table: ${err.message}`);
  }
}

// Remove successfully processed block from failed_blocks table
async function removeFailedBlock(height) {
  try {
    // ClickHouse doesn't support RETURNING - first check if exists, then delete
    const checkQuery = `SELECT id FROM failed_blocks WHERE block_height = {height:UInt64} LIMIT 1`;
    const existingRows = await runClickHouseQuery(checkQuery, { height });
    
    if (existingRows.length > 0) {
      await runClickHouseQuery(`
        DELETE FROM failed_blocks WHERE block_height = {height:UInt64}
      `, { height });
      console.log(`✅ Removed block ${height} from failed_blocks table (successfully processed)`);
    }
  } catch (err) {
    console.error(`❌ Failed to remove block ${height} from failed_blocks table: ${err.message}`);
  }
}

// --------------------- Main Processing ---------------------
async function processBlock(height) {
  try {
    // Fetch block header/data and results concurrently to hide network latency
    const [blockResponse, blockResultsResponse] = await Promise.all([
      fetchBlock(height),
      fetchBlockResults(height)
    ]);
    
    // ClickHouse doesn't need transactions - uses async inserts
    await storeBlockData(height, blockResponse, blockResultsResponse);
    
    console.log(`✅ [${height}] Block processed successfully`);
    await removeFailedBlock(height); // Remove from failed blocks table if present
    return true; // Success
  } catch (blockErr) {
    // Distinguish between different types of errors
    if (blockErr.message.includes('ECONNREFUSED') || blockErr.message.includes('timeout')) {
      console.error(`🔌 Block ${height} failed - ClickHouse connection: ${blockErr.message}`);
      await addFailedBlock(height, 'clickhouse_connection', blockErr.message);
      return false; // Retry later, don't fail the worker
    } else if (blockErr.message.includes('All RPC endpoints failed')) {
      console.error(`🌐 Block ${height} failed - RPC connectivity: ${blockErr.message}`);
      await addFailedBlock(height, 'rpc_connectivity', blockErr.message);
      return false; // Retry later
    } else if (blockErr.message.includes('no transactions')) {
      console.log(`⏭️ Block ${height} - Empty block (no transactions to process)`);
      return true; // Success for empty blocks
    } else {
      console.error(`🔥 Block ${height} failed - General error: ${blockErr.message}`);
      await addFailedBlock(height, 'general_error', blockErr.message);
      return false; // Retry later
    }
  }
}

// Simple concurrency controller
async function mapWithConcurrency(items, limit, iteratorFn) {
  const results = new Array(items.length);
  let i = 0;
  const workers = new Array(Math.min(limit, items.length)).fill(0).map(async () => {
    while (true) {
      const idx = i++;
      if (idx >= items.length) break;
      try {
        results[idx] = await iteratorFn(items[idx], idx);
      } catch (e) {
        results[idx] = { error: e };
      }
    }
  });
  await Promise.all(workers);
  return results;
}

// --------------------- Main ---------------------
(async () => {
  console.log(`🚀 Worker running: ${START} → ${END} (work_queue id: ${WORK_QUEUE_ID})`);

  let successfulBlocks = 0;
  let failedBlocks = 0;
  let retryBlocks = [];

  try {
    // Main processing loop - SEQUENTIAL processing to prevent connection exhaustion
    const totalBlocks = END - START + 1;
    let batchCount = 0;
    
    for (let i = START; i <= END; i += BATCH_SIZE) {
      const batchHeights = [];
      for (let h = i; h < i + BATCH_SIZE && h <= END; h++) batchHeights.push(h);
      
      const batchStartTime = Date.now();
      const batchStartTx = totalTransactions;
      
      // Parallel processing with bounded concurrency per worker
      const results = await mapWithConcurrency(batchHeights, WORKER_CONCURRENCY, async (height) => {
        return processBlock(height);
      });
      for (let ri = 0; ri < results.length; ri++) {
        const res = results[ri];
        const height = batchHeights[ri];
        if (res === true) {
          successfulBlocks++;
        } else if (res === false || (res && res.error)) {
          failedBlocks++;
          retryBlocks.push(height);
        }
      }
      // If we observed a spike in failures, briefly back off and clean pool
      if (failedBlocks > 0 && failedBlocks % 5 === 0) {
        await forceConnectionCleanup();
        await new Promise(resolve => setTimeout(resolve, 2000));
      }

      batchCount++;
      const batchTime = Date.now() - batchStartTime;
      const batchTxs = totalTransactions - batchStartTx;
      const progress = ((successfulBlocks / totalBlocks) * 100).toFixed(1);
      const blocksPerSec = (batchHeights.length / (batchTime / 1000)).toFixed(2);
       
      console.log(`📊 Batch ${batchCount} complete: ${batchHeights[0]}-${batchHeights[batchHeights.length-1]} | Progress: ${progress}% | ${blocksPerSec} blocks/sec | +${batchTxs} txs | Failed: ${failedBlocks}`);
    }

    // Retry failed blocks with exponential backoff
    if (retryBlocks.length > 0) {
      console.log(`🔄 Retrying ${retryBlocks.length} failed blocks...`);
      
      for (let attempt = 1; attempt <= 3; attempt++) {
        const remainingBlocks = [...retryBlocks];
        retryBlocks = [];
        
        console.log(`🔄 Retry attempt ${attempt}/3 for ${remainingBlocks.length} blocks`);
        
        for (const height of remainingBlocks) {
          // Wait between retries to prevent overwhelming the database
          await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
          
          const success = await processBlock(height);
          if (success) {
            successfulBlocks++;
            failedBlocks--;
          } else {
            retryBlocks.push(height);
          }
        }
        
        if (retryBlocks.length === 0) break;
        
        // Exponential backoff between retry attempts
        if (attempt < 3) {
          const delay = Math.pow(2, attempt) * 5000; // 10s, 20s
          console.log(`⏳ Waiting ${delay/1000}s before next retry attempt...`);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }

    // Final performance summary
    const totalTime = Date.now() - startTime;
    const totalProcessed = successfulBlocks + emptyBlocks;
    const avgBlocksPerSec = (totalProcessed / (totalTime / 1000)).toFixed(2);
    const avgTxsPerSec = (totalTransactions / (totalTime / 1000)).toFixed(2);
    
    console.log(`🎉 WORKER COMPLETED: ${START} → ${END}`);
    console.log(`📈 PERFORMANCE SUMMARY:`);
    console.log(`   • Successful blocks: ${successfulBlocks}`);
    console.log(`   • Failed blocks: ${retryBlocks.length}`);
    console.log(`   • Empty blocks: ${emptyBlocks}`);
    console.log(`   • Total transactions: ${totalTransactions}`);
    console.log(`   • Processing time: ${(totalTime / 1000 / 60).toFixed(2)} minutes`);
    console.log(`   • Average speed: ${avgBlocksPerSec} blocks/sec, ${avgTxsPerSec} txs/sec`);
    
    // Exit with appropriate code
    if (retryBlocks.length > 0) {
      console.error(`❌ Worker completed with ${retryBlocks.length} failed blocks: ${retryBlocks.join(', ')}`);
      process.exit(1); // Mark as failed so orchestrator can retry
    } else {
      console.log(`✅ Worker completed successfully - all blocks processed`);
      process.exit(0); // Success
    }

  } catch (err) {
    console.error(`💥 Worker crashed: ${err.message}`);
    await forceConnectionCleanup();
    process.exit(1);
  }
})();

// Heartbeat to update work_queue.updated_at periodically for coordination
let heartbeatTimer = null;
const HEARTBEAT_INTERVAL_MS = parseInt(process.env.HEARTBEAT_INTERVAL_MS || '20000', 10);
function startHeartbeat() {
  // ClickHouse ReplacingMergeTree doesn't support updating the version column (updated_at)
  // Heartbeat is not needed for ClickHouse - work_queue items are tracked by the orchestrator
  // No-op for ClickHouse implementation
}
function stopHeartbeat() {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
}

(async () => {
  console.log(`🚀 Worker running: ${START} → ${END} (work_queue id: ${WORK_QUEUE_ID})`);
  startHeartbeat();
  // ... rest of the code remains the same ...
})();

async function shutdown() {
  console.log('🛑 Worker shutting down, closing connections...');
  try {
    stopHeartbeat();
    const { closeClickHouse } = require('../database/db');
    await closeClickHouse();
    console.log('✅ Worker connections closed');
  } catch (err) {
    console.error('❌ Worker error closing connections:', err.message);
  }
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
process.on('beforeExit', shutdown);
