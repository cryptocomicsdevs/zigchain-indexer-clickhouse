// orchestrator.js
const { spawn } = require('child_process');
const axios = require('axios');
const { testClickHouseConnection } = require('../database/db');
const { initializeDatabase } = require('../database/models');
const ch = require('../database/clickhouse_queries');

// Create a PostgreSQL-compatible wrapper for ClickHouse
const targetDB = {
  query: ch.query,
  exec: ch.exec
};

// Mock reapIdleConnections for ClickHouse (not needed)
const reapIdleConnections = async () => {
  // No-op for ClickHouse
};

// Configuration from environment variables
const NUM_WORKERS = parseInt(process.env.NUM_WORKERS || '5', 10); 
const COMPONENT = 'decoded_indexer';
const START_HEIGHT = parseInt(process.env.START_HEIGHT || '1', 10);
const MAX_QUEUE_ITEMS = parseInt(process.env.MAX_QUEUE_ITEMS || '50'); // Increased queue limit
// Optional cap: stop syncing beyond this height if provided
const TARGET_MAX_HEIGHT = (() => {
  const v = parseInt(process.env.TARGET_MAX_HEIGHT || '', 10);
  return Number.isFinite(v) && v > 0 ? v : null;
})();
// Forward-priority backfill controls
const BACKFILL_MODE = (process.env.BACKFILL_MODE || 'false').toLowerCase() === 'true';
const BACKFILL_WINDOW = parseInt(process.env.BACKFILL_WINDOW || '50000', 10);

// RPC endpoints from environment variable
const RPC_LIST = process.env.RPC_ENDPOINTS 
  ? process.env.RPC_ENDPOINTS.split(',').map(url => url.trim())
  : [
      'http://5.189.171.155:26657',
      'http://5.189.162.146:26657', 
      'http://167.86.79.37:26657',
      'http://82.208.20.12:26657'
    ];

console.log(`🔧 Configuration: ${NUM_WORKERS} workers, ${RPC_LIST.length} RPC endpoints`);

// --------------------- RPC Utils ---------------------
async function fetchWithFallback(path) {
  for (const rpc of RPC_LIST) {
    try {
      const res = await axios.get(`${rpc}${path}`, { timeout: 15000 });
      if (res.status === 200 && res.data) return res.data;
    } catch (e) {
      console.warn(`⚠️ RPC ${rpc} failed for ${path}, trying next...`);
    }
  }
  throw new Error('All RPC endpoints failed');
}

async function getLatestBlockHeight() {
  const res = await fetchWithFallback('/status');
  // Tendermint /status latest height path
  const h = res?.result?.sync_info?.latest_block_height;
  const height = parseInt(h, 10);
  if (!Number.isFinite(height)) throw new Error('Invalid latest_block_height from RPC');
  return height;
}

// Resolve sync target: either explicit cap or live RPC tip
async function getSyncTargetHeight() {
  if (TARGET_MAX_HEIGHT) return TARGET_MAX_HEIGHT;
  return await getLatestBlockHeight();
}

// Global worker tracking
const activeWorkers = new Map(); // key: `${workId}#${partIdx}` -> { process, startTime, workItem, partIdx, lastActivity, pid }
const activeGroups = new Map();  // workId -> { remaining: number, failed: boolean, workItem }

function splitRange(start, end, parts) {
  const total = end - start + 1;
  const n = Math.max(1, Math.min(parts, total));
  const size = Math.floor(total / n);
  const rem = total % n;
  const ranges = [];
  let s = start;
  for (let i = 0; i < n; i++) {
    const extra = i < rem ? 1 : 0;
    const e = s + size + extra - 1;
    ranges.push([s, e]);
    s = e + 1;
  }
  return ranges;
}

async function launchWorkers() {
  try {
    // Check how many workers should be running
    const pendingCount = await ch.countWorkQueue('pending');
    const processingCount = await ch.countWorkQueue('processing');
     
    if (pendingCount === 0) {
      console.log('📭 No pending work items to process');
      return;
    }
     
    // If a group is already in progress, don't start another; just ensure all its parts are launched
    if (activeGroups.size > 0) {
      const [[groupId, grp]] = activeGroups.entries();
      // ensure we haven't lost any children; nothing else to do here
      console.log(`🔁 Group in progress for work id ${groupId} with ${grp.remaining} remaining`);
      return;
    }

    // Compute forward-priority threshold when not in full backfill mode
    let minAllowedStart = null;
    if (!BACKFILL_MODE) {
      const { rows: [mh] } = await targetDB.query('SELECT COALESCE(MAX(height), 0) AS max_h FROM blocks');
      const maxH = parseInt(mh.max_h || '0', 10);
      minAllowedStart = Math.max(START_HEIGHT, maxH - BACKFILL_WINDOW);
    }
     
    // Enforce target cap if configured
    const syncTarget = await getSyncTargetHeight();
     
    // Calculate how many workers we can use (up to NUM_WORKERS)
    const maxWorkers = parseInt(process.env.NUM_WORKERS || 2);

    // Get the next pending work item (prefer newer ranges; optionally enforce window and target cap)
    // ClickHouse: Use simple SELECT with FINAL to get deduplicated data
    const params = [];
    let sql = `
      SELECT id, start_height, end_height 
      FROM work_queue FINAL
      WHERE status = 'pending'
    `;
    if (!BACKFILL_MODE && minAllowedStart !== null) {
      sql += ` AND start_height >= $${params.length + 1}`;
      params.push(minAllowedStart);
    }
    if (syncTarget) {
      sql += ` AND end_height <= $${params.length + 1}`;
      params.push(syncTarget);
    }
    sql += `
      ORDER BY id ASC
      LIMIT 1
    `;
    const workItem = await targetDB.query(sql, params);
    if (workItem.rows.length === 0) {
      console.log('⚠️ No pending work items available');
      return;
    }

    const work = workItem.rows[0];
    
    // Update the status separately (ClickHouse doesn't support UPDATE...RETURNING)
    await ch.updateWorkQueueStatus(work.id, 'processing');
    const ranges = splitRange(parseInt(work.start_height,10), parseInt(work.end_height,10), maxWorkers);
    activeGroups.set(work.id, { remaining: ranges.length, failed: false, workItem: work });
    console.log(`👷 Launching group for work ${work.id}: ${work.start_height}-${work.end_height} split into ${ranges.length} part(s)`);

    ranges.forEach((r, idx) => {
      try {
        const [s,e] = r;
        const worker = spawn('node', ['src/core/worker.js', s, e, work.id], {
          stdio: ['ignore', 'pipe', 'pipe'],
          detached: false
        });
        const key = `${work.id}#${idx}`;
        activeWorkers.set(key, {
          process: worker,
          startTime: Date.now(),
          workItem: work,
          partIdx: idx,
          lastActivity: Date.now(),
          pid: worker.pid
        });

        worker.on('exit', async (code) => {
          try {
            activeWorkers.delete(key);
            const grp = activeGroups.get(work.id);
            if (grp) {
              if (code !== 0) grp.failed = true;
              grp.remaining -= 1;
              if (grp.remaining <= 0) {
                // finalize parent
                if (!grp.failed) {
                  await ch.deleteWorkQueueItem(work.id);
                  console.log(`✅ Group completed: ${work.start_height}-${work.end_height}`);
                } else {
                  await ch.updateWorkQueueStatus(work.id, 'failed', '[group_failed]');
                  console.error(`❌ Group failed: ${work.start_height}-${work.end_height}`);
                }
                activeGroups.delete(work.id);
                // Sync index_state with actual DB progress
                try {
                  const maxH = await ch.getMaxBlockHeight();
                  if (maxH > 0) await updateIndexState(maxH);
                } catch (syncErr) {
                  console.error(`❌ Error syncing index_state from blocks: ${syncErr.message}`);
                }
                // Backfill next group
                setImmediate(() => launchWorkers().catch(e => console.error(`❌ Error launching next group: ${e.message}`)));
              }
            }
          } catch (err) {
            console.error(`❌ Error handling child completion: ${err.message}`);
          } finally {
            try {
              worker.removeAllListeners('exit');
              if (worker.stdout) worker.stdout.removeAllListeners('data');
              if (worker.stderr) worker.stderr.removeAllListeners('data');
            } catch (_) {}
          }
        });

        if (worker.stdout) {
          worker.stdout.on('data', (data) => {
            const info = activeWorkers.get(key);
            if (info) info.lastActivity = Date.now();
            process.stdout.write(`[W:${key}] ${data}`);
          });
        }
        if (worker.stderr) {
          worker.stderr.on('data', (data) => {
            process.stderr.write(`[W:${key}] ${data}`);
          });
        }
      } catch (err) {
        console.error(`❌ Error launching child part for work ${work.id}: ${err.message}`);
      }
    });
     
  } catch (err) {
    console.error(`❌ Error in launchWorkers: ${err.message}`);
  }
}

// ... (rest of the code remains the same)

// Detect gaps in the database and assign workers to fill them
async function detectAndFillGaps() {
  try {
    // Throttle gap detection to prevent duplicate queuing
    const now = Date.now();
    if (now - lastGapDetectionTime < GAP_DETECTION_INTERVAL) {
      console.log(`⏸️ Gap detection throttled (last run ${Math.round((now - lastGapDetectionTime) / 60000)} minutes ago)`);
      return;
    }

    // Check if queue is too full for gap processing
    const queueStatus = await targetDB.query('SELECT COUNT(*) as count FROM work_queue WHERE status IN (\'pending\', \'processing\')');
    const queueCount = parseInt(queueStatus.rows[0].count);
    const maxQueueItems = parseInt(process.env.MAX_QUEUE_ITEMS || 50);
    
    if (queueCount >= maxQueueItems * 0.8) { // 80% threshold
      console.log(`⏸️ Skipping gap detection - queue nearly full (${queueCount}/${maxQueueItems})`);
      return;
    }

    // Determine backfill window threshold when not in full backfill mode
    let minAllowedStart = null;
    if (!BACKFILL_MODE) {
      const { rows: [mh] } = await targetDB.query('SELECT COALESCE(MAX(height), 0) AS max_h FROM blocks');
      const maxH = parseInt(mh.max_h || '0', 10);
      minAllowedStart = Math.max(START_HEIGHT, maxH - BACKFILL_WINDOW);
      console.log(`🪄 Backfill window active: considering gaps >= ${minAllowedStart}`);
    }
    
    // Existing logic to detect missing ranges ...
    // ...
    
    // When queuing each range, skip very old ones if backfill window active
    // example loop placeholder
    // for (const range of ranges) {
    //   if (!BACKFILL_MODE && minAllowedStart !== null && range.end < minAllowedStart) {
    //     continue; // skip very old ranges during catch-up
    //   }
    //   await queueRange(range)
    // }

    console.log(`✅ Gap detection completed - queued ${queuedRanges} ranges`);
    lastGapDetectionTime = now;
    
  } catch (err) {
    console.error(`❌ Error detecting gaps: ${err.message}`);
  }
}

// ... (rest of the code remains the same)

// Clean up old work queue entries
async function cleanupOldWorkQueue() {
  // ClickHouse: First SELECT the old failed entries, then DELETE them
  const oneHourAgo = Math.floor(Date.now() / 1000) - 3600;
  const cleanupResult = await targetDB.query(`
    SELECT start_height, end_height 
    FROM work_queue FINAL
    WHERE status = 'failed' 
      AND updated_at < ${oneHourAgo}
  `);
  
  if (cleanupResult.rows.length > 0) {
    console.log(`🧹 Cleaning up ${cleanupResult.rows.length} old failed work queue items`);
    
    // Delete them using ALTER TABLE DELETE (async in ClickHouse)
    await targetDB.query(`
      ALTER TABLE work_queue DELETE 
      WHERE status = 'failed' AND updated_at < ${oneHourAgo}
    `);
    
    // Re-queue them as pending for retry
    for (const row of cleanupResult.rows) {
      await ch.insertWorkQueue({
        start_height: row.start_height,
        end_height: row.end_height,
        status: 'pending'
      });
    }
  }
  
  // If not in backfill mode and queue is over capacity, drop very old pending items outside the window
  if (!BACKFILL_MODE) {
    const { rows: [mh] } = await targetDB.query('SELECT COALESCE(MAX(height), 0) AS max_h FROM blocks');
    const maxH = parseInt(mh.max_h || '0', 10);
    const minAllowedStart = Math.max(START_HEIGHT, maxH - BACKFILL_WINDOW);
    const { rows: [qc] } = await targetDB.query(`SELECT COUNT(*)::int AS cnt FROM work_queue WHERE status IN ('pending','processing')`);
    const queueCnt = parseInt(qc.cnt || '0', 10);
    if (queueCnt > MAX_QUEUE_ITEMS) {
      const del = await targetDB.query(`
        DELETE FROM work_queue
        WHERE status = 'pending'
          AND end_height < $1
      `, [minAllowedStart]);
      if (del.rowCount > 0) {
        console.log(`🧹 Dropped ${del.rowCount} very old pending items outside backfill window (< ${minAllowedStart})`);
      }
    }
  }
  
  // Enforce TARGET_MAX_HEIGHT: drop pending items beyond the cap so workers never pick them up
  if (TARGET_MAX_HEIGHT) {
    // First, reset any over-cap processing items back to pending
    const reset = await targetDB.query(`
      UPDATE work_queue
      SET status = 'pending', updated_at = NOW(),
          error_message = COALESCE(error_message,'') || ' [reset_over_cap]'
      WHERE status = 'processing'
        AND (start_height > $1 OR end_height > $1)
    `, [TARGET_MAX_HEIGHT]);
    if (reset.rowCount > 0) {
      console.log(`↩️ Reset ${reset.rowCount} processing items over cap back to pending`);
    }

    // Then, delete any pending items over the cap (correctly parenthesized)
    const del = await targetDB.query(`
      DELETE FROM work_queue
      WHERE status = 'pending'
        AND (start_height > $1 OR end_height > $1)
    `, [TARGET_MAX_HEIGHT]);
    if (del.rowCount > 0) {
      console.log(`🧹 Dropped ${del.rowCount} pending items above TARGET_MAX_HEIGHT=${TARGET_MAX_HEIGHT}`);
    }
  }
}

// ... (rest of the code remains the same)

// --------------------- Index State Helpers ---------------------
async function getLastIndexedHeight() {
  // Align progress with actual DB state to avoid re-queuing old ranges
  // Cast all values to UInt64 for ClickHouse type compatibility
  const res = await targetDB.query(
    `SELECT 
       GREATEST(
         COALESCE(CAST((SELECT last_processed_height FROM index_state FINAL WHERE index_name = $1) AS UInt64), CAST($2 - 1 AS UInt64)),
         COALESCE((SELECT MAX(height) FROM blocks), CAST($2 - 1 AS UInt64))
       ) AS last_idx`,
    [COMPONENT, START_HEIGHT]
  );
  return parseInt(res.rows[0]?.last_idx ?? (START_HEIGHT - 1), 10);
}

async function getMaxBlockHeight() {
  const { rows: [r] } = await targetDB.query('SELECT COALESCE(MAX(height), CAST(0 AS UInt64)) AS max_h FROM blocks');
  return parseInt(r.max_h || '0', 10);
}

// Quick completeness check for a small range (uses generate_series over ~100-1000)
async function isRangeComplete(start, end) {
  const { rows: [r] } = await targetDB.query(
    `WITH s AS (
       SELECT generate_series($1::bigint, $2::bigint) AS h
     )
     SELECT COUNT(*)::int AS missing
     FROM s
     LEFT JOIN blocks b ON b.height = s.h
     WHERE b.height IS NULL`,
    [start, end]
  );
  return parseInt(r.missing || '0', 10) === 0;
}

// Reconcile lingering 'processing' items that are actually complete in DB
async function reconcileProcessingItems(limit = 50) {
  const { rows: items } = await targetDB.query(
    `SELECT id, start_height, end_height
     FROM work_queue
     WHERE status = 'processing'
     ORDER BY id ASC
     LIMIT $1`,
    [limit]
  );
  let fixed = 0;
  for (const it of items) {
    try {
      if (await isRangeComplete(it.start_height, it.end_height)) {
        await targetDB.query(
          `UPDATE work_queue
           SET status = 'done', updated_at = NOW(),
               error_message = COALESCE(error_message,'') || ' [auto_mark_done_reconcile]'
           WHERE id = $1`,
          [it.id]
        );
        fixed++;
      }
    } catch (e) {
      console.warn(`⚠️ reconcileProcessingItems failed for ${it.id}: ${e.message}`);
    }
  }
  if (fixed > 0) {
    console.log(`✅ Reconciled ${fixed} processing item(s) already complete`);
  }
}

async function updateIndexState(height) {
  try {
    // ClickHouse: Just insert, ReplacingMergeTree will handle deduplication
    await ch.updateLastIndexedHeight(COMPONENT, height);
  } catch (e) {
    console.error(`❌ updateIndexState failed: ${e.message}`);
  }
}

// ... (rest of the code remains the same)

async function mainLoop() {
  try {
    await optionalCallByName('recoverStuckWorkers');
    await optionalCallByName('processFailedBlocks');
    await optionalCallByName('detectAndFillGaps');

    // Always try to launch workers for pending work
    await launchWorkers();

    // Proactively seed queue if empty or under capacity and chain has advanced
    const [{ rows: [p] }, { rows: [r] }] = await Promise.all([
      targetDB.query(`SELECT COUNT(*)::int AS c FROM work_queue WHERE status='pending'`),
      targetDB.query(`SELECT COUNT(*)::int AS c FROM work_queue WHERE status='processing'`),
    ]);
    const pending = parseInt(p.c || '0', 10);
    const processing = parseInt(r.c || '0', 10);
    const total = pending + processing;
    const maxQueueItems = parseInt(process.env.MAX_QUEUE_ITEMS || '50', 10);
    const capacityThreshold = Math.floor(maxQueueItems * 0.8);

    // If nothing pending and we're at/over the cap, finalize and stop to avoid connection churn
    if (TARGET_MAX_HEIGHT && total === 0) {
      const maxBlock = await getMaxBlockHeight();
      if (activeWorkers.size === 0 && maxBlock >= TARGET_MAX_HEIGHT) {
        console.log(`� TARGET_MAX_HEIGHT ${TARGET_MAX_HEIGHT} reached with no pending/active work; stopping indexer.`);
        await updateIndexState(maxBlock);
        return shutdown();
      }
      // If there are lingering 'processing' rows in DB but no activeWorkers, reconcile them
      if (activeWorkers.size === 0 && processing > 0) {
        await reconcileProcessingItems(50);
      }
    }

    if (total === 0) {
      const latestHeight = await getSyncTargetHeight();
      const lastIndexed = await getLastIndexedHeight();
      console.log(`📭 Queue empty; heights: lastIndexed=${lastIndexed}, latestHeight=${latestHeight}`);
    }

    // Only seed when we have room
    if (total < capacityThreshold) {
      const latestHeight = await getSyncTargetHeight();
      const lastIndexed = await getLastIndexedHeight();

      if (lastIndexed < latestHeight) {
        // Decide a seeding window: up to NUM_WORKERS ranges ahead
        const rangeSize = parseInt(process.env.ORCH_ASSIGN_RANGE || '1000', 10);
        const targetEnd = Math.min(lastIndexed + (rangeSize * (parseInt(process.env.NUM_WORKERS || '5', 10))), latestHeight);
        await seedWorkQueue(lastIndexed + 1, targetEnd);
      } else if (TARGET_MAX_HEIGHT) {
        // Explicitly indicate completion to the configured cap
        console.log(`✅ Reached configured TARGET_MAX_HEIGHT=${TARGET_MAX_HEIGHT}. No further seeding.`);
      }
    }
  } catch (err) {
    console.error(`❌ Main loop error: ${err.message}`);
  }

  // Continue the loop
  setTimeout(mainLoop, 10000); // 10 seconds
}

// ... (rest of the code remains the same)

// Helper to safely call optional functions that might not be defined
async function optionalCall(fn, name) {
  if (typeof fn === 'function') {
    try {
      await fn();
    } catch (e) {
      console.error(`❌ Error in ${name}: ${e.message}`);
    }
  } else {
    // Uncomment for debugging missing hooks
    // console.warn(`ℹ️ Skipping ${name} - not defined`);
  }
}

// Variant that avoids ReferenceError by resolving from globalThis
async function optionalCallByName(name) {
  try {
    const fn = globalThis?.[name];
    if (typeof fn === 'function') {
      await fn();
    }
  } catch (e) {
    console.error(`❌ Error in ${name}: ${e.message}`);
  }
}

// ... (rest of the code remains the same)

// Global error handlers to surface crashes instead of silent restarts
process.on('unhandledRejection', (reason) => {
  console.error('💥 UnhandledRejection:', reason);
});
process.on('uncaughtException', (err) => {
  console.error('💥 UncaughtException:', err);
});

// Graceful shutdown
async function shutdown() {
  console.log('🛑 Shutting down, closing connections...');
  try {
    const { closeClickHouse } = require('../database/db');
    await closeClickHouse();
    console.log('✅ Connections closed');
  } catch (err) {
    console.error('❌ Error closing connections:', err.message);
  }
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// --------------------- Schema Setup ---------------------
async function ensureTables() {
  // Test ClickHouse connection
  console.log('⏳ Testing ClickHouse connection...');
  const clickhouseOk = await testClickHouseConnection();
  if (!clickhouseOk) {
    throw new Error('ClickHouse connection failed - cannot start orchestrator');
  }
  
  // Initialize all required tables
  console.log('⏳ Initializing ClickHouse tables...');
  await initializeDatabase(COMPONENT, START_HEIGHT);
}

// --------------------- Helpers: Work Queue Seeding ---------------------
async function seedWorkQueue(fromHeight, toHeight) {
  if (!Number.isFinite(fromHeight) || !Number.isFinite(toHeight)) {
    console.error(`❌ seedWorkQueue received invalid bounds: ${fromHeight} → ${toHeight}`);
    return 0;
  }
  if (toHeight < fromHeight) return 0;
  const rangeSize = parseInt(process.env.ORCH_ASSIGN_RANGE || '1000', 10);
  let inserted = 0;
  for (let s = fromHeight; s <= toHeight; s += rangeSize) {
    const start = s;
    const end = Math.min(s + rangeSize - 1, toHeight);
    try {
      // Subtract any existing pending/processing overlaps
      const segments = await subtractOverlaps(start, end);
      for (const [xs, xe] of segments) {
        // ClickHouse insert - use unique ID based on range
        const workId = Date.now() * 1000 + Math.floor(Math.random() * 1000);
        await ch.insertWorkQueue({
          id: workId,
          start_height: xs,
          end_height: xe,
          status: 'pending'
        });
        inserted++;
      }
    } catch (e) {
      console.error(`❌ Failed seeding range ${start}-${end}: ${e.message}`);
      break;
    }
  }
  if (inserted > 0) {
    console.log(`📦 Seeded ${inserted} work ranges: ${fromHeight} → ${toHeight} (≈${rangeSize} each)`);
  } else {
    console.log(`ℹ️ No new ranges seeded for ${fromHeight} → ${toHeight} (likely already queued)`);
  }
  return inserted;
}

// Query existing overlapping ranges and subtract them from a candidate [s,e]
async function subtractOverlaps(s, e) {
  if (e < s) return [];
  const rows = await ch.getOverlappingRanges(s, e);
  // Build disjoint segments remaining after subtracting overlaps
  let segments = [[s, e]];
  for (const r of rows) {
    const os = parseInt(r.s, 10), oe = parseInt(r.e, 10);
    const next = [];
    for (const [a, b] of segments) {
      // no overlap
      if (oe < a || os > b) {
        next.push([a, b]);
        continue;
      }
      // overlap: left remainder
      if (os > a) next.push([a, os - 1]);
      // right remainder
      if (oe < b) next.push([oe + 1, b]);
    }
    segments = next.filter(([x, y]) => y >= x);
    if (segments.length === 0) break;
  }
  return segments;
}

// ... (rest of the code remains the same)

// --------------------- Startup ---------------------
let __started = false;
(async () => {
  if (__started) return;
  __started = true;
  try {
    console.log('⏳ Ensuring tables before starting...');
    await ensureTables();

    // Initial seeding so we don't start with an empty queue
    try {
      const latestHeight = await getSyncTargetHeight();
      if (TARGET_MAX_HEIGHT) {
        console.log(`🎯 TARGET_MAX_HEIGHT resolved to ${TARGET_MAX_HEIGHT}`);
      } else {
        console.log('🎯 TARGET_MAX_HEIGHT not set; syncing to live RPC tip');
      }
      const lastIndexed = await getLastIndexedHeight();
      console.log(`🧭 Initial heights: lastIndexed=${lastIndexed}, latestHeight=${latestHeight}`);
      if (lastIndexed < latestHeight) {
        const rangeSize = parseInt(process.env.ORCH_ASSIGN_RANGE || '1000', 10);
        const targetEnd = Math.min(lastIndexed + (rangeSize * (parseInt(process.env.NUM_WORKERS || '5', 10))), latestHeight);
        await seedWorkQueue(lastIndexed + 1, targetEnd);
      } else {
        console.log('✅ Already at tip on startup; no initial seeding needed');
      }
    } catch (seedErr) {
      console.error(`❌ Initial seeding error: ${seedErr.message}`);
    }

    console.log('🚦 Starting orchestrator main loop');
    await mainLoop();
  } catch (e) {
    console.error(`💥 Fatal startup error: ${e.message}`);
    process.exit(1);
  }
})();
