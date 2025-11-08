#!/usr/bin/env node

const { Pool } = require('pg');

// Database configuration using environment variables to match docker-compose
const targetDB = new Pool({
  host: process.env.PGHOST || '38.242.212.160',
  port: process.env.PGPORT || 5432,
  user: process.env.PGUSER || 'postgres',
  password: process.env.PGPASSWORD || 'SW5ae56Fss',
  database: process.env.PGDATABASE || 'zigchain_mainnet_database',
  max: 2,
  idleTimeoutMillis: 10000,
  connectionTimeoutMillis: 5000,
});

async function checkIndexerHealth() {
  console.log('🔍 Zigscan Indexer Health Check');
  console.log('================================');
  
  try {
    // 1. Check database connection
    console.log('\n📡 Database Connection:');
    const connTest = await targetDB.query('SELECT NOW() as current_time');
    console.log(`✅ Connected successfully at ${connTest.rows[0].current_time}`);
    
    // 2. Check index state
    console.log('\n📊 Index State:');
    const indexState = await targetDB.query(`
      SELECT index_name, last_processed_height, updated_at 
      FROM index_state 
      ORDER BY updated_at DESC
    `);
    
    if (indexState.rows.length > 0) {
      indexState.rows.forEach(row => {
        const timeDiff = Math.round((Date.now() - new Date(row.updated_at)) / 1000);
        console.log(`   • ${row.index_name}: Height ${row.last_processed_height} (${timeDiff}s ago)`);
      });
    } else {
      console.log('   ⚠️ No index state found');
    }
    
    // 3. Check work queue status
    console.log('\n🔄 Work Queue Status:');
    const workQueue = await targetDB.query(`
      SELECT status, COUNT(*) as count, 
             MIN(start_height) as min_height, 
             MAX(end_height) as max_height
      FROM work_queue 
      GROUP BY status 
      ORDER BY status
    `);
    
    if (workQueue.rows.length > 0) {
      workQueue.rows.forEach(row => {
        console.log(`   • ${row.status}: ${row.count} items (${row.min_height}-${row.max_height})`);
      });
    } else {
      console.log('   ✅ Work queue is empty');
    }
    
    // 4. Check failed blocks
    console.log('\n❌ Failed Blocks Analysis:');
    const failedBlocks = await targetDB.query(`
      SELECT status, error_type, COUNT(*) as count,
             MIN(height) as min_height,
             MAX(height) as max_height
      FROM failed_blocks 
      GROUP BY status, error_type 
      ORDER BY status, error_type
    `);
    
    if (failedBlocks.rows.length > 0) {
      failedBlocks.rows.forEach(row => {
        console.log(`   • ${row.status}/${row.error_type}: ${row.count} blocks (${row.min_height}-${row.max_height})`);
      });
    } else {
      console.log('   ✅ No failed blocks');
    }
    
    // 5. Check for gaps in blocks table
    console.log('\n🔍 Gap Detection:');
    const lastIndexed = await targetDB.query(`
      SELECT last_processed_height FROM index_state WHERE index_name = 'decoded_indexer'
    `);
    
    if (lastIndexed.rows.length > 0) {
      const maxHeight = lastIndexed.rows[0].last_processed_height;
      
      const gaps = await targetDB.query(`
        WITH height_series AS (
          SELECT generate_series(1, $1) AS expected_height
        ),
        missing_blocks AS (
          SELECT hs.expected_height as missing_height
          FROM height_series hs
          LEFT JOIN blocks b ON hs.expected_height = b.height
          WHERE b.height IS NULL
        )
        SELECT COUNT(*) as gap_count,
               MIN(missing_height) as first_gap,
               MAX(missing_height) as last_gap
        FROM missing_blocks
      `, [Math.min(maxHeight, 10000)]); // Check first 10k blocks for performance
      
      const gapInfo = gaps.rows[0];
      if (parseInt(gapInfo.gap_count) > 0) {
        console.log(`   ⚠️ Found ${gapInfo.gap_count} missing blocks (${gapInfo.first_gap}-${gapInfo.last_gap})`);
      } else {
        console.log(`   ✅ No gaps found in first ${Math.min(maxHeight, 10000)} blocks`);
      }
    } else {
      console.log('   ⚠️ No index state to check gaps against');
    }
    
    // 6. Check recent processing activity
    console.log('\n⏱️ Recent Activity:');
    const recentBlocks = await targetDB.query(`
      SELECT COUNT(*) as recent_blocks
      FROM blocks 
      WHERE created_at > NOW() - INTERVAL '1 hour'
    `);
    
    const recentWork = await targetDB.query(`
      SELECT COUNT(*) as recent_work
      FROM work_queue 
      WHERE updated_at > NOW() - INTERVAL '1 hour'
    `);
    
    console.log(`   • Blocks processed in last hour: ${recentBlocks.rows[0].recent_blocks}`);
    console.log(`   • Work queue updates in last hour: ${recentWork.rows[0].recent_work}`);
    
    // 7. Database statistics
    console.log('\n📈 Database Statistics:');
    const stats = await targetDB.query(`
      SELECT 
        (SELECT COUNT(*) FROM blocks) as total_blocks,
        (SELECT MIN(height) FROM blocks) as min_height,
        (SELECT MAX(height) FROM blocks) as max_height,
        (SELECT COUNT(*) FROM transactions_raw) as total_transactions
    `);
    
    if (stats.rows.length > 0) {
      const s = stats.rows[0];
      console.log(`   • Total blocks: ${s.total_blocks}`);
      console.log(`   • Height range: ${s.min_height} - ${s.max_height}`);
      console.log(`   • Total transactions: ${s.total_transactions}`);
    }
    
    // 8. Connection pool status
    console.log('\n🔗 Connection Pool Status:');
    console.log(`   • Total connections: ${targetDB.totalCount}`);
    console.log(`   • Idle connections: ${targetDB.idleCount}`);
    console.log(`   • Waiting clients: ${targetDB.waitingCount}`);
    
    // 9. Loop Detection Analysis
    console.log('\n🔄 Loop Detection Analysis:');
    
    // Check for stuck processing items
    const stuckItems = await targetDB.query(`
      SELECT COUNT(*) as stuck_count,
             MIN(EXTRACT(EPOCH FROM (NOW() - updated_at))/60) as min_minutes,
             MAX(EXTRACT(EPOCH FROM (NOW() - updated_at))/60) as max_minutes
      FROM work_queue 
      WHERE status = 'processing' 
        AND updated_at < NOW() - INTERVAL '30 minutes'
    `);
    
    if (parseInt(stuckItems.rows[0].stuck_count) > 0) {
      console.log(`   ⚠️ Stuck processing items: ${stuckItems.rows[0].stuck_count} (${Math.round(stuckItems.rows[0].min_minutes)}-${Math.round(stuckItems.rows[0].max_minutes)} minutes)`);
    } else {
      console.log(`   ✅ No stuck processing items`);
    }
    
    // Check for potential loops (same range queued multiple times)
    const loopingItems = await targetDB.query(`
      WITH work_item_history AS (
        SELECT start_height, end_height, COUNT(*) as queue_count
        FROM work_queue 
        WHERE updated_at > NOW() - INTERVAL '2 hours'
        GROUP BY start_height, end_height
        HAVING COUNT(*) > 2
      )
      SELECT COUNT(*) as looping_ranges,
             MAX(queue_count) as max_queue_count
      FROM work_item_history
    `);
    
    if (parseInt(loopingItems.rows[0].looping_ranges) > 0) {
      console.log(`   ⚠️ Potential loops: ${loopingItems.rows[0].looping_ranges} ranges (max ${loopingItems.rows[0].max_queue_count} times)`);
    } else {
      console.log(`   ✅ No looping work items detected`);
    }
    
    // Check for duplicate pending items
    const duplicates = await targetDB.query(`
      SELECT COUNT(*) as duplicate_ranges
      FROM (
        SELECT start_height, end_height
        FROM work_queue 
        WHERE status = 'pending'
        GROUP BY start_height, end_height
        HAVING COUNT(*) > 1
      ) duplicates
    `);
    
    if (parseInt(duplicates.rows[0].duplicate_ranges) > 0) {
      console.log(`   ⚠️ Duplicate pending ranges: ${duplicates.rows[0].duplicate_ranges}`);
    } else {
      console.log(`   ✅ No duplicate pending items`);
    }
    
    // Check for stale pending items
    const stalePending = await targetDB.query(`
      SELECT COUNT(*) as stale_count,
             MIN(EXTRACT(EPOCH FROM (NOW() - updated_at))/60) as min_minutes,
             MAX(EXTRACT(EPOCH FROM (NOW() - updated_at))/60) as max_minutes
      FROM work_queue 
      WHERE status = 'pending' 
        AND updated_at < NOW() - INTERVAL '1 hour'
    `);
    
    if (parseInt(stalePending.rows[0].stale_count) > 0) {
      console.log(`   ⚠️ Stale pending items: ${stalePending.rows[0].stale_count} (${Math.round(stalePending.rows[0].min_minutes)}-${Math.round(stalePending.rows[0].max_minutes)} minutes)`);
    } else {
      console.log(`   ✅ No stale pending items`);
    }
    
    console.log('\n✅ Health check completed successfully');
    
  } catch (error) {
    console.error(`❌ Health check failed: ${error.message}`);
    process.exit(1);
  } finally {
    await targetDB.end();
  }
}

// Run the health check
checkIndexerHealth().catch(console.error);
