#!/usr/bin/env node

const { Pool } = require('pg');

// Test database connection with the optimized configuration
async function testConnection() {
  console.log('🔧 Testing Database Connection...');
  
  // Test PgBouncer connection (port 6432)
  const pgBouncerDB = new Pool({
    host: 'localhost',
    port: 6432,
    user: 'postgres',
    password: process.env.PGPASSWORD || 'YtW4/=aieFslds',
    database: 'zigchain_testnet_database',
    max: 2,
    connectionTimeoutMillis: 5000,
  });

  try {
    console.log('📡 Testing PgBouncer connection (localhost:6432)...');
    const result = await pgBouncerDB.query('SELECT NOW() as current_time, version() as pg_version');
    console.log(`✅ PgBouncer connection successful!`);
    console.log(`   Time: ${result.rows[0].current_time}`);
    console.log(`   PostgreSQL: ${result.rows[0].pg_version.split(' ')[0]} ${result.rows[0].pg_version.split(' ')[1]}`);
    
    // Test basic table existence
    console.log('\n🔍 Checking table structure...');
    const tables = await pgBouncerDB.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_name IN ('blocks', 'transactions_raw', 'index_state', 'work_queue', 'failed_blocks')
      ORDER BY table_name
    `);
    
    console.log('📋 Available tables:');
    tables.rows.forEach(row => {
      console.log(`   • ${row.table_name}`);
    });
    
    // Check index state
    const indexState = await pgBouncerDB.query(`
      SELECT index_name, last_processed_height, updated_at 
      FROM index_state 
      WHERE index_name = 'orchestrator'
    `);
    
    if (indexState.rows.length > 0) {
      const state = indexState.rows[0];
      const timeDiff = Math.round((Date.now() - new Date(state.updated_at)) / 1000);
      console.log(`\n📊 Current Index State:`);
      console.log(`   • Component: ${state.index_name}`);
      console.log(`   • Last Height: ${state.last_processed_height}`);
      console.log(`   • Updated: ${timeDiff}s ago`);
    } else {
      console.log('\n⚠️ No index state found - indexer may not have started yet');
    }
    
    console.log('\n✅ Connection test completed successfully!');
    
  } catch (error) {
    console.error(`❌ Connection test failed: ${error.message}`);
    
    if (error.message.includes('ECONNREFUSED')) {
      console.log('\n💡 Troubleshooting suggestions:');
      console.log('   • Check if PgBouncer is running: docker-compose ps');
      console.log('   • Check if port 6432 is accessible: netstat -tlnp | grep 6432');
      console.log('   • Restart services: docker-compose down && docker-compose up -d');
    }
    
    process.exit(1);
  } finally {
    await pgBouncerDB.end();
  }
}

// Run the connection test
testConnection().catch(console.error);
