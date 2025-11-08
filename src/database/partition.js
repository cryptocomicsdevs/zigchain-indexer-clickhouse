// src/database/partition/partition.js - dynamic native PostgreSQL range partitioning by block height

const BLOCKS_PER_PARTITION = 50000;

/**
 * Ensure the parent partitioned table exists
 */
async function ensureParentTable(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.transactions_raw (
      tx_hash TEXT,
      height INT NOT NULL,
      timestamp TIMESTAMPTZ,
      message_type TEXT,
      decoded JSONB,
      PRIMARY KEY (tx_hash, height, message_type)
    ) PARTITION BY RANGE (height);
  `);
}

/**
 * Calculate the partition range for a given block height
 */
function getPartitionRange(height) {
  const start = Math.floor((height - 1) / BLOCKS_PER_PARTITION) * BLOCKS_PER_PARTITION + 1;
  const end = start + BLOCKS_PER_PARTITION;
  return { start, end };
}

/**
 * Ensure a partition exists for a given block height
 * Returns the partition table name
 */
async function ensurePartitionForHeight(pool, height) {
  await ensureParentTable(pool);

  const { start, end } = getPartitionRange(height);
  const partName = `transactions_raw_p${start}_${end - 1}`;

  // Check if partition already exists
  const exists = await pool.query(
    `
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = $1 AND n.nspname = 'public'
    `,
    [partName]
  );

  if (exists.rowCount > 0) {
    return partName;
  }

  console.log(`🆕 Creating partition ${partName} for blocks ${start}–${end - 1}`);

  // ✅ Start transaction so LOCK TABLE works
  await pool.query('BEGIN');
  try {
    // Lock parent table to avoid race conditions
    await pool.query(`LOCK TABLE public.transactions_raw IN SHARE MODE`);

    // Create partition table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS public."${partName}"
      PARTITION OF public.transactions_raw
      FOR VALUES FROM (${start}) TO (${end});
    `);

    // Add indexes to the new partition
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_${partName}_height ON public."${partName}" (height);
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_${partName}_txhash ON public."${partName}" (tx_hash);
    `);

    await pool.query('COMMIT');
  } catch (err) {
    await pool.query('ROLLBACK');
    throw err;
  }

  return partName;
}

module.exports = { ensurePartitionForHeight, getPartitionRange, ensureParentTable };
