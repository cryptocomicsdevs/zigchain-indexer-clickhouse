// scripts/retry_failed.js
const http = require('http');
const https = require('https');
const axios = require('axios');
const crypto = require('crypto');
const { targetDB } = require('./database/db');
const { ensurePartitionForHeight } = require('./database/partition'); // use height-based partitioning

// RPC endpoints from environment variable
const RPC_LIST = process.env.RPC_ENDPOINTS 
  ? process.env.RPC_ENDPOINTS.split(',').map(url => url.trim())
  : [
      'http://5.189.171.155:26657',
      'http://5.189.162.146:26657',
      'http://167.86.79.37:26657',
      'http://82.208.20.12:26657',
    ];

console.log(`🔧 Retry script using ${RPC_LIST.length} RPC endpoints`);

// Configuration from environment variables
const MAX_RETRY_ATTEMPTS = parseInt(process.env.MAX_RETRY_ATTEMPTS || '5', 10);
const RETRY_CONCURRENCY = parseInt(process.env.RETRY_CONCURRENCY || '4', 10);

// ---------- axios with keep-alive ----------
const axiosInstance = axios.create({
  timeout: 15000,
  httpAgent: new http.Agent({ keepAlive: true, maxSockets: 100 }),
  httpsAgent: new https.Agent({ keepAlive: true, maxSockets: 100 }),
});

let rpcIndex = 0;
function currentRPC() {
  return RPC_LIST[rpcIndex % RPC_LIST.length];
}
function rotateRPC() {
  rpcIndex = (rpcIndex + 1) % RPC_LIST.length;
  console.warn(` Switched RPC to ${currentRPC()}`);
}

async function rpcGet(path, retries = 3, backoffMs = 600) {
  for (let attempt = 1; attempt <= retries * RPC_LIST.length; attempt++) {
    const url = `${currentRPC()}${path}`;
    try {
      const res = await axiosInstance.get(url);
      if (res.status === 200 && res.data) return res.data;
      throw new Error(`HTTP ${res.status}`);
    } catch (e) {
      if (attempt % retries === 0) rotateRPC();
      await new Promise(r => setTimeout(r, backoffMs * Math.min(attempt, 5)));
    }
  }
  throw new Error(`All RPCs failed for ${path}`);
}

function maybeB64Decode(s) {
  if (typeof s !== 'string') return s;
  if (/^[A-Za-z0-9+/=]+$/.test(s) && s.length % 4 === 0) {
    try {
      const dec = Buffer.from(s, 'base64').toString('utf8');
      if (/^[\x09\x0A\x0D\x20-\x7E]*$/.test(dec)) return dec;
    } catch (_) {}
  }
  return s;
}

function extractMessageTypeFromEvents(events = []) {
  for (const e of events) {
    const type = maybeB64Decode(e.type);
    if (type === 'message' && Array.isArray(e.attributes)) {
      for (const a of e.attributes) {
        const k = maybeB64Decode(a.key);
        const v = maybeB64Decode(a.value);
        if (k === 'action' && v) return v;
      }
    }
  }
  return 'unknown';
}

function computeBackoffSeconds(attempts) {
  return Math.min(600, Math.pow(2, attempts) * 5);
}

async function ensureFailedSchema() {
  await targetDB.query(`
    ALTER TABLE failed_txs
      ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      ADD COLUMN IF NOT EXISTS last_error TEXT;
  `);
  await targetDB.query(`CREATE INDEX IF NOT EXISTS idx_failed_txs_next_retry ON failed_txs (next_retry_at)`);
  await targetDB.query(`CREATE INDEX IF NOT EXISTS idx_failed_txs_height ON failed_txs (height)`);
}

async function mapWithConcurrency(items, limit, fn) {
  const out = new Array(items.length);
  let next = 0, active = 0;
  return await new Promise((resolve, reject) => {
    const kick = () => {
      if (next === items.length && active === 0) return resolve(out);
      while (active < limit && next < items.length) {
        const i = next++, item = items[i];
        active++;
        Promise.resolve(fn(item, i))
          .then(v => { out[i] = v; active--; kick(); })
          .catch(err => reject(err));
      }
    };
    kick();
  });
}

async function processBatch(limitTxRows = 800, maxAttempts = MAX_RETRY_ATTEMPTS, heightConcurrency = RETRY_CONCURRENCY) {
  const { rows } = await targetDB.query(
    `
    SELECT tx_hash, height, attempts
    FROM failed_txs
    WHERE next_retry_at <= NOW()
      AND attempts < $1
    ORDER BY next_retry_at ASC, created_at ASC
    LIMIT $2
    `,
    [maxAttempts, limitTxRows]
  );

  if (rows.length === 0) {
    console.log(' No failed txs eligible for retry.');
    return 0;
  }

  const byHeight = new Map();
  for (const r of rows) {
    const h = Number(r.height);
    if (!byHeight.has(h)) byHeight.set(h, []);
    byHeight.get(h).push({ tx_hash: r.tx_hash.toUpperCase(), attempts: r.attempts });
  }
  const heights = Array.from(byHeight.keys()).sort((a, b) => a - b);

  let totalSuccess = 0;

  await mapWithConcurrency(heights, heightConcurrency, async (height) => {
    try {
      const [blockResp, blockResultsResp] = await Promise.all([
        rpcGet(`/block?height=${height}`),
        rpcGet(`/block_results?height=${height}`)
      ]);
      const block = blockResp.result.block;
      const blockTime = new Date(block.header.time).toISOString();
      const rawTxs = block.data.txs || [];
      const txsResults = blockResultsResp.result?.txs_results || [];

      const hashToEvents = new Map();
      for (let i = 0; i < rawTxs.length; i++) {
        const raw = rawTxs[i];
        const hash = crypto.createHash('sha256').update(Buffer.from(raw, 'base64')).digest('hex').toUpperCase();
        const ev = (txsResults[i] && txsResults[i].events) || [];
        hashToEvents.set(hash, ev);
      }

      // Use correct partition based on height
      const partition = await ensurePartitionForHeight(targetDB, height);

      const successHashes = [];

      for (const { tx_hash, attempts } of byHeight.get(height)) {
        const events = hashToEvents.get(tx_hash) || [];
        const messageType = extractMessageTypeFromEvents(events);

        if (messageType === 'unknown') {
          const nextDelay = computeBackoffSeconds(attempts + 1);
          await targetDB.query(
            `UPDATE failed_txs
               SET attempts = attempts + 1,
                   last_error = $2,
                   next_retry_at = NOW() + ($3 || ' seconds')::interval
             WHERE tx_hash = $1`,
            [tx_hash, 'unknown_message_type', String(nextDelay)]
          );
          continue;
        }

        await targetDB.query(
          `
          INSERT INTO ${partition} (tx_hash, height, timestamp, message_type, decoded)
          VALUES ($1, $2, $3, $4, $5)
          ON CONFLICT (tx_hash, height, message_type) DO NOTHING
          `,
          [tx_hash, height, blockTime, messageType, JSON.stringify(events)]
        );
        successHashes.push(tx_hash);
      }

      if (successHashes.length > 0) {
        await targetDB.query(`DELETE FROM failed_txs WHERE tx_hash = ANY($1::text[])`, [successHashes]);
        totalSuccess += successHashes.length;
        console.log(` Height ${height}: retried ${successHashes.length}/${byHeight.get(height).length}`);
      } else {
        console.log(` Height ${height}: no successful retries this round`);
      }
    } catch (err) {
      const msg = String(err.message || err).slice(0, 800);
      console.warn(` Height ${height} retry pass failed: ${msg}`);
      const rowsForH = byHeight.get(height);
      for (const { tx_hash, attempts } of rowsForH) {
        const nextDelay = computeBackoffSeconds(attempts + 1);
        await targetDB.query(
          `UPDATE failed_txs
             SET attempts = attempts + 1,
                 last_error = $2,
                 next_retry_at = NOW() + ($3 || ' seconds')::interval
           WHERE tx_hash = $1`,
          [tx_hash, msg, String(nextDelay)]
        );
      }
    }
  });

  return totalSuccess;
}

(async () => {
  console.log('🔁 Retrying failed transactions (grouped by block)…');
  console.log(`🔧 Config: max_attempts=${MAX_RETRY_ATTEMPTS}, concurrency=${RETRY_CONCURRENCY}`);

  await ensureFailedSchema();

  let total = 0;
  while (true) {
    const succeeded = await processBatch(800, MAX_RETRY_ATTEMPTS, RETRY_CONCURRENCY);
    total += succeeded;
    if (succeeded === 0) break;
  }

  console.log(`🎉 Retry script finished. Succeeded this run: ${total}`);
  await targetDB.end();
})();
