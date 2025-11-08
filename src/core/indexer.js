// src/core/indexer.js - Legacy single-threaded indexer
const axios = require('axios');
const crypto = require('crypto');
const { targetDB } = require('./database/db');
const { ensurePartitionForHeight } = require('./database/partition');

const RPC = 'http://82.208.20.12:26657';
const [, , startArg, endArg] = process.argv;
const START = parseInt(startArg || '1300000');
const END = parseInt(endArg || START + 99);

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

async function fetchBlock(height) {
  const res = await axios.get(`${RPC}/block?height=${height}`);
  return res.data.result.block;
}

async function fetchBlockResults(height) {
  const res = await axios.get(`${RPC}/block_results?height=${height}`);
  return res.data.result;
}

(async () => {
  console.log(`🚀 Legacy Indexer running: ${START} → ${END}`);

  try {
    for (let height = START; height <= END; height++) {
      try {
        const [block, blockResults] = await Promise.all([
          fetchBlock(height),
          fetchBlockResults(height)
        ]);

        const blockTime = new Date(block.header.time).toISOString();
        const rawTxs = block.data.txs || [];
        const eventsByTx = blockResults.txs_results || [];

        if (rawTxs.length === 0) {
          console.log(`⏭️ [${height}] No transactions`);
          continue;
        }

        const partition = await ensurePartitionForHeight(targetDB, height);
        console.log(`📦 [${height}] Using partition: ${partition} (${rawTxs.length} txs)`);

        for (let i = 0; i < rawTxs.length; i++) {
          const raw = rawTxs[i];
          const events = eventsByTx[i]?.events || [];

          const hash = crypto.createHash('sha256')
            .update(Buffer.from(raw, 'base64'))
            .digest('hex')
            .toUpperCase();

          try {
            const msgEvent = events.find(e =>
              e.type === 'message' &&
              e.attributes?.some(a => a.key === 'action' && a.value)
            );

            if (!msgEvent) {
              console.log(`⏭️ [${height}] Skipping TX ${hash} — no action attribute`);
              continue;
            }

            const actionAttr = msgEvent.attributes.find(a => a.key === 'action');
            const messageType = actionAttr?.value || 'unknown';

            const insertData = {
              tx_hash: hash,
              height,
              timestamp: blockTime,
              message_type: messageType,
              decoded: events,
            };

            const client = await targetDB.connect();
            try {
              await client.query(
                `INSERT INTO ${partition} (
                  tx_hash, height, timestamp, message_type, decoded
                ) VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (tx_hash, height, message_type) DO NOTHING`,
                [
                  insertData.tx_hash,
                  insertData.height,
                  insertData.timestamp,
                  insertData.message_type,
                  JSON.stringify(insertData.decoded)
                ]
              );
            } finally {
              client.release();
            }

            console.log(`✅ [${height}] TX ${hash} → ${messageType}`);

          } catch (err) {
            console.error(`❌ TX ${hash} failed: ${err.message}`);

            const client = await targetDB.connect();
            try {
              await client.query(`
                INSERT INTO failed_txs (tx_hash, height, error)
                VALUES ($1, $2, $3)
                ON CONFLICT (tx_hash) DO UPDATE SET error = $3, created_at = NOW()
              `, [hash, height, err.message]);
            } finally {
              client.release();
            }
          }
        }

      } catch (err) {
        console.error(`🔥 Block ${height} failed: ${err.message}`);
        await targetDB.query(`
          INSERT INTO failed_blocks (height, error)
          VALUES ($1, $2)
          ON CONFLICT (height) DO UPDATE SET error = $2, created_at = NOW()
        `, [height, err.message]);

        await sleep(2000);
      }
    }

    console.log(`🎉 Completed: ${START} → ${END}`);
  } catch (err) {
    console.error(`💥 Unhandled error in legacy indexer: ${err.message}`);
  } finally {
    await targetDB.end();
    process.exit(0);
  }
})();
