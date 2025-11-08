// src/tools/check_msgs.js - Debug tool to analyze transaction message types
const axios = require('axios');
const crypto = require('crypto');

// RPC endpoint from environment variable
const RPC = process.env.RPC_ENDPOINTS 
  ? process.env.RPC_ENDPOINTS.split(',')[0].trim()
  : 'http://82.208.20.12:26657';

console.log(`🔧 Check messages using RPC: ${RPC}`);

async function fetchBlock(height) {
  const res = await axios.get(`${RPC}/block?height=${height}`);
  return res.data.result.block;
}

async function fetchTx(hash) {
  const res = await axios.get(`${RPC}/tx?hash=0x${hash}`);
  return res.data.result;
}

(async () => {
  for (let height = 584717; height <= 584720; height++) {
    try {
      const block = await fetchBlock(height);
      const rawTxs = block.data.txs || [];
      console.log(`\nBlock ${height} has ${rawTxs.length} tx(s)`);

      for (const raw of rawTxs) {
        const hash = crypto.createHash('sha256')
          .update(Buffer.from(raw, 'base64'))
          .digest('hex')
          .toUpperCase();

        const tx = await fetchTx(hash);
        const events = tx.tx_result?.events || [];

        // Get all message actions (could be multiple per tx)
        const messageActions = events
          .filter(ev => ev.type === 'message')
          .flatMap(ev => ev.attributes
            .filter(attr => attr.key === 'action')
            .map(attr => attr.value)
          );

        const uniqueActions = [...new Set(messageActions)];

        console.log(` TX ${hash}: message types → ${uniqueActions.join(', ') || 'none'}`);
      }
    } catch (err) {
      console.error(`Error fetching block ${height}: ${err.message}`);
    }
  }
})();
