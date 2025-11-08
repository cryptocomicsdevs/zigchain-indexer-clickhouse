// src/utils/rpc.js

const axios = require('axios');
const http = require('http');
const https = require('https');

const RPC = 'http://82.208.20.12:26657'; // Your RPC

const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 50 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 50 });

const rpcClient = axios.create({
  baseURL: RPC,
  timeout: 10000,
  httpAgent,
  httpsAgent,
});

async function fetchBlock(height) {
  const res = await rpcClient.get(`/block?height=${height}`);
  return res.data.result.block;
}

async function fetchBlockResults(height) {
  const res = await rpcClient.get(`/block_results?height=${height}`);
  return res.data.result;
}

async function getStatus() {
  const res = await rpcClient.get(`/status`);
  return res.data.result;
}

module.exports = {
  fetchBlock,
  fetchBlockResults,
  getStatus,
};
