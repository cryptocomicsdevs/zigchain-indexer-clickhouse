// src/utils/decoder.js

const { Registry } = require('@cosmjs/proto-signing');
const { decodeTxRaw } = require('@cosmjs/proto-signing');
const { fromBase64 } = require('@cosmjs/encoding');
const { defaultRegistryTypes } = require('@cosmjs/stargate');

// Extend this with ZigChain custom types if needed
const registry = new Registry(defaultRegistryTypes);

function decodeTxMessages(rawTxBase64) {
  const txRaw = decodeTxRaw(fromBase64(rawTxBase64));
  const decodedMessages = [];

  for (const msg of txRaw.body.messages) {
    const typeUrl = msg.typeUrl;
    let decoded;

    try {
      decoded = registry.decode(msg);
    } catch (err) {
      console.warn(`⚠️ Unknown message type ${typeUrl}`);
      decoded = { error: 'Unknown message type', raw: msg };
    }

    decodedMessages.push({ typeUrl, decoded });
  }

  return decodedMessages;
}

module.exports = { decodeTxMessages };
