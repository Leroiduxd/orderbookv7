// executor/proofClient.js (CommonJS)
const fetch = require("node-fetch");

class PullServiceClient {
  constructor(address) {
    this.address = String(address).replace(/\/+$/, "");
    this.timeoutMs = 12_000;
  }

  async _post(url, body) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), this.timeoutMs);
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
        signal: ctrl.signal,
      });

      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status} @ ${url} :: ${text.slice(0, 200)}`);
      }
      return await res.json();
    } finally {
      clearTimeout(t);
    }
  }

  async getProof({ pair_indexes, chain_type }) {
    if (!Array.isArray(pair_indexes) || pair_indexes.length === 0) {
      throw new Error("pair_indexes must be a non-empty array");
    }
    const chain = chain_type || "evm";

    const endpoints = [
      {
        url: `${this.address}`,
        body: { id: 1, jsonrpc: "2.0", method: "get_proof", params: { pair_indexes, chain_type: chain } },
        pick: (j) => j?.result?.proof_bytes || j?.result?.proofBytes || j?.proof_bytes || j?.proofBytes,
      },
      {
        url: `${this.address}/rpc`,
        body: { id: 1, jsonrpc: "2.0", method: "get_proof", params: { pair_indexes, chain_type: chain } },
        pick: (j) => j?.result?.proof_bytes || j?.result?.proofBytes || j?.proof_bytes || j?.proofBytes,
      },
      {
        url: `${this.address}/v2/pull/get_proof`,
        body: { pair_indexes, chain_type: chain },
        pick: (j) => j?.proof_bytes || j?.proofBytes || j?.data?.proof_bytes || j?.data?.proofBytes,
      },
      {
        url: `${this.address}/pull-service/get_proof`,
        body: { pair_indexes, chain_type: chain },
        pick: (j) => j?.proof_bytes || j?.proofBytes || j?.data?.proof_bytes || j?.data?.proofBytes,
      },
      {
        url: `${this.address}/get_proof`,
        body: { pair_indexes, chain_type: chain },
        pick: (j) => j?.proof_bytes || j?.proofBytes || j?.data?.proof_bytes || j?.data?.proofBytes,
      },
    ];

    let lastErr;
    for (const cand of endpoints) {
      try {
        const json = await this._post(cand.url, cand.body);
        const proof = cand.pick(json) || json?.data?.proof_bytes;
        if (proof) return { proof_bytes: String(proof) };
        lastErr = new Error(`No proof_bytes in response from ${cand.url}`);
      } catch (e) {
        lastErr = e;
      }
    }
    throw lastErr || new Error("Unable to fetch proof");
  }
}

// cache 1s
const cache = new Map(); // key => {proof, ts}

function makeKey(pairs) {
  return [...pairs].sort((a, b) => a - b).join(",");
}

function normalizeProof(p) {
  const s = String(p);
  return s.startsWith("0x") ? s : "0x" + s;
}

function createProofFetcher({ doraRpc, chainType }) {
  const client = new PullServiceClient(doraRpc);

  return async function fetchProof(pairIndexes) {
    const key = makeKey(pairIndexes);
    const now = Date.now();
    const hit = cache.get(key);
    if (hit && now - hit.ts < 1000) return hit.proof;

    const data = await client.getProof({ pair_indexes: pairIndexes, chain_type: chainType });
    const proof = normalizeProof(data.proof_bytes);
    cache.set(key, { proof, ts: now });
    return proof;
  };
}

module.exports = { createProofFetcher };