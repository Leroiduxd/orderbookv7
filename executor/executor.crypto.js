#!/usr/bin/env node
/**
 * executor.multi.js
 * - 1 BATCH = 1 ASSET UNIQUE (Préserve le backend Supra)
 * - Verrouillage strict des Wallets (Wait Mined + 3s de cooldown)
 */

require("dotenv").config();

const fetch = require("node-fetch");
const http = require("http");
const { WebSocket } = require("ws");
const { ethers } = require("ethers");
const { spawn } = require("child_process");
const path = require("path");

// --------------------
// SMART CONTRACT ABIs & ADDRESSES
// --------------------
const CORE_ADDRESS = process.env.CORE_ADDRESS;
const VAULT_ADDRESS = process.env.VAULT_ADDRESS; 
const BATCHER_ADDRESS = "0x7e5215cfBF83C5B7737425cC79f072a266e5028B";

const VAULT_ABI = [
  {
    inputs: [],
    name: "lpFreeCapital",
    outputs: [{ internalType: "uint256", name: "", type: "uint256" }],
    stateMutability: "view",
    type: "function",
  },
];

const BATCHER_ABI = [
  {
    "inputs": [
      { "internalType": "uint8[]", "name": "actions", "type": "uint8[]" },
      { "internalType": "uint256[]", "name": "tradeIds", "type": "uint256[]" },
      { "internalType": "bytes", "name": "oracleProof", "type": "bytes" }
    ],
    "name": "executeBatch",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  }
];

const { createProofFetcher } = require("./proofClient");

// --------------------
// WALLET POOL STRICT (Wait Mined + Cooldown)
// --------------------
class StrictWalletPool {
  constructor(provider, privateKeys, cooldownMs) {
    this.cooldownMs = cooldownMs;
    this.wallets = privateKeys.map(pk => ({
      instance: new ethers.Wallet(pk, provider),
      isBusy: false,
      availableAt: 0
    }));
  }

  async acquire() {
    while (true) {
      const now = Date.now();
      for (const w of this.wallets) {
        if (!w.isBusy && now >= w.availableAt) {
          w.isBusy = true; // VERROUILLAGE
          return w; 
        }
      }
      // Attend 100ms avant de revérifier si un wallet est libre
      await new Promise(r => setTimeout(r, 100));
    }
  }

  release(walletWrapper) {
    walletWrapper.availableAt = Date.now() + this.cooldownMs;
    walletWrapper.isBusy = false; // DÉVERROUILLAGE
  }
}

// --------------------
// CONFIG
// --------------------
const SUPRA_API_KEY = process.env.SUPRA_API_KEY;
const WS_URL = "wss://prod-kline-ws.supra.com";
const RESOLUTION = 1;

const DORA_RPC = process.env.DORA_RPC || "https://rpc-testnet-dora-2.supra.com";
const DORA_CHAIN = process.env.DORA_CHAIN || "evm";

const RPC_URL = process.env.RPC_URL;
const READ_BASE = process.env.READ_BASE || "http://127.0.0.1:7000";

const PRIVATE_KEYS = (process.env.PRIVATE_KEYS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const LP_FREE_TTL_MS = Number(process.env.LP_FREE_TTL_MS || 1500); 
const WSS_NO_TICK_TIMEOUT_MS = Number(process.env.WSS_NO_TICK_TIMEOUT_MS || 8000);

// --------------------
// SUPRA PAIRS + MAPS
// --------------------
const PAIRS = [
  "btc_usdt", "eth_usdt", "sol_usdt", "xrp_usdt", "avax_usdt", 
  "doge_usdt", "trx_usdt", "ada_usdt", "sui_usdt", "link_usdt"
];

const PAIR_MAP = {
  0: "btc_usdt", 1: "eth_usdt", 10: "sol_usdt", 14: "xrp_usdt",
  5: "avax_usdt", 3: "doge_usdt", 15: "trx_usdt", 16: "ada_usdt",
  90: "sui_usdt", 2: "link_usdt"
};

const REVERSE_MAP = {};
for (const [idStr, pair] of Object.entries(PAIR_MAP)) {
  REVERSE_MAP[pair] = Number(idStr);
}

// --------------------
// HELPERS
// --------------------
const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 100 });
const SYNC_PATH = path.resolve(__dirname, "../sync.js");
const RESYNC_FLUSH_MS = Number(process.env.RESYNC_FLUSH_MS || 1000); 

function createResyncBatcher() {
  const pending = new Set();
  let timer = null;
  let inFlight = false;

  function scheduleFlush() {
    if (timer) return;
    timer = setTimeout(async () => {
      timer = null;
      await flush();
    }, RESYNC_FLUSH_MS);
  }

  async function flush() {
    if (inFlight) { scheduleFlush(); return; }
    if (pending.size === 0) return;

    inFlight = true;
    try {
      const ids = Array.from(pending);
      pending.clear();
      console.log(`[RESYNC-BATCH] flushing ALL ${ids.length} ids: ${ids.join(",")}`);

      await new Promise((resolve) => {
        const p = spawn("node", [SYNC_PATH, "--mode", "full", "--ids", ids.join(",")], { stdio: "inherit" });
        p.on("close", (code) => {
          console.log(`[RESYNC-BATCH] done (code=${code}) ids=${ids.length}`);
          resolve();
        });
      });
    } finally {
      inFlight = false;
      if (pending.size > 0) scheduleFlush();
    }
  }

  function enqueue(tradeId) {
    if (!Number.isFinite(tradeId) || tradeId <= 0) return;
    pending.add(Number(tradeId));
    scheduleFlush();
  }

  return { enqueue, flush };
}

async function httpGetJson(url) {
  const res = await fetch(url, { agent: httpAgent });
  const txt = await res.text();
  let data;
  try { data = txt ? JSON.parse(txt) : null; }
  catch { data = { raw: txt }; }
  if (!res.ok) throw new Error(data?.error || data?.raw || `HTTP ${res.status}`);
  return data;
}

function decimalToE6(value) {
  if (value == null) return null;
  const s0 = typeof value === "string" ? value : String(value);
  const s = s0.trim();
  if (!s) return null;

  let neg = false;
  let t = s;
  if (t.startsWith("-")) { neg = true; t = t.slice(1); }

  const parts = t.split(".");
  const intPart = parts[0] ? parts[0].replace(/^0+(?=\d)/, "") : "0";
  const fracRaw = (parts[1] || "");

  const fracPadded = (fracRaw + "0000000").slice(0, 7);
  const frac6 = fracPadded.slice(0, 6);
  const d7 = fracPadded[6] ? Number(fracPadded[6]) : 0;

  let bi = BigInt(intPart || "0") * 1000000n + BigInt(frac6 || "0");
  if (d7 >= 5) bi += 1n;

  if (neg) bi = -bi;
  return Number(bi);
}

function pickMarketFromTick(tick) {
  if (tick.currentPrice != null) return tick.currentPrice;
  if (tick.close != null) return tick.close;
  return null;
}

// --------------------
// INTELLIGENT BATCH QUEUE (1 ASSET PER BATCH)
// --------------------
class BatchExecutionQueue {
  constructor(walletPool, fetchProof, resyncBatcher, getLpFreeCapitalE6, getTradeLockedE6) {
    this.pendingTasksByAsset = {}; // Trie les tâches par AssetId
    this.executedTradeIds = new Map(); 

    this.walletPool = walletPool;
    this.fetchProof = fetchProof;
    this.resyncBatcher = resyncBatcher;
    this.getLpFreeCapitalE6 = getLpFreeCapitalE6;
    this.getTradeLockedE6 = getTradeLockedE6;

    this.batchTimer = null;
    this.MAX_BATCH_SIZE = 50; 
    this.FLUSH_TIMEOUT_MS = 500; 
  }

  async enqueue(task) {
    const { kind, tradeId, assetId } = task;

    if (this.executedTradeIds.has(tradeId)) return;

    try {
      if (kind === "entry") {
        const locked = await this.getTradeLockedE6(tradeId);
        if (locked <= 0n) {
          this.resyncBatcher.enqueue(tradeId);
          return; 
        }

        const free = await this.getLpFreeCapitalE6();
        if (free < locked) return; 
      }

      this.executedTradeIds.set(tradeId, Date.now());

      // Initialise le tableau pour cet actif s'il n'existe pas
      if (!this.pendingTasksByAsset[assetId]) {
        this.pendingTasksByAsset[assetId] = [];
      }
      this.pendingTasksByAsset[assetId].push(task);

      // Si le panier d'un actif atteint 50, on envoie SEULEMENT cet actif
      if (this.pendingTasksByAsset[assetId].length >= this.MAX_BATCH_SIZE) {
        this.flushAsset(assetId); 
      } else if (!this.batchTimer) {
        this.batchTimer = setTimeout(() => this.flushAll(), this.FLUSH_TIMEOUT_MS);
      }

    } catch (err) {
      console.error(`[QUEUE ERR] Setup failed for tradeId=${tradeId}`, err.message);
      this.executedTradeIds.delete(tradeId);
    }
  }

  // Lancé par le Timer : On vide tous les paniers un par un
  async flushAll() {
    this.batchTimer = null;
    for (const assetId of Object.keys(this.pendingTasksByAsset)) {
      if (this.pendingTasksByAsset[assetId] && this.pendingTasksByAsset[assetId].length > 0) {
        this.flushAsset(Number(assetId));
      }
    }
  }

  // Envoie les tâches pour UN SEUL actif
  async flushAsset(assetId) {
    const batch = this.pendingTasksByAsset[assetId].splice(0, this.MAX_BATCH_SIZE);
    if (batch.length === 0) return;

    try {
      console.log(`[BATCH PREPARE] Grouping ${batch.length} actions for Asset ${assetId}...`);
      
      let proof;
      try {
        // Demande une preuve pour 1 SEUL actif (Très rapide pour Supra)
        proof = await this.fetchProof([assetId]);
      } catch (supraErr) {
        console.error(`[SUPRA API DOWN] Skipped batch for Asset ${assetId}.`);
        for (const task of batch) this.executedTradeIds.delete(task.tradeId); 
        return; 
      }

      const actions = [];
      const tradeIds = [];

      for (const task of batch) {
        tradeIds.push(task.tradeId);
        if (task.kind === "entry") actions.push(0);
        else if (task.kind === "liquidation") actions.push(1);
        else if (task.kind === "exit") actions.push(2);
      }

      // Attente intelligente d'un wallet LIBRE
      const walletWrapper = await this.walletPool.acquire();

      // On lance la transaction en arrière-plan
      this.executeOnChain(actions, tradeIds, proof, walletWrapper, batch);

    } catch (err) {
      console.error("[BATCH FLUSH ERR]", err);
    }
    
    this.cleanUpMemory();
  }

  async executeOnChain(actions, tradeIds, proof, walletWrapper, batchData) {
    try {
      const batcherContract = new ethers.Contract(BATCHER_ADDRESS, BATCHER_ABI, walletWrapper.instance);

      console.log(`[BATCH TX SENT] Sending ${actions.length} ops via ${walletWrapper.instance.address}...`);
      
      const txParams = { gasLimit: 8000000 };
      const tx = await batcherContract.executeBatch(actions, tradeIds, proof, txParams);
      
      // ON ATTEND QUE LA TRANSACTION SOIT MINÉE DANS LE BLOC
      await tx.wait(1);

      console.log(`[BATCH TX MINED] ${actions.length} ops via ${walletWrapper.instance.address} !`);

    } catch (e) {
      console.error(`[BATCH EXEC ERR]`, e.reason || e.message);
      for (const task of batchData) {
        this.resyncBatcher.enqueue(task.tradeId);
        this.executedTradeIds.delete(task.tradeId); 
      }
    } finally {
      // DÉVERROUILLAGE DU WALLET (Il sera disponible dans 3 secondes)
      this.walletPool.release(walletWrapper);
    }
  }

  cleanUpMemory() {
    const now = Date.now();
    for (const [id, ts] of this.executedTradeIds.entries()) {
      if (now - ts > 120_000) { 
        this.executedTradeIds.delete(id);
      }
    }
  }
}

// --------------------
// MAIN
// --------------------
async function main() {
  if (!SUPRA_API_KEY) throw new Error("Missing SUPRA_API_KEY");
  if (!RPC_URL) throw new Error("Missing RPC_URL");
  if (!CORE_ADDRESS) throw new Error("Missing CORE_ADDRESS");
  if (!VAULT_ADDRESS) throw new Error("Missing VAULT_ADDRESS");
  if (!PRIVATE_KEYS.length) throw new Error("Missing PRIVATE_KEYS");
  if (!READ_BASE) throw new Error("Missing READ_BASE");

  const provider = new ethers.providers.JsonRpcProvider(RPC_URL);
  const vault = new ethers.Contract(VAULT_ADDRESS, VAULT_ABI, provider);

  // Instanciation de notre nouvelle Pool stricte (3000ms = 3 secondes)
  const walletPool = new StrictWalletPool(provider, PRIVATE_KEYS, 3000);

  const fetchProof = createProofFetcher({ doraRpc: DORA_RPC, chainType: DORA_CHAIN });
  const resyncBatcher = createResyncBatcher();

  // Cache anti-spam pour le RPC
  let lpFreeCache = { ts: 0, valueE6: 0n, fetchPromise: null };
  async function getLpFreeCapitalE6() {
    const now = Date.now();
    if (now - lpFreeCache.ts < LP_FREE_TTL_MS && lpFreeCache.valueE6 > 0n) return lpFreeCache.valueE6;
    if (lpFreeCache.fetchPromise) return await lpFreeCache.fetchPromise;

    lpFreeCache.fetchPromise = vault.lpFreeCapital().then(v => {
      const bi = BigInt(v.toString());
      lpFreeCache = { ts: Date.now(), valueE6: bi, fetchPromise: null };
      return bi;
    }).catch(err => {
      lpFreeCache.fetchPromise = null;
      throw err;
    });
    return await lpFreeCache.fetchPromise;
  }

  const lockedCache = new Map();
  async function getTradeLockedE6(tradeId) {
    const hit = lockedCache.get(tradeId);
    if (hit !== undefined) return hit;
    const t = await httpGetJson(`${READ_BASE}/trade/${tradeId}`);
    const locked = BigInt(String(t.lpLockedCapital ?? "0"));
    lockedCache.set(tradeId, locked);
    return locked;
  }

  const execQueue = new BatchExecutionQueue(
    walletPool, fetchProof, resyncBatcher, getLpFreeCapitalE6, getTradeLockedE6
  );

  function connectSupra() {
    console.log("[Executor] Connecting Supra WS:", WS_URL);
    let closedByUs = false;
    let lastTickAt = Date.now();
    let watchdog = null;

    function startWatchdog(ws) {
      if (watchdog) clearInterval(watchdog);
      watchdog = setInterval(() => {
        const now = Date.now();
        if (now - lastTickAt > WSS_NO_TICK_TIMEOUT_MS) {
          console.error(`[Executor] No ticks for ${now - lastTickAt}ms. Reconnecting...`);
          closedByUs = true;
          try { ws.terminate(); } catch {}
        }
      }, 1000);
    }

    const ws = new WebSocket(WS_URL, { headers: { "x-api-key": SUPRA_API_KEY } });

    ws.on("open", () => {
      console.log("[Executor] Supra connected, subscribing to", PAIRS.length, "pairs…");
      lastTickAt = Date.now();
      startWatchdog(ws);

      ws.send(JSON.stringify({
        action: "subscribe",
        channels: [{ name: "ohlc_datafeed", resolution: RESOLUTION, tradingPairs: PAIRS }],
      }));
    });

    ws.on("message", async (buf) => {
      let msg;
      try { msg = JSON.parse(buf.toString()); } catch { return; }
      if (msg.event !== "ohlc_datafeed" || !Array.isArray(msg.payload)) return;

      lastTickAt = Date.now();

      for (const tick of msg.payload) {
        const pair = tick.tradingPair;
        if (!pair) continue;
        const assetId = REVERSE_MAP[pair];
        if (assetId === undefined) continue;

        const marketRaw = pickMarketFromTick(tick);
        const marketE6 = decimalToE6(marketRaw);
        if (marketE6 === null) continue;

        try {
          const entry = await httpGetJson(`${READ_BASE}/match/entry?assetId=${assetId}&market=${marketE6}&unit=e6`);
          const exits = await httpGetJson(`${READ_BASE}/match/exits?assetId=${assetId}&market=${marketE6}&unit=e6`);
          const liqs = await httpGetJson(`${READ_BASE}/match/liquidations?assetId=${assetId}&market=${marketE6}&unit=e6`);

          const entryIds = [...(entry.limit || []), ...(entry.stop || [])];
          const exitIds = [...(exits.stopLoss || []), ...(exits.takeProfit || [])];
          const liqIds = liqs.liquidations || [];

          for (const id of entryIds) execQueue.enqueue({ kind: "entry", tradeId: id, assetId });
          for (const id of exitIds) execQueue.enqueue({ kind: "exit", tradeId: id, assetId });
          for (const id of liqIds) execQueue.enqueue({ kind: "liquidation", tradeId: id, assetId });

        } catch (e) {
          console.error("[Executor] match API error:", pair, "assetId=", assetId, e.message);
        }
      }
    });

    ws.on("close", () => {
      if (watchdog) clearInterval(watchdog);
      console.error("[Executor] Supra WS closed.", closedByUs ? "(forced reconnect)" : "");
      console.error("[Executor] Reconnecting in 3s…");
      setTimeout(connectSupra, 3000);
    });

    ws.on("error", (err) => console.error("[Executor] Supra WS error:", err.message || err));
  }

  console.log("[Executor] READY");
  console.log(" - BATCHER:", BATCHER_ADDRESS);
  console.log(" - CORE:", CORE_ADDRESS);
  console.log(" - wallets:", PRIVATE_KEYS.length);
  connectSupra();
}

main().catch((e) => { console.error("executor fatal:", e); process.exit(1); });