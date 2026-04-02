#!/usr/bin/env node
/**
 * executor.multi.js
 * - Subscribe Supra WS to all PAIRS
 * - For each tick: call /match/entry, /match/exits, and /match/liquidations
 * - Group tasks into BATCHES (Max 50 items or 500ms time-window)
 * - Execute via BrokexBatcher contract with a SINGLE aggregated Supra proof
 * - Intelligent Execution Queue with deduplication
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
const BATCHER_ADDRESS = "0x7e5215cfBF83C5B7737425cC79f072a266e5028B"; // TON BATCHER

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
const { WalletPool } = require("./walletPool");

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
// INTELLIGENT BATCH QUEUE
// --------------------
class BatchExecutionQueue {
  constructor(walletPool, fetchProof, resyncBatcher, getLpFreeCapitalE6, getTradeLockedE6) {
    this.pendingTasks = []; // Panier actuel
    this.executedTradeIds = new Map(); 

    this.walletPool = walletPool;
    this.fetchProof = fetchProof;
    this.resyncBatcher = resyncBatcher;
    this.getLpFreeCapitalE6 = getLpFreeCapitalE6;
    this.getTradeLockedE6 = getTradeLockedE6;

    this.batchTimer = null;
    this.MAX_BATCH_SIZE = 50; 
    this.FLUSH_TIMEOUT_MS = 500; // 500ms d'attente max pour fusionner les ticks
  }

  async enqueue(task) {
    const { kind, tradeId, assetId } = task;

    // 1. Anti-spam: Ignore si déjà exécuté récemment
    if (this.executedTradeIds.has(tradeId)) return;

    try {
      // 2. Vérifications de capital pour les "entry"
      if (kind === "entry") {
        const locked = await this.getTradeLockedE6(tradeId);
        if (locked <= 0n) {
          this.resyncBatcher.enqueue(tradeId);
          return; 
        }

        const free = await this.getLpFreeCapitalE6();
        if (free < locked) return; 
      }

      // 3. On l'ajoute au panier et on le marque comme traité pour éviter les doublons instantanés
      this.executedTradeIds.set(tradeId, Date.now());
      this.pendingTasks.push(task);

      // 4. Logique de déclenchement du Batch
      if (this.pendingTasks.length >= this.MAX_BATCH_SIZE) {
        this.flushBatch(); // Panier plein (50) -> Envoi immédiat !
      } else if (!this.batchTimer) {
        // Premier objet dans le panier : on lance le chrono de 500ms
        this.batchTimer = setTimeout(() => this.flushBatch(), this.FLUSH_TIMEOUT_MS);
      }

    } catch (err) {
      console.error(`[QUEUE ERR] Setup failed for tradeId=${tradeId}`, err.message);
      this.executedTradeIds.delete(tradeId);
    }
  }

  async flushBatch() {
    // Nettoyer le chrono
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }

    if (this.pendingTasks.length === 0) return;

    // Isoler le batch actuel (jusqu'à 50) et vider la queue pour les prochains ticks
    const batch = this.pendingTasks.splice(0, this.MAX_BATCH_SIZE);

    try {
      // Extraire TOUS les assetIds uniques de ce lot pour la preuve Supra
      const uniqueAssetIds = [...new Set(batch.map(t => t.assetId))];
      
      console.log(`[BATCH PREPARE] Grouping ${batch.length} actions across ${uniqueAssetIds.length} assets...`);
      
      // Fetch 1 seule preuve Supra pour tout le monde
      const proof = await this.fetchProof(uniqueAssetIds);

      // Préparer les données pour le contrat Batcher
      const actions = [];
      const tradeIds = [];

      for (const task of batch) {
        tradeIds.push(task.tradeId);
        if (task.kind === "entry") actions.push(0);
        else if (task.kind === "liquidation") actions.push(1);
        else if (task.kind === "exit") actions.push(2);
      }

      // Attente intelligente d'un wallet
      const wallet = await this.walletPool.acquire();

      this.executeOnChain(actions, tradeIds, proof, wallet, batch).catch(e => {
        console.error(`[BATCH EXEC ERR]`, e.reason || e.message);
        // Si échec global, on supprime de l'historique et on force la synchro
        for (const task of batch) {
          this.resyncBatcher.enqueue(task.tradeId);
          this.executedTradeIds.delete(task.tradeId); 
        }
      });

    } catch (err) {
      console.error("[BATCH FLUSH ERR]", err);
    }
    
    // Nettoyage de la RAM pour les vieux trades
    this.cleanUpMemory();
  }

  async executeOnChain(actions, tradeIds, proof, wallet, batchData) {
    const batcherContract = new ethers.Contract(BATCHER_ADDRESS, BATCHER_ABI, wallet);

    console.log(`[BATCH TX SENT] Sending ${actions.length} ops via ${wallet.address}...`);
    
    // Appel de la fonction executeBatch du Smart Contract
    const tx = await batcherContract.executeBatch(actions, tradeIds, proof);
    await tx.wait(1);

    console.log(`[BATCH TX MINED] ${actions.length} ops processed! Hash: ${tx.hash}`);
  }

  cleanUpMemory() {
    const now = Date.now();
    for (const [id, ts] of this.executedTradeIds.entries()) {
      if (now - ts > 120_000) { // Nettoyage après 2 minutes
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

  const walletPool = new WalletPool({
    provider,
    privateKeys: PRIVATE_KEYS,
    perWalletDelayMs: 3000, 
  });

  const fetchProof = createProofFetcher({ doraRpc: DORA_RPC, chainType: DORA_CHAIN });
  const resyncBatcher = createResyncBatcher();

  let lpFreeCache = { ts: 0, valueE6: 0n, fetchPromise: null };
  
  async function getLpFreeCapitalE6() {
    const now = Date.now();
    
    // 1. Si le cache est valide, on retourne la valeur immédiatement
    if (now - lpFreeCache.ts < LP_FREE_TTL_MS && lpFreeCache.valueE6 > 0n) {
      return lpFreeCache.valueE6;
    }

    // 2. Si une requête réseau est DÉJÀ en cours, on ne spamme pas le RPC !
    // On attend simplement que la requête existante se termine.
    if (lpFreeCache.fetchPromise) {
      return await lpFreeCache.fetchPromise;
    }

    // 3. Sinon, on est le premier. On lance la requête et on la stocke
    // pour que les autres puissent l'attendre.
    lpFreeCache.fetchPromise = vault.lpFreeCapital().then(v => {
      const bi = BigInt(v.toString());
      // On met à jour le cache et on libère le verrou
      lpFreeCache = { ts: Date.now(), valueE6: bi, fetchPromise: null };
      return bi;
    }).catch(err => {
      // En cas d'erreur du RPC, on libère le verrou pour pouvoir réessayer
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

  // Initialisation de la NOUVELLE file d'attente BATCH
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

        // Optionnel: Décommente la ligne suivante si tu veux voir tous les ticks (ça spam beaucoup)
        // console.log(`[TICK] ${pair.toUpperCase()} : ${marketRaw} (AssetID: ${assetId})`);

        try {
          const entry = await httpGetJson(`${READ_BASE}/match/entry?assetId=${assetId}&market=${marketE6}&unit=e6`);
          const exits = await httpGetJson(`${READ_BASE}/match/exits?assetId=${assetId}&market=${marketE6}&unit=e6`);
          const liqs = await httpGetJson(`${READ_BASE}/match/liquidations?assetId=${assetId}&market=${marketE6}&unit=e6`);

          const entryIds = [...(entry.limit || []), ...(entry.stop || [])];
          const exitIds = [...(exits.stopLoss || []), ...(exits.takeProfit || [])];
          const liqIds = liqs.liquidations || [];

          // Envoi dans le panier. Le panier s'occupe de grouper les appels !
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