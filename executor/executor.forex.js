#!/usr/bin/env node
/**
 * executor.multi.js
 * - Subscribe Supra WS to all PAIRS
 * - For each tick: call /match/entry and /match/exits on your public read API
 * - Execute on CORE: executeOrder / executeStopOrTakeProfit with Supra proof
 * - Wallet rotation: 1 tx/sec per wallet
 */

require("dotenv").config();

const fetch = require("node-fetch");
const http = require("http");
const { WebSocket } = require("ws");
const { ethers } = require("ethers");
const { spawn } = require("child_process");
const path = require("path");

const CORE_ABI = require("./coreAbi");

const VAULT_ABI = [
  {
    inputs: [],
    name: "lpFreeCapital",
    outputs: [{ internalType: "uint256", name: "", type: "uint256" }],
    stateMutability: "view",
    type: "function",
  },
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
const CORE_ADDRESS = process.env.CORE_ADDRESS;
const VAULT_ADDRESS = process.env.VAULT_ADDRESS; 

const READ_BASE = process.env.READ_BASE || "http://127.0.0.1:7000";

const PRIVATE_KEYS = (process.env.PRIVATE_KEYS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const DEDUP_MS = Number(process.env.EXECUTION_DEDUP_MS || 15000);
const LP_FREE_TTL_MS = Number(process.env.LP_FREE_TTL_MS || 1500); 

const WSS_NO_TICK_TIMEOUT_MS = Number(process.env.WSS_NO_TICK_TIMEOUT_MS || 8000);


const PAIRS = [
    "aud_usd", "eur_usd", "gbp_usd", "nzd_usd", "usd_cad", 
    "usd_chf", "usd_jpy", "xag_usd", "xau_usd"
  ];
  
  const PAIR_MAP = {
    5010: "aud_usd",
    5000: "eur_usd",
    5002: "gbp_usd",
    5013: "nzd_usd",
    5011: "usd_cad",
    5012: "usd_chf",
    5001: "usd_jpy",
    5501: "xag_usd",
    5500: "xau_usd"
  };

const REVERSE_MAP = {};
for (const [idStr, pair] of Object.entries(PAIR_MAP)) {
  REVERSE_MAP[pair] = Number(idStr);
}

// --------------------
// HELPERS
// --------------------
const httpAgent = new http.Agent({
  keepAlive: true,
  maxSockets: 100
});

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
    if (inFlight) {
      scheduleFlush();
      return;
    }
    if (pending.size === 0) return;

    inFlight = true;

    try {
      const ids = Array.from(pending);
      pending.clear();

      console.log(`[RESYNC-BATCH] flushing ALL ${ids.length} ids: ${ids.join(",")}`);

      await new Promise((resolve) => {
        const p = spawn(
          "node",
          [SYNC_PATH, "--mode", "full", "--ids", ids.join(",")],
          { stdio: "inherit" }
        );

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

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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
  if (value === null || value === undefined) return null;
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
  if (tick.currentPrice !== undefined && tick.currentPrice !== null) return tick.currentPrice;
  if (tick.close !== undefined && tick.close !== null) return tick.close;
  return null;
}

// --------------------
// MAIN
// --------------------
async function main() {
  if (!SUPRA_API_KEY) throw new Error("Missing SUPRA_API_KEY in .env");
  if (!RPC_URL) throw new Error("Missing RPC_URL in .env");
  if (!CORE_ADDRESS) throw new Error("Missing CORE_ADDRESS in .env");
  if (!VAULT_ADDRESS) throw new Error("Missing VAULT_ADDRESS in .env");
  if (!PRIVATE_KEYS.length) throw new Error("Missing PRIVATE_KEYS in .env");
  if (!READ_BASE) throw new Error("Missing READ_BASE in .env");

  const provider = new ethers.providers.JsonRpcProvider(RPC_URL);
  const vault = new ethers.Contract(VAULT_ADDRESS, VAULT_ABI, provider);

  const walletPool = new WalletPool({
    provider,
    privateKeys: PRIVATE_KEYS,
    perWalletDelayMs: 1000,
  });

  const fetchProof = createProofFetcher({ doraRpc: DORA_RPC, chainType: DORA_CHAIN });
  const resyncBatcher = createResyncBatcher();

  let lpFreeCache = { ts: 0, valueE6: 0n };

  async function getLpFreeCapitalE6() {
    const now = Date.now();
    if (now - lpFreeCache.ts < LP_FREE_TTL_MS) return lpFreeCache.valueE6;

    const v = await vault.lpFreeCapital();
    const bi = BigInt(v.toString());

    lpFreeCache = { ts: now, valueE6: bi };
    return bi;
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

  const recentlySent = new Map();

  async function executeOnchain({ kind, tradeId, assetId }) {
    const key = `${kind}:${tradeId}`;
    const now = Date.now();
    const last = recentlySent.get(key) || 0;
    if (now - last < DEDUP_MS) return;
    recentlySent.set(key, now);

    if (kind === "entry") {
      const locked = await getTradeLockedE6(tradeId);
      if (locked <= 0n) {
        console.log(`[SKIP] tradeId=${tradeId} locked=0 => enqueue resync`);
        resyncBatcher.enqueue(tradeId);
        return; 
      }

      const free = await getLpFreeCapitalE6();

      if (free < locked) {
        console.log(`[SKIP] Not enough LP free capital. tradeId=${tradeId} locked=${locked} free=${free}`);
        return; 
      }
    }

    const wallet = await walletPool.acquire();
    const core = new ethers.Contract(CORE_ADDRESS, CORE_ABI, wallet);

    try {
      const proof = await fetchProof([assetId]);

      if (kind === "entry") {
        const tx = await core.executeOrder(tradeId, proof);
        console.log(`[TX] executeOrder assetId=${assetId} tradeId=${tradeId} from=${wallet.address} hash=${tx.hash}`);
        await tx.wait(1);
        console.log(`[OK] executeOrder tradeId=${tradeId}`);
        return;
      }

      if (kind === "exit") {
        const tx = await core.executeStopOrTakeProfit(tradeId, proof);
        console.log(`[TX] executeStopOrTakeProfit assetId=${assetId} tradeId=${tradeId} from=${wallet.address} hash=${tx.hash}`);
        await tx.wait(1);
        console.log(`[OK] executeStopOrTakeProfit tradeId=${tradeId}`);
        return;
      }

      throw new Error(`Unknown kind ${kind}`);

    } catch (err) {
      console.error(`[EXEC ERROR] kind=${kind} tradeId=${tradeId} assetId=${assetId}`, err.reason || err.message);
      resyncBatcher.enqueue(tradeId);
    }
  }

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

    const ws = new WebSocket(WS_URL, {
      headers: { "x-api-key": SUPRA_API_KEY },
    });

    ws.on("open", () => {
      console.log("[Executor] Supra connected, subscribing to", PAIRS.length, "pairs…");
      lastTickAt = Date.now();
      startWatchdog(ws);

      ws.send(JSON.stringify({
        action: "subscribe",
        channels: [
          {
            name: "ohlc_datafeed",
            resolution: RESOLUTION,
            tradingPairs: PAIRS,
          }
        ],
      }));
    });

    ws.on("message", async (buf) => {
      let msg;
      try { msg = JSON.parse(buf.toString()); }
      catch { return; }

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

        // 🔄 Affichage du prix dans la console sans throttle
        console.log(`[TICK] ${pair.toUpperCase()} : ${marketRaw} (AssetID: ${assetId})`);

        try {
          const entry = await httpGetJson(
            `${READ_BASE}/match/entry?assetId=${assetId}&market=${marketE6}&unit=e6`
          );

          const exits = await httpGetJson(
            `${READ_BASE}/match/exits?assetId=${assetId}&market=${marketE6}&unit=e6`
          );

          const entryIds = [...(entry.limit || []), ...(entry.stop || [])];
          const exitIds = [...(exits.stopLoss || []), ...(exits.takeProfit || [])];

          for (const id of entryIds) {
            executeOnchain({ kind: "entry", tradeId: id, assetId })
              .catch((e) => console.error("[ERR] executeOrder", { assetId, id }, e.message));
            await sleep(5);
          }

          for (const id of exitIds) {
            executeOnchain({ kind: "exit", tradeId: id, assetId })
              .catch((e) => console.error("[ERR] executeStopOrTakeProfit", { assetId, id }, e.message));
            await sleep(5);
          }
        } catch (e) {
          console.error("[Executor] match/exec error:", pair, "assetId=", assetId, e.message);
        }
      }
    });

    ws.on("close", () => {
      if (watchdog) clearInterval(watchdog);
      console.error("[Executor] Supra WS closed.", closedByUs ? "(forced reconnect)" : "");
      console.error("[Executor] Reconnecting in 3s…");
      setTimeout(connectSupra, 3000);
    });

    ws.on("error", (err) => {
      console.error("[Executor] Supra WS error:", err.message || err);
    });
  }

  console.log("[Executor] READY");
  console.log(" - CORE:", CORE_ADDRESS);
  console.log(" - VAULT:", VAULT_ADDRESS);
  console.log(" - READ_BASE:", READ_BASE);
  console.log(" - wallets:", PRIVATE_KEYS.length);
  console.log(" - pairs:", PAIRS.length);

  connectSupra();
}

main().catch((e) => {
  console.error("executor fatal:", e);
  process.exit(1);
});