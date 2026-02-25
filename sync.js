#!/usr/bin/env node
/**
 * sync.js
 *
 * Modes:
 * - full   : fetch full trades via getTradesFromList + batchUpsert
 * - sltp   : fetch SL/TP via getSLTPFromList + batchPatchSLTP
 * - states : fetch states via getStatesFromList + batchPatchStates
 * If a trade transitions to Closed(2) or Cancelled(3) => full fetch for that id
 *
 * Logic:
 * - Always read nextTradeID() from CORE to know max existing id onchain
 * - If id <= maxExistingId but missing in DB => full fetch (not sltp/states)
 *
 * Usage examples:
 * node sync.js --mode sltp --ids 1,2,3,4
 * node sync.js --mode states --range 0 5000
 * node sync.js --mode full --missing-scan 0 40000   (optional helper)
 */

const Database = require("better-sqlite3");
const pLimit = require("p-limit");

// --------------------
// CONFIG (create a config.js next to this file)
// --------------------
// module.exports = {
//   RPC_URL: "https://...",
//   CORE_ADDRESS: "0x...",
//   PAYMASTER_ADDRESS: "0x...", // the contract that has getTradesFromList/getSLTPFromList/getStatesFromList
//   DB_PATH: "trades.db",
//   WRITE_BASE: "http://127.0.0.1:3001"
// };
const cfg = require("./config");

const ethersPkg = require("ethers");
const ethers = ethersPkg.ethers ?? ethersPkg; // v6 -> ethersPkg.ethers, v5 -> ethersPkg

// --------------------
// Tunables
// --------------------
const BATCH_SIZE = 50;
const RPC_CONCURRENCY = 20;
const HTTP_TIMEOUT_MS = 30_000;

// --------------------
// Minimal ABIs
// --------------------
const CORE_ABI = [
  {
    inputs: [],
    name: "nextTradeID",
    outputs: [{ internalType: "uint256", name: "", type: "uint256" }],
    stateMutability: "view",
    type: "function",
  },
];

const PAYMASTER_ABI = [
  // getTradesFromList(uint256[])
  {
    inputs: [{ internalType: "uint256[]", name: "tradeIds", type: "uint256[]" }],
    name: "getTradesFromList",
    outputs: [
      {
        components: [
          { internalType: "address", name: "trader", type: "address" },
          { internalType: "uint32", name: "assetId", type: "uint32" },
          { internalType: "bool", name: "isLong", type: "bool" },
          { internalType: "bool", name: "isLimit", type: "bool" },
          { internalType: "uint8", name: "leverage", type: "uint8" },
          { internalType: "uint48", name: "openPrice", type: "uint48" },
          { internalType: "uint8", name: "state", type: "uint8" },
          { internalType: "uint32", name: "openTimestamp", type: "uint32" },
          { internalType: "uint128", name: "fundingIndex", type: "uint128" },
          { internalType: "uint48", name: "closePrice", type: "uint48" },
          { internalType: "int32", name: "lotSize", type: "int32" },
          { internalType: "int32", name: "closedLotSize", type: "int32" },
          { internalType: "uint48", name: "stopLoss", type: "uint48" },
          { internalType: "uint48", name: "takeProfit", type: "uint48" },
          { internalType: "uint64", name: "lpLockedCapital", type: "uint64" },
          { internalType: "uint64", name: "marginUsdc", type: "uint64" },
        ],
        internalType: "struct IBrokexCore.Trade[]",
        name: "fetchedTrades",
        type: "tuple[]",
      },
    ],
    stateMutability: "view",
    type: "function",
  },

  // getSLTPFromList(uint256[]) returns (uint48[], uint48[])
  {
    inputs: [{ internalType: "uint256[]", name: "tradeIds", type: "uint256[]" }],
    name: "getSLTPFromList",
    outputs: [
      { internalType: "uint48[]", name: "stopLosses", type: "uint48[]" },
      { internalType: "uint48[]", name: "takeProfits", type: "uint48[]" },
    ],
    stateMutability: "view",
    type: "function",
  },

  // getStatesFromList(uint256[]) returns (uint8[])
  // ⚠️ Ajuste si ton ABI exact est différent (nom / outputs)
  {
    inputs: [{ internalType: "uint256[]", name: "tradeIds", type: "uint256[]" }],
    name: "getTradeStatesFromList",
    outputs: [{ internalType: "uint8[]", name: "states", type: "uint8[]" }],
    stateMutability: "view",
    type: "function",
    },
];

// --------------------
// Helpers
// --------------------
function parseArgs(argv) {
  const out = { mode: null, ids: null, range: null, missingScan: null };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--mode") out.mode = argv[++i];
    else if (a === "--ids") out.ids = argv[++i];
    else if (a === "--range") out.range = [Number(argv[++i]), Number(argv[++i])];
    else if (a === "--missing-scan") out.missingScan = [Number(argv[++i]), Number(argv[++i])];
  }
  return out;
}

function chunk(arr, size) {
  const res = [];
  for (let i = 0; i < arr.length; i += size) res.push(arr.slice(i, i + size));
  return res;
}

function toIntSafeBN(x) {
  // ethers v6 returns BigInt
  if (typeof x === "bigint") return x;
  // v5 returns BigNumber
  if (x && typeof x.toString === "function") return BigInt(x.toString());
  return BigInt(x);
}

function normalizeAddr(a) {
  return String(a).toLowerCase();
}

async function httpJson(url, { method = "GET", body = null } = {}) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), HTTP_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method,
      headers: body ? { "Content-Type": "application/json" } : undefined,
      body: body ? JSON.stringify(body) : undefined,
      signal: ctrl.signal,
    });
    const txt = await res.text();
    let data = null;
    try {
      data = txt ? JSON.parse(txt) : null;
    } catch {
      data = { raw: txt };
    }
    if (!res.ok) {
      const msg = data?.error || data?.raw || `HTTP ${res.status}`;
      const e = new Error(msg);
      e.status = res.status;
      throw e;
    }
    return data;
  } finally {
    clearTimeout(t);
  }
}

// --------------------
// DB read-only access (fast existence + compare)
// --------------------
function openDbReadOnly() {
  const path = cfg.DB_PATH || "trades.db";
  // read-only prevents accidental writes and reduces lock issues
  return new Database(path, { readonly: true, fileMustExist: true });
}

function buildDbReaders(dbRO) {
  const getRow = dbRO.prepare(`SELECT id, state, stopLoss, takeProfit FROM trades WHERE id = ?;`);
  const exists = dbRO.prepare(`SELECT 1 FROM trades WHERE id = ?;`);

  return {
    getRow: (id) => getRow.get(id),
    exists: (id) => !!exists.get(id),
  };
}

// --------------------
// Mapping onchain trade -> DB payload (E6 already onchain for your case)
// --------------------
function tradeToPayload(id, t) {
  return {
    id: Number(id),
    trader: normalizeAddr(t.trader),
    assetId: Number(t.assetId),
    isLong: t.isLong ? 1 : 0,
    isLimit: t.isLimit ? 1 : 0,
    leverage: t.leverage === undefined || t.leverage === null ? null : Number(t.leverage),

    openPrice: t.openPrice === undefined || t.openPrice === null ? null : Number(t.openPrice),
    state: Number(t.state),
    openTimestamp: t.openTimestamp === undefined || t.openTimestamp === null ? null : Number(t.openTimestamp),
    fundingIndex: t.fundingIndex === undefined || t.fundingIndex === null ? null : String(t.fundingIndex),

    closePrice: t.closePrice ? Number(t.closePrice) : 0,
    lotSize: t.lotSize === undefined || t.lotSize === null ? null : Number(t.lotSize),
    closedLotSize: t.closedLotSize ? Number(t.closedLotSize) : 0,

    stopLoss: t.stopLoss ? Number(t.stopLoss) : 0,
    takeProfit: t.takeProfit ? Number(t.takeProfit) : 0,

    lpLockedCapital: t.lpLockedCapital === undefined || t.lpLockedCapital === null ? null : String(t.lpLockedCapital),
    marginUsdc: t.marginUsdc === undefined || t.marginUsdc === null ? null : String(t.marginUsdc),
  };
}

// --------------------
// Core sync actions
// --------------------
async function syncFull({ paymaster, ids, writeBase }) {
  if (ids.length === 0) return { upserted: 0 };

  const trades = await paymaster.getTradesFromList(ids);
  // returns tuple[] in ethers: array of structs
  const payloads = ids.map((id, i) => tradeToPayload(id, trades[i]));

  // Use batchUpsert (fast)
  const r = await httpJson(`${writeBase}/trades/batchUpsert`, {
    method: "POST",
    body: { trades: payloads },
  });

  return { upserted: r.upserted ?? payloads.length };
}

async function syncSLTP({ paymaster, ids, writeBase, dbReaders, maxExistingId }) {
  // If missing in DB but exists onchain => full fetch
  const missing = [];
  const present = [];

  for (const id of ids) {
    if (id > maxExistingId) continue;
    if (!dbReaders.exists(id)) missing.push(id);
    else present.push(id);
  }

  let upsertedMissing = 0;
  if (missing.length) {
    const r = await syncFull({ paymaster, ids: missing, writeBase });
    upsertedMissing += r.upserted;
  }

  if (!present.length) return { patched: 0, upsertedMissing };

  const [sls, tps] = await paymaster.getSLTPFromList(present);

  const patches = [];
  for (let i = 0; i < present.length; i++) {
    const id = present[i];
    const sl = Number(sls[i] ?? 0);
    const tp = Number(tps[i] ?? 0);

    const row = dbReaders.getRow(id);
    if (!row) {
      // Rare race: became missing; schedule full
      missing.push(id);
      continue;
    }

    const prevSL = Number(row.stopLoss ?? 0);
    const prevTP = Number(row.takeProfit ?? 0);

    if (sl !== prevSL || tp !== prevTP) {
      patches.push({ id, stopLoss: sl, takeProfit: tp });
    }
  }

  let patched = 0;
  if (patches.length) {
    const r = await httpJson(`${writeBase}/trades/batchPatchSLTP`, {
      method: "POST",
      body: { patches },
    });
    patched = r.updated ?? 0;
  }

  return { patched, upsertedMissing };
}

async function syncStates({ paymaster, ids, writeBase, dbReaders, maxExistingId }) {
  // If missing in DB but exists onchain => full fetch
  const missing = [];
  const present = [];

  for (const id of ids) {
    if (id > maxExistingId) continue;
    if (!dbReaders.exists(id)) missing.push(id);
    else present.push(id);
  }

  let upsertedMissing = 0;
  if (missing.length) {
    const r = await syncFull({ paymaster, ids: missing, writeBase });
    upsertedMissing += r.upserted;
  }

  if (!present.length) return { patched: 0, upsertedMissing, fullForClosed: 0 };

  const states = await paymaster.getTradeStatesFromList(present);
  
  const patches = [];
  const needFull = [];

  for (let i = 0; i < present.length; i++) {
    const id = present[i];
    const newState = Number(states[i]);

    const row = dbReaders.getRow(id);
    if (!row) {
      // Rare race: became missing
      needFull.push(id);
      continue;
    }

    const prevState = Number(row.state);

    if (newState !== prevState) {
      // Any state change triggers a full sync now
      needFull.push(id);
    }
  }

  let patched = 0;
  if (patches.length) {
    const r = await httpJson(`${writeBase}/trades/batchPatchStates`, {
      method: "POST",
      body: { patches },
    });
    patched = r.updated ?? 0;
  }

  let fullForClosed = 0;
  if (needFull.length) {
    const r = await syncFull({ paymaster, ids: needFull, writeBase });
    fullForClosed = r.upserted;
  }

  return { patched, upsertedMissing, fullForClosed };
}

// Optional helper: scan a range [start..end] and return ids missing in DB
function computeMissingIds(dbReaders, start, end) {
  const missing = [];
  for (let id = start; id <= end; id++) {
    if (!dbReaders.exists(id)) missing.push(id);
  }
  return missing;
}

// --------------------
// MAIN
// --------------------
async function main() {
  const args = parseArgs(process.argv);

  const mode = args.mode;
  if (!["full", "sltp", "states"].includes(mode)) {
    console.error("Usage: node sync.js --mode full|sltp|states [--ids 1,2,3 | --range start count | --missing-scan start end]");
    process.exit(1);
  }

  const provider = ethers.JsonRpcProvider
  ? new ethers.JsonRpcProvider(cfg.RPC_URL)               // ethers v6
  : new ethers.providers.JsonRpcProvider(cfg.RPC_URL);    // ethers v5
  
  const core = new ethers.Contract(cfg.CORE_ADDRESS, CORE_ABI, provider);
  const paymaster = new ethers.Contract(cfg.PAYMASTER_ADDRESS, PAYMASTER_ABI, provider);

  const nextId = toIntSafeBN(await core.nextTradeID());
  const maxExistingId = Number(nextId); // last existing tradeId (0 if none)

  if (maxExistingId === 0) {
    console.log("No trades onchain yet (nextTradeID=0). Nothing to sync.");
    process.exit(0);
  }
  
  const writeBase = cfg.WRITE_BASE_URL || cfg.WRITE_BASE || "http://127.0.0.1:3001";
  // DB readers
  const dbRO = openDbReadOnly();
  const dbReaders = buildDbReaders(dbRO);

  let ids = [];

  if (args.ids) {
    ids = args.ids
      .split(",")
      .map((s) => Number(s.trim()))
      .filter((n) => Number.isFinite(n) && n >= 1);
    } else if (args.range) {
        let [start, count] = args.range;
        start = Math.max(1, start);
        for (let i = 0; i < count; i++) ids.push(start + i);
      } else if (args.missingScan) {
    // Build ids = missing ids in [start..end], but only up to maxExistingId
    const [start, end] = args.missingScan;
    const realStart = Math.max(1, start);           // never scan 0
    const realEnd = Math.min(end, maxExistingId);
    ids = computeMissingIds(dbReaders, realStart, realEnd);
    console.log(`Missing in DB within [${realStart}..${realEnd}]: ${ids.length}`);
  } else {
    console.error("Provide --ids or --range or --missing-scan");
    process.exit(1);
  }

  // In your Core, tradeIds start at 1 (because ++nextTradeID)
  ids = ids.filter((id) => id >= 1 && id <= maxExistingId);

  // Batch + concurrency limit for RPC calls
  const limit = pLimit(RPC_CONCURRENCY);
  const batches = chunk(ids, BATCH_SIZE);

  let totals = { upserted: 0, patched: 0, upsertedMissing: 0, fullForClosed: 0 };

  const tasks = batches.map((b, idx) =>
    limit(async () => {
      if (!b.length) return;

      if (mode === "full") {
        const r = await syncFull({ paymaster, ids: b, writeBase });
        totals.upserted += r.upserted;
      } else if (mode === "sltp") {
        const r = await syncSLTP({ paymaster, ids: b, writeBase, dbReaders, maxExistingId });
        totals.patched += r.patched;
        totals.upsertedMissing += r.upsertedMissing;
      } else if (mode === "states") {
        const r = await syncStates({ paymaster, ids: b, writeBase, dbReaders, maxExistingId });
        totals.patched += r.patched;
        totals.upsertedMissing += r.upsertedMissing;
        totals.fullForClosed += r.fullForClosed;
      }

      if ((idx + 1) % 10 === 0 || idx === batches.length - 1) {
        console.log(`[${mode}] batches ${idx + 1}/${batches.length} done`);
      }
    })
  );

  try {
    await Promise.all(tasks);
  } finally {
    dbRO.close();
  }

  console.log("Done.", {
    mode,
    maxExistingId,
    ...totals,
  });
}

main().catch((e) => {
  console.error("sync.js error:", e);
  process.exit(1);
});