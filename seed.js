// seed.js (BATCH VERSION)
// Uses: POST http://127.0.0.1:3001/trades/batchUpsert  with { trades: [...] }

const http = require("http");

const WRITE_HOST = "127.0.0.1";
const WRITE_PORT = 3001;

const ASSET_ID = 0;
const MARKET_HUMAN = 69000;
const MARKET_E6 = MARKET_HUMAN * 1_000_000;

// Total trades
const N = 10_000;

// Split
const N_ORDERS = 4000; // state=0
const N_OPEN = 6000;   // state=1

// Expected match counts at MARKET
const MATCH_LIMIT = 1200; // state=0, isLimit=1 matches
const MATCH_STOP  = 800;  // state=0, isLimit=0 matches

const MATCH_SL = 900;     // state=1 stopLoss triggers
const MATCH_TP = 700;     // state=1 takeProfit triggers

// Price dispersion around market
const RANGE_E6 = 500 * 1_000_000; // +/- 500

// Batch size (<= 2000 to match your endpoint guard)
const BATCH_SIZE = 500;

function postBatch(trades, attempt = 1) {
  const payload = JSON.stringify({ trades });

  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: WRITE_HOST,
        port: WRITE_PORT,
        path: "/trades/batchUpsert",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(payload),
          "Connection": "close",
        },
      },
      (res) => {
        let data = "";
        res.on("data", (c) => (data += c));
        res.on("end", () => {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            resolve(data ? JSON.parse(data) : { ok: true });
          } else {
            reject(new Error(`POST batch failed: ${res.statusCode} ${data}`));
          }
        });
      }
    );

    req.on("error", (err) => {
      // retry a bit in case of transient resets
      if ((err.code === "ECONNRESET" || err.code === "ECONNREFUSED" || err.code === "EADDRNOTAVAIL") && attempt < 6) {
        return setTimeout(() => {
          postBatch(trades, attempt + 1).then(resolve).catch(reject);
        }, 250 * attempt);
      }
      reject(err);
    });

    req.write(payload);
    req.end();
  });
}

// deterministic RNG (LCG)
let seed = 123456789;
function rnd() {
  seed = (seed * 1664525 + 1013904223) >>> 0;
  return seed / 0xffffffff;
}
function randInt(min, max) {
  return Math.floor(rnd() * (max - min + 1)) + min;
}

function traderAddr(i) {
  const hex = i.toString(16).padStart(40, "0");
  return `0x${hex}`;
}

function aroundMarket(deltaMin, deltaMax) {
  const delta = randInt(deltaMin, deltaMax);
  return MARKET_E6 + delta;
}

/**
 * state=0 entry orders
 * LIMIT:
 *  - long: match if market <= openPrice  => openPrice >= market
 *  - short: match if market >= openPrice => openPrice <= market
 * STOP:
 *  - long: match if market >= openPrice  => openPrice <= market
 *  - short: match if market <= openPrice => openPrice >= market
 */
function buildOrder({ mustMatch, isLimit, isLong }) {
  let openPrice;

  if (isLimit) {
    if (isLong) openPrice = mustMatch ? aroundMarket(0, RANGE_E6) : aroundMarket(-RANGE_E6, -1);
    else        openPrice = mustMatch ? aroundMarket(-RANGE_E6, 0) : aroundMarket(1, RANGE_E6);
  } else {
    if (isLong) openPrice = mustMatch ? aroundMarket(-RANGE_E6, 0) : aroundMarket(1, RANGE_E6);
    else        openPrice = mustMatch ? aroundMarket(0, RANGE_E6) : aroundMarket(-RANGE_E6, -1);
  }

  const stopLoss = Math.max(0, openPrice - randInt(50_000_000, 150_000_000));
  const takeProfit = Math.max(0, openPrice + randInt(50_000_000, 150_000_000));

  return { state: 0, isLimit, isLong, openPrice, stopLoss, takeProfit };
}

/**
 * state=1 open positions exits
 * SL triggers:
 *  - long: market <= stopLoss   => stopLoss >= market
 *  - short: market >= stopLoss  => stopLoss <= market
 * TP triggers:
 *  - long: market >= takeProfit => takeProfit <= market
 *  - short: market <= takeProfit=> takeProfit >= market
 */
function buildOpen({ mustMatchKind, isLong }) {
  const openPrice = aroundMarket(-RANGE_E6, RANGE_E6);

  let stopLoss = 0;
  let takeProfit = 0;

  if (mustMatchKind === "sl") {
    stopLoss = isLong ? aroundMarket(0, RANGE_E6) : aroundMarket(-RANGE_E6, 0);
    takeProfit = 0;
  } else if (mustMatchKind === "tp") {
    takeProfit = isLong ? aroundMarket(-RANGE_E6, 0) : aroundMarket(0, RANGE_E6);
    stopLoss = 0;
  } else {
    // non-match
    if (isLong) {
      stopLoss = aroundMarket(-RANGE_E6, -1);
      takeProfit = aroundMarket(1, RANGE_E6);
    } else {
      stopLoss = aroundMarket(1, RANGE_E6);
      takeProfit = aroundMarket(-RANGE_E6, -1);
    }
    if (rnd() < 0.3) stopLoss = 0;
    if (rnd() < 0.3) takeProfit = 0;
  }

  return { state: 1, isLong, openPrice, stopLoss: Math.max(0, stopLoss), takeProfit: Math.max(0, takeProfit) };
}

function makeTrade(id, t) {
  return {
    id,
    trader: traderAddr(id % 2000),
    assetId: ASSET_ID,
    isLong: t.isLong,
    isLimit: t.isLimit || false,
    leverage: 10,

    openPrice: t.openPrice,
    state: t.state,
    openTimestamp: 1700000000 + id,
    fundingIndex: "0",

    closePrice: 0,
    lotSize: 100,
    closedLotSize: 0,

    stopLoss: t.stopLoss || 0,
    takeProfit: t.takeProfit || 0,

    lpLockedCapital: "0",
    marginUsdc: "100000000",
  };
}

async function main() {
  console.log("Seeding (batch)… make sure write server is running on 127.0.0.1:3001");
  console.log({ ASSET_ID, MARKET_HUMAN, MARKET_E6, BATCH_SIZE });

  const expected = {
    entry: { limit: MATCH_LIMIT, stop: MATCH_STOP },
    exits: { stopLoss: MATCH_SL, takeProfit: MATCH_TP },
    total: N,
  };

  const trades = [];
  let id = 1;

  // --- state=0 orders (match limit)
  for (let i = 0; i < MATCH_LIMIT; i++, id++) {
    const isLong = i % 2 === 0;
    const t = buildOrder({ mustMatch: true, isLimit: true, isLong });
    trades.push(makeTrade(id, t));
  }

  // --- state=0 orders (match stop)
  for (let i = 0; i < MATCH_STOP; i++, id++) {
    const isLong = i % 2 === 0;
    const t = buildOrder({ mustMatch: true, isLimit: false, isLong });
    trades.push(makeTrade(id, t));
  }

  // --- remaining orders non-match
  const remainingOrders = N_ORDERS - MATCH_LIMIT - MATCH_STOP;
  for (let i = 0; i < remainingOrders; i++, id++) {
    const isLong = i % 2 === 0;
    const isLimit = i % 3 !== 0;
    const t = buildOrder({ mustMatch: false, isLimit, isLong });
    trades.push(makeTrade(id, t));
  }

  // --- state=1 open (match SL)
  for (let i = 0; i < MATCH_SL; i++, id++) {
    const isLong = i % 2 === 0;
    const t = buildOpen({ mustMatchKind: "sl", isLong });
    // ensure isLimit present false
    t.isLimit = false;
    trades.push(makeTrade(id, t));
  }

  // --- state=1 open (match TP)
  for (let i = 0; i < MATCH_TP; i++, id++) {
    const isLong = i % 2 === 0;
    const t = buildOpen({ mustMatchKind: "tp", isLong });
    t.isLimit = false;
    trades.push(makeTrade(id, t));
  }

  // --- remaining open non-match
  const remainingOpen = N_OPEN - MATCH_SL - MATCH_TP;
  for (let i = 0; i < remainingOpen; i++, id++) {
    const isLong = i % 2 === 0;
    const t = buildOpen({ mustMatchKind: "none", isLong });
    t.isLimit = false;
    trades.push(makeTrade(id, t));
  }

  if (trades.length !== N) throw new Error(`Created ${trades.length} trades, expected ${N}`);

  // Send in batches
  for (let i = 0; i < trades.length; i += BATCH_SIZE) {
    const chunk = trades.slice(i, i + BATCH_SIZE);
    const r = await postBatch(chunk);
    if (!r?.ok) throw new Error(`Batch failed at ${i}: ${JSON.stringify(r)}`);
    if ((i + chunk.length) % 2000 === 0) {
      console.log(`Upserted ${i + chunk.length}/${trades.length}`);
    }
  }

  console.log("Seed done.");
  console.log("Expected match counts at MARKET:", expected);
  console.log("Now test:");
  console.log(`  curl "http://localhost:3000/match/entry?assetId=${ASSET_ID}&market=${MARKET_HUMAN}"`);
  console.log(`  curl "http://localhost:3000/match/exits?assetId=${ASSET_ID}&market=${MARKET_HUMAN}"`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
