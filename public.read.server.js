// public.read.server.js
// PUBLIC read-only API + PRIVATE write-only API (local only).
// Run this single file. It starts TWO servers:
// - Public READ server (0.0.0.0:3000)  -> safe to expose
// - Private WRITE server (127.0.0.1:3001) -> local only

const express = require("express");
const { stmt } = require("./db");
const writeRoutes = require("./write.routes");

const PUBLIC_PORT = Number(process.env.PUBLIC_PORT || 3000);
const PRIVATE_PORT = Number(process.env.PRIVATE_PORT || 3001);

function normalizeAddress(addr) {
  if (typeof addr !== "string") return "";
  return addr.trim().toLowerCase();
}

function toInt(v, name) {
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`Invalid ${name}`);
  return Math.trunc(n);
}

function parseMarketE6(query) {
  // market can be: 69000 (human) OR 69000000000 (E6) if unit=e6
  const raw = query.market;
  if (raw === undefined) throw new Error("Missing market");
  const unit = (query.unit || "human").toString().toLowerCase();
  const n = Number(raw);
  if (!Number.isFinite(n)) throw new Error("Invalid market");

  if (unit === "e6") return Math.trunc(n);
  return Math.round(n * 1e6);
}

// --------------------
// PUBLIC READ server
// --------------------
const readApp = express();

readApp.get("/health", (req, res) => res.json({ ok: true, mode: "public-read" }));

// GET /trader/:address/ids?state=all|0|1|2|3
readApp.get("/trader/:address/ids", (req, res) => {
  try {
    const trader = normalizeAddress(req.params.address);
    if (!trader.startsWith("0x") || trader.length < 10) {
      return res.status(400).json({ error: "Invalid address" });
    }

    const stateQ = (req.query.state || "all").toString().toLowerCase();
    let ids;

    if (stateQ === "all") {
      ids = stmt.getTraderIdsAll.all(trader).map((r) => r.id);
    } else {
      const state = toInt(stateQ, "state");
      ids = stmt.getTraderIdsByState.all(trader, state).map((r) => r.id);
    }

    res.json({ trader, ids });
  } catch (e) {
    res.status(400).json({ error: e.message || "Bad request" });
  }
});

// GET /trade/:id
readApp.get("/trade/:id", (req, res) => {
  try {
    const id = toInt(req.params.id, "id");
    const row = stmt.getTradeById.get(id);
    if (!row) return res.status(404).json({ error: "Trade not found" });
    res.json(row);
  } catch (e) {
    res.status(400).json({ error: e.message || "Bad request" });
  }
});

// GET /match/entry?assetId=0&market=69000[&unit=human|e6]
// returns ids executable for state=0 (orders)
readApp.get("/match/entry", (req, res) => {
  try {
    const assetId = toInt(req.query.assetId, "assetId");
    const marketE6 = parseMarketE6(req.query);

    const rows = stmt.matchEntry.all(assetId, marketE6, marketE6, marketE6, marketE6);

    const out = { limit: [], stop: [] };
    for (const r of rows) {
      if (r.kind === "limit") out.limit.push(r.id);
      else if (r.kind === "stop") out.stop.push(r.id);
    }

    res.json({ assetId, marketE6, ...out });
  } catch (e) {
    res.status(400).json({ error: e.message || "Bad request" });
  }
});

// GET /match/exits?assetId=0&market=69000[&unit=human|e6]
// returns ids executable for state=1 (open positions) based on SL/TP
readApp.get("/match/exits", (req, res) => {
  try {
    const assetId = toInt(req.query.assetId, "assetId");
    const marketE6 = parseMarketE6(req.query);

    const rows = stmt.matchExits.all(
      marketE6, marketE6, marketE6, marketE6,
      assetId,
      marketE6, marketE6, marketE6, marketE6
    );

    const out = { stopLoss: [], takeProfit: [] };
    for (const r of rows) {
      if (r.kind === "stopLoss") out.stopLoss.push(r.id);
      else if (r.kind === "takeProfit") out.takeProfit.push(r.id);
    }

    res.json({ assetId, marketE6, ...out });
  } catch (e) {
    res.status(400).json({ error: e.message || "Bad request" });
  }
});

// Public server listens on all interfaces
readApp.listen(PUBLIC_PORT, "0.0.0.0", () => {
  console.log(`Public READ API: http://0.0.0.0:${PUBLIC_PORT}`);
});

// --------------------
// PRIVATE WRITE server (LOCAL ONLY)
// --------------------
const writeApp = express();
writeApp.use(express.json({ limit: "1mb" }));

writeApp.get("/health", (req, res) => res.json({ ok: true, mode: "private-write" }));
writeApp.use("/", writeRoutes);

// Private server listens ONLY on localhost
writeApp.listen(PRIVATE_PORT, "127.0.0.1", () => {
  console.log(`Private WRITE API (local only): http://127.0.0.1:${PRIVATE_PORT}`);
});
