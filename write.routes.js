// write.routes.js
// PRIVATE write endpoints (bind this server to 127.0.0.1 only).
// You provide the trade id in the URL.

const express = require("express");
const { stmt, tx } = require("./db");

const router = express.Router();

function normalizeAddress(addr) {
  if (typeof addr !== "string") return "";
  return addr.trim().toLowerCase();
}

function toInt(v, name, { allowNull = false } = {}) {
  if (v === null || v === undefined) {
    if (allowNull) return null;
    throw new Error(`Missing ${name}`);
  }
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`Invalid ${name}`);
  return Math.trunc(n);
}

function toBoolInt(v, name) {
  if (v === true || v === 1 || v === "1" || v === "true") return 1;
  if (v === false || v === 0 || v === "0" || v === "false") return 0;
  throw new Error(`Invalid ${name} (expected boolean)`);
}

function requireTradeExists(id) {
  const row = stmt.getTradeById.get(id);
  if (!row) {
    const err = new Error("Trade not found");
    err.status = 404;
    throw err;
  }
  return row;
}

/**
 * PUT /trade/:id
 * Full upsert (insert or replace/update) with your provided id.
 * Body fields are expected in E6 for prices.
 */
router.put("/trade/:id", (req, res) => {
  try {
    const id = toInt(req.params.id, "id");
    const b = req.body || {};

    const payload = {
      id,
      trader: normalizeAddress(b.trader),
      assetId: toInt(b.assetId, "assetId"),
      isLong: toBoolInt(b.isLong, "isLong"),
      isLimit: toBoolInt(b.isLimit, "isLimit"),
      leverage: b.leverage === undefined || b.leverage === null ? null : toInt(b.leverage, "leverage"),

      openPrice: b.openPrice === undefined || b.openPrice === null ? null : toInt(b.openPrice, "openPrice"),
      state: toInt(b.state, "state"),
      openTimestamp: b.openTimestamp === undefined || b.openTimestamp === null ? null : toInt(b.openTimestamp, "openTimestamp"),
      fundingIndex: b.fundingIndex === undefined || b.fundingIndex === null ? null : String(b.fundingIndex),

      closePrice: b.closePrice === undefined || b.closePrice === null ? 0 : toInt(b.closePrice, "closePrice"),
      lotSize: b.lotSize === undefined || b.lotSize === null ? null : toInt(b.lotSize, "lotSize"),
      closedLotSize: b.closedLotSize === undefined || b.closedLotSize === null ? 0 : toInt(b.closedLotSize, "closedLotSize"),

      stopLoss: b.stopLoss === undefined || b.stopLoss === null ? 0 : toInt(b.stopLoss, "stopLoss"),
      takeProfit: b.takeProfit === undefined || b.takeProfit === null ? 0 : toInt(b.takeProfit, "takeProfit"),

      lpLockedCapital: b.lpLockedCapital === undefined || b.lpLockedCapital === null ? null : String(b.lpLockedCapital),
      marginUsdc: b.marginUsdc === undefined || b.marginUsdc === null ? null : String(b.marginUsdc),
    };

    if (!payload.trader.startsWith("0x") || payload.trader.length < 10) {
      return res.status(400).json({ ok: false, error: "Invalid trader address" });
    }

    // Minimal sanity: if Closed => closePrice must be set
    if (payload.state === 2 && (!payload.closePrice || payload.closePrice === 0)) {
      return res.status(400).json({ ok: false, error: "state=2 requires closePrice != 0" });
    }

    const trade = tx.upsertTrade(payload);
    res.json({ ok: true, trade });
  } catch (e) {
    res.status(e.status || 400).json({ ok: false, error: e.message || "Bad request" });
  }
});

/**
 * PATCH /trade/:id
 * Partial updates: state, closePrice, stopLoss, takeProfit, closedLotSize, marginUsdc, lpLockedCapital, fundingIndex
 */
router.patch("/trade/:id", (req, res) => {
  try {
    const id = toInt(req.params.id, "id");
    const existing = requireTradeExists(id);
    const b = req.body || {};

    const patch = {
      id,
      state: b.state === undefined || b.state === null ? null : toInt(b.state, "state"),
      closePrice: b.closePrice === undefined || b.closePrice === null ? null : toInt(b.closePrice, "closePrice"),
      stopLoss: b.stopLoss === undefined || b.stopLoss === null ? null : toInt(b.stopLoss, "stopLoss"),
      takeProfit: b.takeProfit === undefined || b.takeProfit === null ? null : toInt(b.takeProfit, "takeProfit"),
      closedLotSize: b.closedLotSize === undefined || b.closedLotSize === null ? null : toInt(b.closedLotSize, "closedLotSize"),

      marginUsdc: b.marginUsdc === undefined || b.marginUsdc === null ? null : String(b.marginUsdc),
      lpLockedCapital: b.lpLockedCapital === undefined || b.lpLockedCapital === null ? null : String(b.lpLockedCapital),
      fundingIndex: b.fundingIndex === undefined || b.fundingIndex === null ? null : String(b.fundingIndex),
    };

    // If closing now, ensure closePrice exists (in patch or already stored)
    if (patch.state === 2) {
      const close = patch.closePrice ?? existing.closePrice;
      if (!close || close === 0) {
        return res.status(400).json({ ok: false, error: "state=2 requires closePrice != 0" });
      }
      // if you want: auto-set full close if not provided
      if (patch.closedLotSize === null) patch.closedLotSize = existing.lotSize;
    }

    const trade = tx.patchTrade(patch);
    res.json({ ok: true, trade });
  } catch (e) {
    res.status(e.status || 400).json({ ok: false, error: e.message || "Bad request" });
  }
});

router.post("/trades/batchUpsert", (req, res) => {
    try {
      const items = req.body?.trades;
      if (!Array.isArray(items) || items.length === 0) {
        return res.status(400).json({ ok: false, error: "Body must include trades: []" });
      }
      if (items.length > 2000) {
        return res.status(400).json({ ok: false, error: "Too many trades in one batch (max 2000)" });
      }
  
      // Build payloads like PUT /trade/:id expects, but in batch.
      const payloads = items.map((b) => {
        const id = Number(b.id);
        if (!Number.isFinite(id)) throw new Error("Invalid id in batch");
  
        const trader = normalizeAddress(b.trader);
        if (!trader.startsWith("0x") || trader.length < 10) throw new Error(`Invalid trader for id=${id}`);
  
        return {
          id,
          trader,
          assetId: toInt(b.assetId, "assetId"),
          isLong: toBoolInt(b.isLong, "isLong"),
          isLimit: toBoolInt(b.isLimit, "isLimit"),
          leverage: b.leverage === undefined || b.leverage === null ? null : toInt(b.leverage, "leverage"),
          openPrice: b.openPrice === undefined || b.openPrice === null ? null : toInt(b.openPrice, "openPrice"),
          state: toInt(b.state, "state"),
          openTimestamp: b.openTimestamp === undefined || b.openTimestamp === null ? null : toInt(b.openTimestamp, "openTimestamp"),
          fundingIndex: b.fundingIndex === undefined || b.fundingIndex === null ? null : String(b.fundingIndex),
          closePrice: b.closePrice === undefined || b.closePrice === null ? 0 : toInt(b.closePrice, "closePrice"),
          lotSize: b.lotSize === undefined || b.lotSize === null ? null : toInt(b.lotSize, "lotSize"),
          closedLotSize: b.closedLotSize === undefined || b.closedLotSize === null ? 0 : toInt(b.closedLotSize, "closedLotSize"),
          stopLoss: b.stopLoss === undefined || b.stopLoss === null ? 0 : toInt(b.stopLoss, "stopLoss"),
          takeProfit: b.takeProfit === undefined || b.takeProfit === null ? 0 : toInt(b.takeProfit, "takeProfit"),
          lpLockedCapital: b.lpLockedCapital === undefined || b.lpLockedCapital === null ? null : String(b.lpLockedCapital),
          marginUsdc: b.marginUsdc === undefined || b.marginUsdc === null ? null : String(b.marginUsdc),
        };
      });
  
      // Do one transaction for the whole batch (fast + safe)
      const { db, stmt } = require("./db");
      const runBatch = db.transaction((ps) => {
        for (const p of ps) stmt.upsertTrade.run(p);
        return ps.length;
      });
  
      const count = runBatch(payloads);
      res.json({ ok: true, upserted: count });
    } catch (e) {
      res.status(e.status || 400).json({ ok: false, error: e.message || "Bad request" });
    }
});

router.post("/trades/batchPatchStates", (req, res) => {
  try {
    const items = req.body?.patches;
    if (!Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ ok: false, error: "Body must include patches: []" });
    }
    if (items.length > 5000) {
      return res.status(400).json({ ok: false, error: "Too many patches in one batch (max 5000)" });
    }

    const patches = items.map((b) => {
      const id = toInt(b.id, "id");
      return {
        id,
        state: b.state === undefined || b.state === null ? null : toInt(b.state, "state"),
        closePrice: b.closePrice === undefined || b.closePrice === null ? null : toInt(b.closePrice, "closePrice"),
        closedLotSize: b.closedLotSize === undefined || b.closedLotSize === null ? null : toInt(b.closedLotSize, "closedLotSize"),
        fundingIndex: b.fundingIndex === undefined || b.fundingIndex === null ? null : String(b.fundingIndex),
      };
    });

    const { tx } = require("./db");
    const updated = tx.batchPatchStates(patches);
    res.json({ ok: true, updated });
  } catch (e) {
    res.status(e.status || 400).json({ ok: false, error: e.message || "Bad request" });
  }
});

router.post("/trades/batchPatchSLTP", (req, res) => {
  try {
    const items = req.body?.patches;
    if (!Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ ok: false, error: "Body must include patches: []" });
    }
    if (items.length > 5000) {
      return res.status(400).json({ ok: false, error: "Too many patches in one batch (max 5000)" });
    }

    const patches = items.map((b) => {
      const id = toInt(b.id, "id");
      return {
        id,
        stopLoss: b.stopLoss === undefined || b.stopLoss === null ? null : toInt(b.stopLoss, "stopLoss"),
        takeProfit: b.takeProfit === undefined || b.takeProfit === null ? null : toInt(b.takeProfit, "takeProfit"),
      };
    });

    const { tx } = require("./db");
    const updated = tx.batchPatchSLTP(patches);
    res.json({ ok: true, updated });
  } catch (e) {
    res.status(e.status || 400).json({ ok: false, error: e.message || "Bad request" });
  }
});
  

module.exports = router;
