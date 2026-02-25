// db.js
// All SQL + prepared statements live here.
// Trade IDs are PROVIDED BY YOU (no autoincrement).

const Database = require("better-sqlite3");

const DB_PATH = process.env.DB_PATH || "trades.db";

const db = new Database(DB_PATH, process.env.SQL_VERBOSE === "1"
  ? { verbose: console.log }
  : {}
);

// WAL for better concurrency
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");

function initDb() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS trades (
      id INTEGER PRIMARY KEY,           -- <-- YOU provide the trade id

      trader TEXT NOT NULL,             -- lowercase 0x...
      assetId INTEGER NOT NULL,         -- uint32

      isLong INTEGER NOT NULL,          -- 0/1
      isLimit INTEGER NOT NULL DEFAULT 0, -- 0=stop entry, 1=limit entry

      leverage INTEGER,                -- uint8

      openPrice INTEGER,               -- E6
      state INTEGER,                   -- 0=Order,1=Open,2=Closed,3=Cancelled
      openTimestamp INTEGER,           -- uint32

      fundingIndex TEXT,               -- uint128 decimal string

      closePrice INTEGER,              -- E6

      lotSize INTEGER,                 -- int32
      closedLotSize INTEGER NOT NULL DEFAULT 0, -- int32 (partial close tracking)

      stopLoss INTEGER NOT NULL DEFAULT 0,     -- E6, 0 = ignore
      takeProfit INTEGER NOT NULL DEFAULT 0,   -- E6, 0 = ignore

      lpLockedCapital TEXT,            -- uint64 decimal string
      marginUsdc TEXT                  -- uint64 decimal string
    );
  `);

  // Indexes
  db.exec(`CREATE INDEX IF NOT EXISTS idx_trader ON trades(trader);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_trader_state_id ON trades(trader, state, id);`);

  // Matching entry (state=0)
  db.exec(`CREATE INDEX IF NOT EXISTS idx_entry_fast ON trades(assetId, state, isLimit, isLong, openPrice);`);

  // Matching exits (state=1)
  db.exec(`CREATE INDEX IF NOT EXISTS idx_sl_fast ON trades(assetId, state, isLong, stopLoss);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_tp_fast ON trades(assetId, state, isLong, takeProfit);`);
}

initDb();

// --------------------
// READ statements
// --------------------
const stmt = {
  getTradeById: db.prepare(`SELECT * FROM trades WHERE id = ?;`),

  getTraderIdsAll: db.prepare(`
    SELECT id FROM trades
    WHERE trader = ?
    ORDER BY id DESC;
  `),

  getTraderIdsByState: db.prepare(`
    SELECT id FROM trades
    WHERE trader = ? AND state = ?
    ORDER BY id DESC;
  `),

  // Entry match (state=0): returns {id, kind}
  matchEntry: db.prepare(`
    SELECT id,
           CASE WHEN isLimit = 1 THEN 'limit' ELSE 'stop' END AS kind
    FROM trades
    WHERE assetId = ?
      AND state = 0
      AND (
        (isLimit = 1 AND (
            (isLong = 1 AND ? <= openPrice) OR
            (isLong = 0 AND ? >= openPrice)
        ))
        OR
        (isLimit = 0 AND (
            (isLong = 1 AND ? >= openPrice) OR
            (isLong = 0 AND ? <= openPrice)
        ))
      );
  `),

  // Exits match (state=1): returns {id, kind} where kind is stopLoss/takeProfit
  matchExits: db.prepare(`
    SELECT id,
           CASE
             WHEN stopLoss != 0 AND (
               (isLong = 1 AND ? <= stopLoss) OR
               (isLong = 0 AND ? >= stopLoss)
             ) THEN 'stopLoss'
             WHEN takeProfit != 0 AND (
               (isLong = 1 AND ? >= takeProfit) OR
               (isLong = 0 AND ? <= takeProfit)
             ) THEN 'takeProfit'
             ELSE NULL
           END AS kind
    FROM trades
    WHERE assetId = ?
      AND state = 1
      AND (
        (stopLoss != 0 AND (
          (isLong = 1 AND ? <= stopLoss) OR
          (isLong = 0 AND ? >= stopLoss)
        ))
        OR
        (takeProfit != 0 AND (
          (isLong = 1 AND ? >= takeProfit) OR
          (isLong = 0 AND ? <= takeProfit)
        ))
      );
  `),
};

// --------------------
// WRITE statements
// --------------------

// Full UPSERT (atomic): insert if missing, update if exists.
const upsertTradeSql = `
  INSERT INTO trades (
    id, trader, assetId, isLong, isLimit, leverage,
    openPrice, state, openTimestamp, fundingIndex,
    closePrice, lotSize, closedLotSize, stopLoss, takeProfit,
    lpLockedCapital, marginUsdc
  ) VALUES (
    @id, @trader, @assetId, @isLong, @isLimit, @leverage,
    @openPrice, @state, @openTimestamp, @fundingIndex,
    @closePrice, @lotSize, @closedLotSize, @stopLoss, @takeProfit,
    @lpLockedCapital, @marginUsdc
  )
  ON CONFLICT(id) DO UPDATE SET
    trader=excluded.trader,
    assetId=excluded.assetId,
    isLong=excluded.isLong,
    isLimit=excluded.isLimit,
    leverage=excluded.leverage,
    openPrice=excluded.openPrice,
    state=excluded.state,
    openTimestamp=excluded.openTimestamp,
    fundingIndex=excluded.fundingIndex,
    closePrice=excluded.closePrice,
    lotSize=excluded.lotSize,
    closedLotSize=excluded.closedLotSize,
    stopLoss=excluded.stopLoss,
    takeProfit=excluded.takeProfit,
    lpLockedCapital=excluded.lpLockedCapital,
    marginUsdc=excluded.marginUsdc;
`;

stmt.upsertTrade = db.prepare(upsertTradeSql);

// Partial patch (common updates)
stmt.patchTrade = db.prepare(`
  UPDATE trades SET
    state = COALESCE(@state, state),
    closePrice = COALESCE(@closePrice, closePrice),
    stopLoss = COALESCE(@stopLoss, stopLoss),
    takeProfit = COALESCE(@takeProfit, takeProfit),
    closedLotSize = COALESCE(@closedLotSize, closedLotSize),
    marginUsdc = COALESCE(@marginUsdc, marginUsdc),
    lpLockedCapital = COALESCE(@lpLockedCapital, lpLockedCapital),
    fundingIndex = COALESCE(@fundingIndex, fundingIndex)
  WHERE id = @id;
`);

// Patch state (+ closePrice + closedLotSize optional)
stmt.patchState = db.prepare(`
  UPDATE trades SET
    state = COALESCE(@state, state),
    closePrice = COALESCE(@closePrice, closePrice),
    closedLotSize = COALESCE(@closedLotSize, closedLotSize),
    fundingIndex = COALESCE(@fundingIndex, fundingIndex)
  WHERE id = @id
`);

// Patch SL/TP
stmt.patchSLTP = db.prepare(`
  UPDATE trades SET
    stopLoss = COALESCE(@stopLoss, stopLoss),
    takeProfit = COALESCE(@takeProfit, takeProfit)
  WHERE id = @id
`);

const tx = {
  upsertTrade: db.transaction((payload) => {
    stmt.upsertTrade.run(payload);
    return stmt.getTradeById.get(payload.id);
  }),

  patchTrade: db.transaction((payload) => {
    const info = stmt.patchTrade.run(payload);
    if (info.changes === 0) return null;
    return stmt.getTradeById.get(payload.id);
  }),

  batchPatchStates: db.transaction((patches) => {
    let updated = 0;
    for (const p of patches) {
      const info = stmt.patchState.run(p);
      updated += info.changes ? 1 : 0;
    }
    return updated;
  }),

  batchPatchSLTP: db.transaction((patches) => {
    let updated = 0;
    for (const p of patches) {
      const info = stmt.patchSLTP.run(p);
      updated += info.changes ? 1 : 0;
    }
    return updated;
  })
};

module.exports = { db, stmt, tx };