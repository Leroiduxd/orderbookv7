#!/usr/bin/env node
/**
 * Listen to CORE TradeEvent(tradeId, code) on WSS
 * On event: run sync.js --mode full --ids <tradeId> (Batched)
 */

const path = require("path");
const { spawn } = require("child_process");
const { ethers } = require("ethers");
const cfg = require("../config");

// ---- ABI minimal (event only)
const CORE_ABI = [
  "event TradeEvent(uint256 tradeId, uint8 code)"
];

// --- NOUVEAU : Le Batcher de Resync ---
const RESYNC_FLUSH_MS = 1000; // Attend 1 seconde pour grouper les IDs

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
      // Si une synchro est déjà en cours, on retente plus tard
      scheduleFlush();
      return;
    }
    if (pending.size === 0) return;

    inFlight = true;

    try {
      const ids = Array.from(pending);
      pending.clear(); // Vide la file d'attente

      console.log(`[RESYNC-BATCH] Flushing ${ids.length} ids: ${ids.join(",")}`);

      await new Promise((resolve) => {
        const syncPath = path.join(__dirname, "..", "sync.js");

        const p = spawn(
          process.execPath, 
          [syncPath, "--mode", "full", "--ids", ids.join(",")], 
          {
            stdio: "inherit",
            env: process.env,
          }
        );

        p.on("exit", (code) => {
          console.log(`[RESYNC-BATCH] Done (code=${code}) for ${ids.length} ids`);
          resolve(); // On résout toujours pour débloquer inFlight
        });
      });
    } finally {
      inFlight = false;
      // S'il y a eu de nouveaux events pendant qu'on synchronisait, on relance
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
// --------------------------------------

async function main() {
  if (!cfg.WSS_URL) {
    console.error("Missing WSS_URL in config.js");
    process.exit(1);
  }

  console.log("Connecting CORE WSS:", cfg.WSS_URL);
  const wss = new ethers.providers.WebSocketProvider(cfg.WSS_URL);

  // optional: keepalive / reconnect
  wss._websocket?.on("close", (code) => {
    console.error("WSS closed:", code, "=> restart the process (pm2 recommended)");
  });
  wss._websocket?.on("error", (e) => {
    console.error("WSS error:", e);
  });

  const core = new ethers.Contract(cfg.CORE_ADDRESS, CORE_ABI, wss);

  // simple in-memory debounce to avoid duplicates
  const recently = new Map(); // tradeId -> ts
  const DEDUP_MS = 10_000;
  
  // Initialisation du batcher
  const resyncBatcher = createResyncBatcher();

  core.on("TradeEvent", async (tradeIdBn, code) => {
    try {
      const tradeId = Number(tradeIdBn.toString());
      const now = Date.now();

      const last = recently.get(tradeId) || 0;
      if (now - last < DEDUP_MS) return;
      recently.set(tradeId, now);

      console.log(`[TradeEvent] tradeId=${tradeId} code=${code} => ajout à la file d'attente...`);

      // Ajout au batcher au lieu de lancer le script tout de suite
      resyncBatcher.enqueue(tradeId);

    } catch (e) {
      console.error("Listener error:", e);
    }
  });

  console.log("Listening TradeEvent on CORE:", cfg.CORE_ADDRESS);
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});