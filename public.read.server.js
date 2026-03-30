// public.read.server.js
// PUBLIC read-only API + PRIVATE write-only API (local only).
// Run this single file. It starts TWO servers:
// - Public READ server (0.0.0.0:3000)  -> safe to expose
// - Private WRITE server (127.0.0.1:3001) -> local only

const express = require("express");
const cors = require("cors");
const path = require("path");
const fs = require("fs");
const { ethers } = require("ethers");

const { stmt } = require("./db");
const writeRoutes = require("./write.routes");
const { getAllExposures } = require("./services/exposures"); 
const { generateTraderCard } = require("./services/image.service");
const { initBaseFunding, getLiveFunding } = require("./services/funding.service");
const { initBaseSpreads, getAllBaseSpreads } = require("./services/spread.service");

const PUBLIC_PORT = Number(process.env.PUBLIC_PORT || 7000);
const PRIVATE_PORT = Number(process.env.PRIVATE_PORT || 7001);

const RPC_URL = process.env.RPC_URL;
const PRIVATE_KEYS = (process.env.PRIVATE_KEYS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// On extrait les adresses publiques une seule fois au démarrage
const executorWallets = PRIVATE_KEYS.map(pk => {
  try {
    return new ethers.Wallet(pk).address;
  } catch (e) {
    return null;
  }
}).filter(Boolean);

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

// 1. Middlewares
readApp.use(cors()); 
readApp.use("/output", express.static(path.join(__dirname, "output"))); // <-- Déplacé ici, APRES la création de readApp !

// 2. Routes
readApp.get("/health", (req, res) => res.json({ ok: true, mode: "public-read" }));

readApp.get("/stats/max-trade-id", (req, res) => {
  try {
    const row = stmt.getMaxTradeId.get();
    const maxId = row && row.maxId !== null ? row.maxId : 0;
    res.json({ success: true, maxId });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch max trade ID" });
  }
});

readApp.get("/stats/total-traders", (req, res) => {
  try {
    const row = stmt.getTotalTraders.get();
    const totalTraders = row ? row.totalTraders : 0;
    res.json({ success: true, totalTraders });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch total traders count" });
  }
});

// GET /trades/open/:assetId
// Récupère toutes les infos des trades actuellement ouverts (state = 1) pour un actif précis
readApp.get("/trades/open/:assetId", (req, res) => {
  try {
    // On sécurise l'input pour être sûr que c'est bien un nombre
    const assetId = toInt(req.params.assetId, "assetId");
    
    // On exécute la requête SQL
    const rows = stmt.getOpenTradesByAssetId.all(assetId);
    
    // On renvoie un JSON propre avec le compte et toutes les données
    res.json({
      success: true,
      assetId: assetId,
      count: rows.length,
      data: rows
    });
  } catch (e) {
    res.status(400).json({ error: e.message || "Failed to fetch open trades for this asset" });
  }
});

// GET /trades/orders/:assetId
// Récupère toutes les infos des ordres en attente (state = 0) pour un actif précis
readApp.get("/trades/orders/:assetId", (req, res) => {
  try {
    const assetId = toInt(req.params.assetId, "assetId");
    const rows = stmt.getOrdersByAssetId.all(assetId);
    
    res.json({
      success: true,
      assetId: assetId,
      count: rows.length,
      data: rows
    });
  } catch (e) {
    res.status(400).json({ error: e.message || "Failed to fetch orders for this asset" });
  }
});

// GET /trades/closed/:assetId
// Récupère toutes les infos des trades fermés (state = 2) pour un actif précis
readApp.get("/trades/closed/:assetId", (req, res) => {
  try {
    const assetId = toInt(req.params.assetId, "assetId");
    const rows = stmt.getClosedTradesByAssetId.all(assetId);
    
    res.json({
      success: true,
      assetId: assetId,
      count: rows.length,
      data: rows
    });
  } catch (e) {
    res.status(400).json({ error: e.message || "Failed to fetch closed trades for this asset" });
  }
});

readApp.get("/stats/open-trades", (req, res) => {
  try {
    const rows = stmt.getOpenStatsPerAssetAndDirection.all();
    res.json({ success: true, data: rows });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch open trades stats" });
  }
});

readApp.get("/stats/volume-24h", (req, res) => {
  try {
    const twentyFourHoursAgo = Math.floor(Date.now() / 1000) - 86400;
    const row = stmt.getVolume24h.get(twentyFourHoursAgo);
    res.json({ 
      success: true, 
      volume24h: row && row.volume24h ? row.volume24h : 0
    });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch 24h volume" });
  }
});

readApp.get("/traders/list", (req, res) => {
  try {
    const rows = stmt.getAllUniqueTraders.all();
    const wallets = rows.map((r) => r.trader);
    res.json({ success: true, count: wallets.length, wallets });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch traders list" });
  }
});

readApp.get("/traders/top", (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 10, 100);
    const rows = stmt.getTopTradersAll.all(limit);
    res.json({ success: true, limit, data: rows });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch top traders" });
  }
});

readApp.get("/traders/top/active", (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 10, 100);
    const rows = stmt.getTopTradersByState.all(1, limit);
    res.json({ success: true, limit, data: rows });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch top active traders" });
  }
});

readApp.get("/metrics/trader/:address", (req, res) => {
  try {
    const trader = normalizeAddress(req.params.address);
    if (!trader.startsWith("0x") || trader.length < 10) {
      return res.status(400).json({ error: "Invalid address" });
    }
    const row = stmt.getTraderMetrics.get(trader);
    res.json({ 
      success: true, 
      trader, 
      metrics: {
        totalPnl: row && row.totalPnl ? row.totalPnl : 0,
        totalVolume: row && row.totalVolume ? row.totalVolume : 0
      } 
    });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch trader metrics" });
  }
});

readApp.get("/metrics/top/volume", (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 10, 100);
    const rows = stmt.getTopTradersByVolume.all(limit);
    res.json({ success: true, limit, data: rows });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch top volume traders" });
  }
});

readApp.get("/metrics/top/pnl", (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 10, 100);
    const rows = stmt.getTopTradersByPnl.all(limit);
    res.json({ success: true, limit, data: rows });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch top PnL traders" });
  }
});

readApp.get("/metrics/top/trades", (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 10, 100);
    const rows = stmt.getTopTradesByPnl.all(limit);
    res.json({ success: true, limit, data: rows });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch top trades by PnL" });
  }
});

readApp.get("/exposures", (req, res) => {
  try {
    const memory = getAllExposures();
    res.json({
        success: true,
        count: Object.keys(memory).length,
        data: memory
    });
  } catch (e) {
    res.status(500).json({ error: "Failed to read exposures memory" });
  }
});

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

readApp.get("/traders/leaderboard", (req, res) => {
  try {
    const limit = 100;
    const topByPnl = stmt.getTop100Pnl.all(limit);
    const topByVolume = stmt.getTop100Volume.all(limit);
    const topByTrades = stmt.getTop100Trades.all(limit);

    res.json({ 
      success: true, 
      topByPnl: topByPnl,
      topByVolume: topByVolume,
      topByTrades: topByTrades
    });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch leaderboards" });
  }
});

readApp.get("/trader/:address/ranks", (req, res) => {
  try {
    const trader = normalizeAddress(req.params.address);
    if (!trader.startsWith("0x") || trader.length < 10) {
      return res.status(400).json({ error: "Invalid address" });
    }

    const activityRow = stmt.getTraderRankByActivity.get(trader);
    const volumeRow = stmt.getTraderRankByVolume.get(trader);
    const pnlRow = stmt.getTraderRankByPnl.get(trader);

    res.json({
      success: true,
      trader,
      ranks: {
        activity: activityRow ? { rank: activityRow.rank, value: activityRow.tradesCount } : { rank: null, value: 0 },
        volume: volumeRow ? { rank: volumeRow.rank, value: volumeRow.totalVolume } : { rank: null, value: 0 },
        pnl: pnlRow ? { rank: pnlRow.rank, value: pnlRow.totalPnl } : { rank: null, value: 0 }
      }
    });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch trader ranks" });
  }
});
// --- SYSTÈME DE POINTS ---

// Classement Top 100 par points
readApp.get("/traders/points", (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 100, 500);
    const rows = stmt.getPointsLeaderboard.all(limit);
    
    const leaderboard = rows.map((r, index) => ({
      rank: index + 1,
      trader: r.trader,
      points: Math.round(r.points * 100) / 100,
      breakdown: {
        pointsFromTrades: r.totalTrades,
        pointsFromVolume: Math.round((r.totalVolumeE6 / 1000000000) * 100) / 100,
        pointsFromPnl: Math.max(0, Math.round((r.totalPnlE6 / 1000000) * 100) / 100)
      }
    }));

    res.json({ success: true, limit, data: leaderboard });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch points leaderboard" });
  }
});

// Points d'un trader spécifique
// Points et Classement d'un trader spécifique
readApp.get("/trader/:address/points", (req, res) => {
  try {
    const address = normalizeAddress(req.params.address);
    if (!address || address.length < 10) return res.status(400).send("Invalid address");

    const row = stmt.getTraderPoints.get(address);

    if (!row) {
      return res.json({ success: true, trader: address, rank: null, points: 0, breakdown: null });
    }

    res.json({
      success: true,
      trader: address,
      rank: row.rank, // <-- LE CLASSEMENT EST LÀ
      points: Math.round(row.points * 100) / 100,
      breakdown: {
        pointsFromTrades: row.totalTrades,
        pointsFromVolume: Math.round((row.totalVolumeE6 / 1000000000) * 100) / 100,
        pointsFromPnl: Math.max(0, Math.round((row.totalPnlE6 / 1000000) * 100) / 100)
      }
    });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch trader points" });
  }
});
// GET /trader/:address/card.png - ZÉRO STOCKAGE, généré à la volée !
readApp.get("/trader/:address/card.png", (req, res) => {
  try {
    // On garde l'adresse originale pour l'affichage (avec les majuscules)
    const rawAddress = req.params.address; 
    const address = normalizeAddress(rawAddress);
    
    if (!address || address.length < 10) return res.status(400).send("Invalid");

    // 1. On lit les stats direct dans SQLite
    const actRow = stmt.getTraderRankByActivity.get(address);
    const volRow = stmt.getTraderRankByVolume.get(address);
    const pnlRow = stmt.getTraderRankByPnl.get(address);
    
    const ranks = {
      activity: { rank: actRow?.rank, value: actRow?.tradesCount || 0 },
      volume: { rank: volRow?.rank, value: volRow?.totalVolume || 0 },
      pnl: { rank: pnlRow?.rank, value: pnlRow?.totalPnl || 0 }
    };

    // 2. On génère l'image (ça retourne juste un Buffer en mémoire)
    // On utilise rawAddress pour garder le format visuel "0xCa3..."
    const pngBuffer = generateTraderCard({ address: rawAddress, ranks });

    // 3. LA MAGIE EST ICI : On force le téléchargement avec "attachment"
    res.setHeader("Content-Type", "image/png");
    res.setHeader("Content-Disposition", `attachment; filename="Brokex-Stats-${rawAddress.slice(0, 6)}.png"`);
    
    // 4. On envoie l'image au navigateur direct
    res.send(pngBuffer);

  } catch (err) {
    console.error(err);
    res.status(500).send("Error");
  }
});
// GET /funding/live/:assetId
// Récupère l'index de funding live SANS appeler la blockchain
readApp.get("/funding/live/:assetId", (req, res) => {
  try {
    const assetId = toInt(req.params.assetId, "assetId");
    const data = getLiveFunding(assetId);

    if (!data) {
      return res.status(404).json({ error: "Funding data not initialized for this asset yet or not supported" });
    }

    res.json({ success: true, data });
  } catch (e) {
    res.status(400).json({ error: "Bad request" });
  }
});

// GET /stats/funding/all
// Récupère le funding index moyen pondéré pour TOUS les actifs en une seule requête
readApp.get("/stats/funding/all", (req, res) => {
  try {
    const rows = stmt.getAllWeightedFundingIndices.all();

    // On prépare un objet vide pour stocker les résultats par assetId
    const result = {};

    for (const row of rows) {
      const assetId = row.assetId;
      
      // Si l'actif n'est pas encore dans l'objet, on l'initialise
      if (!result[assetId]) {
        result[assetId] = {
          longAvgFundingIndex: "0",
          shortAvgFundingIndex: "0",
          longTotalLots: 0,
          shortTotalLots: 0
        };
      }

      // On remplit les données Long ou Short
      if (row.isLong === 1) {
        result[assetId].longAvgFundingIndex = Math.round(row.avgFundingIndex).toString();
        result[assetId].longTotalLots = row.totalLots;
      } else {
        result[assetId].shortAvgFundingIndex = Math.round(row.avgFundingIndex).toString();
        result[assetId].shortTotalLots = row.totalLots;
      }
    }

    res.json({
      success: true,
      data: result
    });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch all weighted funding indices" });
  }
}); 

// GET /stats/funding/:assetId
// Récupère le funding index moyen pondéré par la taille des lots pour les Longs et les Shorts ouverts
readApp.get("/stats/funding/:assetId", (req, res) => {
  try {
    const assetId = toInt(req.params.assetId, "assetId");
    const rows = stmt.getWeightedFundingIndex.all(assetId);

    // On formate la réponse pour séparer Long et Short proprement
    const result = {
      assetId: assetId,
      long: { avgFundingIndex: 0, totalLots: 0 },
      short: { avgFundingIndex: 0, totalLots: 0 }
    };

    for (const row of rows) {
      if (row.isLong === 1) {
        result.long.avgFundingIndex = row.avgFundingIndex;
        result.long.totalLots = row.totalLots;
      } else {
        result.short.avgFundingIndex = row.avgFundingIndex;
        result.short.totalLots = row.totalLots;
      }
    }

    // On convertit les E18 en strings ou entiers pour éviter les bugs de précision
    res.json({
      success: true,
      data: {
        longAvgFundingIndex: Math.round(result.long.avgFundingIndex).toString(),
        shortAvgFundingIndex: Math.round(result.short.avgFundingIndex).toString(),
        longTotalLots: result.long.totalLots,
        shortTotalLots: result.short.totalLots
      }
    });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch weighted funding index" });
  }
});

// GET /match/liquidations
// Retourne les IDs des trades qui ont perdu 90% ou plus de leur marge
readApp.get("/match/liquidations", (req, res) => {
  try {
    const assetId = toInt(req.query.assetId, "assetId");
    const marketE6 = parseMarketE6(req.query);

    // On passe l'assetId, et 2 fois le marketE6 (une fois pour le calcul Long, une fois pour le calcul Short)
    const rows = stmt.matchLiquidations.all(assetId, marketE6, marketE6);

    // On renvoie un tableau simple avec juste les IDs à liquider
    res.json({ 
      assetId, 
      marketE6, 
      liquidations: rows.map(r => r.id) 
    });
  } catch (e) {
    res.status(400).json({ error: e.message || "Bad request" });
  }
});

// GET /spreads/base
// Récupère les spreads de base de tous les actifs (WAD)
readApp.get("/spreads/base", (req, res) => {
  try {
    const data = getAllBaseSpreads();
    res.json({ success: true, data });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch base spreads" });
  }
});

// GET /executor/wallets
// Récupère les adresses publiques des bots d'exécution et leur solde en ETH
readApp.get("/executor/wallets", async (req, res) => {
  try {
    if (!RPC_URL || executorWallets.length === 0) {
      return res.status(400).json({ error: "RPC_URL or PRIVATE_KEYS missing in .env" });
    }

    // On se connecte à la blockchain
    const provider = new ethers.providers.JsonRpcProvider(RPC_URL);

    // On récupère le solde de chaque wallet en parallèle
    const walletsInfo = await Promise.all(
      executorWallets.map(async (address) => {
        try {
          const balanceWei = await provider.getBalance(address);
          return {
            address: address,
            balanceEth: ethers.utils.formatEther(balanceWei) // Convertit les Wei en ETH lisible
          };
        } catch (err) {
          return { address, balanceEth: "Error fetching balance" };
        }
      })
    );

    res.json({ 
      success: true, 
      count: walletsInfo.length,
      data: walletsInfo 
    });
  } catch (e) {
    console.error("Erreur fetching balances:", e);
    res.status(500).json({ error: "Failed to fetch wallets info" });
  }
});

// On initialise le Funding ET le Spread AVANT de lancer l'API publique
Promise.all([
  initBaseFunding(),
  initBaseSpreads()
]).then(() => {
  readApp.listen(PUBLIC_PORT, "0.0.0.0", () => {
    console.log(`Public READ API: http://0.0.0.0:${PUBLIC_PORT}`);
  });
}).catch(err => {
  console.error("Erreur critique lors de l'initialisation:", err);
});

// --------------------
// PRIVATE WRITE server (LOCAL ONLY)
// --------------------
const writeApp = express();

writeApp.use(cors()); 
writeApp.use(express.json({ limit: "1mb" }));

writeApp.get("/health", (req, res) => res.json({ ok: true, mode: "private-write" }));
writeApp.use("/", writeRoutes);

writeApp.listen(PRIVATE_PORT, "127.0.0.1", () => {
  console.log(`Private WRITE API (local only): http://127.0.0.1:${PRIVATE_PORT}`);
});