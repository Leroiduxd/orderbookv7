// exposures.service.js
require('dotenv').config();
const { ethers } = require("ethers");

const provider = new ethers.providers.JsonRpcProvider(process.env.RPC_URL);
const coreAddress = process.env.CORE_ADDRESS;

const exposureAbi = [
  {
    "inputs": [{ "internalType": "uint32", "name": "", "type": "uint32" }],
    "name": "exposures",
    "outputs": [
      { "internalType": "int32", "name": "longLots", "type": "int32" },
      { "internalType": "int32", "name": "shortLots", "type": "int32" },
      { "internalType": "uint128", "name": "longValueSum", "type": "uint128" },
      { "internalType": "uint128", "name": "shortValueSum", "type": "uint128" },
      { "internalType": "uint128", "name": "longMaxProfit", "type": "uint128" },
      { "internalType": "uint128", "name": "shortMaxProfit", "type": "uint128" },
      { "internalType": "uint128", "name": "longMaxLoss", "type": "uint128" },
      { "internalType": "uint128", "name": "shortMaxLoss", "type": "uint128" }
    ],
    "stateMutability": "view",
    "type": "function"
  }
];

const contract = new ethers.Contract(coreAddress, exposureAbi, provider);

const assets = {
  6004:'aapl_usd', 6005:'amzn_usd', 6010:'coin_usd', 6003:'goog_usd',
  6011:'gme_usd', 6009:'intc_usd', 6059:'ko_usd', 6068:'mcd_usd',
  6001:'msft_usd', 6066:'ibm_usd', 6006:'meta_usd', 6002:'nvda_usd',
  6000:'tsla_usd', 5010:'aud_usd', 5000:'eur_usd', 5002:'gbp_usd',
  5013:'nzd_usd', 5011:'usd_cad', 5012:'usd_chf', 5001:'usd_jpy',
  5501:'xag_usd', 5500:'xau_usd', 0:'btc_usdt', 1:'eth_usdt',
  10:'sol_usdt', 14:'xrp_usdt', 5:'avax_usdt', 3:'doge_usdt',
  15:'trx_usdt', 16:'ada_usdt', 90:'sui_usdt', 2:'link_usdt',
  6034:'nike_usd', 6113:'spdia_usd', 6114:'qqqm_usd', 6115:'iwm_usd'
};

// Notre mémoire locale
let exposuresMemory = {};

async function fetchExposure(id) {
    const data = await contract.exposures(id);
    return {
        id: Number(id),
        name: assets[id],
        longLots: data.longLots.toString(),
        shortLots: data.shortLots.toString(),
        longValueSum: data.longValueSum.toString(),
        shortValueSum: data.shortValueSum.toString(),
        longMaxProfit: data.longMaxProfit.toString(),
        shortMaxProfit: data.shortMaxProfit.toString(),
        longMaxLoss: data.longMaxLoss.toString(),
        shortMaxLoss: data.shortMaxLoss.toString(),
    };
}

// Fonction pour tout mettre à jour
async function updateAllExposures() {
    console.log("[Exposures] Démarrage de la mise à jour complète depuis le RPC...");
    const ids = Object.keys(assets);
    try {
        const promises = ids.map(id => fetchExposure(id));
        const results = await Promise.all(promises);
        results.forEach(item => {
            exposuresMemory[item.id] = item;
        });
        console.log(`[Exposures] Mise à jour terminée en mémoire pour ${ids.length} actifs.`);
        return true;
    } catch (error) {
        console.error("[Exposures] Erreur lors de la mise à jour globale:", error);
        return false;
    }
}

// Fonction pour mettre à jour un seul actif
async function updateExposure(id) {
    if (!assets[id]) return false;
    try {
        const item = await fetchExposure(id);
        exposuresMemory[id] = item;
        return true;
    } catch (error) {
        console.error(`[Exposures] Erreur maj ID ${id}:`, error);
        return false;
    }
}

// Fonction synchrone pour lire la mémoire sans bloquer
function getAllExposures() {
    return exposuresMemory;
}

// Lancement automatique de la première mise à jour quand le fichier est chargé par Node
updateAllExposures();

// On exporte les fonctions pour qu'elles soient utilisables par d'autres fichiers en interne
module.exports = {
    updateAllExposures,
    updateExposure,
    getAllExposures
};