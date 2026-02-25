#!/usr/bin/env node
/**
 * cron/sync.cron.js
 * Lance des sync périodiques (states + sltp).
 * - Toutes les heures: range 1..maxExistingId
 * - Optionnel: au démarrage, lance une première fois après 10s
 */

const cron = require("node-cron");
const { spawn } = require("child_process");

function runSync(args, label) {
  return new Promise((resolve) => {
    const p = spawn("node", ["sync.js", ...args], { stdio: "inherit" });
    p.on("close", (code) => {
      console.log(`[cron] ${label} exit code=${code}`);
      resolve(code);
    });
  });
}

async function runHourly() {
  // On utilise --range 1 N (N= count), et sync.js va filter <= maxExistingId onchain.
  // Donc on met un count "très grand" et le script coupera tout seul.
  // Mais ici on veut plutôt faire: range 1 1000000, comme ça ça couvre tout.
  // Si tu veux être plus strict, je te donne une variante après.
  const COUNT_BIG = 1000000;

  console.log(`[cron] Hourly sync starting @ ${new Date().toISOString()}`);

  // 1) states (et full fetch si état change, comme ton code le fait)
  await runSync(["--mode", "states", "--range", "1", String(COUNT_BIG)], "states");

  // 2) sltp
  await runSync(["--mode", "sltp", "--range", "1", String(COUNT_BIG)], "sltp");

  console.log(`[cron] Hourly sync done @ ${new Date().toISOString()}`);
}

// Petit run au démarrage (pratique après reboot)
setTimeout(() => {
  runHourly().catch((e) => console.error("[cron] startup run error:", e));
}, 10_000);

// Toutes les heures à minute 0 (UTC locale machine)
cron.schedule("*/10 * * * *", () => {
  runHourly().catch((e) => console.error("[cron] hourly error:", e));
});

console.log("[cron] Sync cron started. Schedule: 0 * * * *");