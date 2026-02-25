// executor/walletPool.js
const { ethers } = require("ethers");

class WalletPool {
  constructor({ provider, privateKeys, perWalletDelayMs = 1000 }) {
    if (!privateKeys.length) throw new Error("No PRIVATE_KEYS provided");
    this.provider = provider;
    this.wallets = privateKeys.map((pk) => new ethers.Wallet(pk, provider));
    this.perWalletDelayMs = perWalletDelayMs;

    this.nextIndex = 0;
    this.busyUntil = new Array(this.wallets.length).fill(0);
  }

  // pick first wallet that is free, else wait the shortest time
  async acquire() {
    while (true) {
      const now = Date.now();

      // try round-robin to be fair
      for (let tries = 0; tries < this.wallets.length; tries++) {
        const i = (this.nextIndex + tries) % this.wallets.length;
        if (now >= this.busyUntil[i]) {
          this.nextIndex = (i + 1) % this.wallets.length;
          this.busyUntil[i] = now + this.perWalletDelayMs;
          return this.wallets[i];
        }
      }

      // all busy => sleep until earliest free
      const earliest = Math.min(...this.busyUntil);
      const waitMs = Math.max(10, earliest - now);
      await new Promise((r) => setTimeout(r, waitMs));
    }
  }
}

module.exports = { WalletPool };