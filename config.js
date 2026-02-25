// config.js
module.exports = {
    // RPC / WSS
    RPC_URL: "https://atlantic.dplabs-internal.com",
    WSS_URL: "wss://atlantic.dplabs-internal.com", // optionnel, pas utilisé par sync.js mais utile plus tard pour listener
  
    // Contracts
    CORE_ADDRESS: "0x2F3B27EFeBDa093b48b568d6F2C3aad7F64f7DEc",
    PAYMASTER_ADDRESS: "0xC7eA1B52D20d0B4135ae5cc8E4225b3F12eA279B",
  
    // Local private write server
    WRITE_BASE_URL: "http://127.0.0.1:3001",
  
    // Local sqlite file (used ONLY for repair scan)
    DB_PATH: "./trades.db",
  
    // Limits you requested
    MAX_IDS_PER_RPC_CALL: 50,
    MAX_RPC_CONCURRENCY: 20,
  
    // Write batching (your endpoint max 2000)
    MAX_TRADES_PER_WRITE_BATCH: 2000,
  };