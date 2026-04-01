// services/wss.service.js
const WebSocket = require('ws');

function initWss(server) {
  // On attache le WebSocket au serveur HTTP existant, sur une route précise
  const wss = new WebSocket.Server({ server, path: '/stream' });

  console.log("[WSS] WebSocket initialisé sur la route /stream");

  // --- GESTION DU PING/PONG ---
  const HEARTBEAT_INTERVAL = 10000; // 10 secondes

  wss.on('connection', (ws) => {
    ws.isAlive = true;
    
    ws.on('pong', () => {
      ws.isAlive = true;
    });
  });

  const pingInterval = setInterval(() => {
    wss.clients.forEach((ws) => {
      if (ws.isAlive === false) {
        return ws.terminate();
      }
      ws.isAlive = false;
      ws.ping();
    });
  }, HEARTBEAT_INTERVAL);

  wss.on('close', () => {
    clearInterval(pingInterval);
  });

  // --- FETCH ET BROADCAST ---
  async function fetchAndBroadcast() {
    try {
      const response = await fetch('https://metadata-backend.ostium.io/PricePublish/latest-prices');
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      
      const data = await response.json();
      const messageString = JSON.stringify(data);

      wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(messageString);
        }
      });
    } catch (error) {
      console.error('[WSS] Erreur Ostium:', error.message);
    }
  }

  // Boucle d'1 seconde
  setInterval(fetchAndBroadcast, 1000);
}

module.exports = { initWss };