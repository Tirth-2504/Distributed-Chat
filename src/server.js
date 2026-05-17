const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { Pool } = require('pg');
const { createClient } = require('redis');
require('dotenv').config();

const PORT = process.argv[2] || process.env.PORT || 3000;
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Database Connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// Redis Cluster Pub/Sub Client Connections
const pubClient = createClient({ url: process.env.REDIS_URL });
const subClient = pubClient.duplicate();

// Track active local socket connections mapped by their target chat room
const roomSubscriptions = new Map(); 

async function startServer() {
  await pubClient.connect();
  await subClient.connect();
  console.log(`Redis connected on Port ${PORT}`);

  // Global Redis Layer Listener: Captures cluster updates and pushes to local sockets
  await subClient.subscribe('CHAT_CHANNEL', (message) => {
    const parsedData = JSON.parse(message);
    const { roomId, sender, content, createdAt } = parsedData;
    
    const localClients = roomSubscriptions.get(roomId) || [];
    localClients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({ roomId, sender, content, createdAt }));
      }
    });
  });

  // Client WebSocket Connection Handler
  wss.on('connection', (ws) => {
    let currentRoomId = null;

    ws.on('message', async (messageData) => {
      try {
        const data = JSON.parse(messageData);
        
        // Handle User Joining Room
        if (data.type === 'JOIN') {
          currentRoomId = data.roomId;
          if (!roomSubscriptions.has(currentRoomId)) {
            roomSubscriptions.set(currentRoomId, []);
          }
          roomSubscriptions.get(currentRoomId).push(ws);
          
          // Optimization: Fetch historical messages via composite index pipeline
          const history = await pool.query(
            'SELECT sender, content, created_at FROM messages WHERE room_id = $1 ORDER BY created_at ASC LIMIT 50',
            [currentRoomId]
          );
          ws.send(JSON.stringify({ type: 'HISTORY', messages: history.rows }));
          return;
        }

        // Handle Messaging Pipeline
        if (data.type === 'MESSAGE' && currentRoomId) {
          const { sender, content } = data;
          
          // Step 1: Write directly to persistent database
          const dbResult = await pool.query(
            'INSERT INTO messages (room_id, sender, content) VALUES ($1, $2, $3) RETURNING created_at',
            [currentRoomId, sender, content]
          );
          
          // Step 2: Publish to Redis cluster to alert all instances
          const payload = {
            roomId: currentRoomId,
            sender,
            content,
            createdAt: dbResult.rows[0].created_at
          };
          await pubClient.publish('CHAT_CHANNEL', JSON.stringify(payload));
        }
      } catch (err) {
        console.error('Socket communication processing error:', err);
      }
    });

    ws.on('close', () => {
      if (currentRoomId && roomSubscriptions.has(currentRoomId)) {
        const filtered = roomSubscriptions.get(currentRoomId).filter(client => client !== ws);
        roomSubscriptions.set(currentRoomId, filtered);
      }
    });
  });

  server.listen(PORT, () => {
    console.log(`Server instance operating safely on port: ${PORT}`);
  });
}

startServer().catch(console.error);
