const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://chat_user:chat_password@localhost:5432/chat_db'
});

async function initDb() {
  const schemaQuery = `
    CREATE TABLE IF NOT EXISTS rooms (
      id SERIAL PRIMARY KEY,
      name VARCHAR(255) UNIQUE NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS messages (
      id SERIAL PRIMARY KEY,
      room_id INT REFERENCES rooms(id) ON DELETE CASCADE,
      sender VARCHAR(100) NOT NULL,
      content TEXT NOT NULL,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_messages_room_created 
    ON messages(room_id, created_at DESC);
  `;

  try {
    await pool.query(schemaQuery);
    console.log("Database tables and composite indices initialized successfully.");
    await pool.end();
  } catch (err) {
    console.error("Error initializing database:", err);
    process.exit(1);
  }
}

initDb();
