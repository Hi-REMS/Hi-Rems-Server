const { Pool } = require('pg');

const useSSL = String(process.env.DB_SSL || '').toLowerCase() === 'true';

const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 5432),
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  max: 30,           
  connectionTimeoutMillis: 10000,
  idleTimeoutMillis: 30000,

  ssl: useSSL ? { rejectUnauthorized: false } : false,
});

module.exports = { pool };
