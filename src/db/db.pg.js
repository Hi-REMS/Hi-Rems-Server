const { Pool } = require('pg');
require('dotenv').config();

const {
  DB_HOST,
  DB_PORT = '5432',
  DB_USER,
  DB_PASS,
  DB_NAME,
  DB_SSL = 'false'
} = process.env;

const useSSL = String(DB_SSL).toLowerCase() === 'true';

const pool = new Pool({
  host: DB_HOST,
  port: Number(DB_PORT),
  user: DB_USER,
  password: DB_PASS,
  database: DB_NAME,
  max: 30,           
  connectionTimeoutMillis: 10000,
  idleTimeoutMillis: 30000,

  ssl: useSSL ? { rejectUnauthorized: false } : false,
});

module.exports = { pool };