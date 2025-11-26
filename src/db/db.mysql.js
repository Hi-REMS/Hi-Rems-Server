const mysql = require('mysql2/promise');
require('dotenv').config();

const {
  MYSQL_HOST,
  MYSQL_PORT = '3307',
  MYSQL_DB,
  MYSQL_USER,
  MYSQL_PASS,
  MYSQL_SSL = 'false',
  MYSQL_CONN_LIMIT = '10',
} = process.env;

const mysqlPool = mysql.createPool({
  host: MYSQL_HOST || '127.0.0.1',
  port: Number(MYSQL_PORT) || 3307,
  user: MYSQL_USER || 'root',
  password: MYSQL_PASS || '',
  database: MYSQL_DB || 'rems',
  connectionLimit: Number(MYSQL_CONN_LIMIT) || 10,
  waitForConnections: true,
  ssl: MYSQL_SSL === 'true' ? { rejectUnauthorized: true } : undefined,
});

module.exports = { mysqlPool };
