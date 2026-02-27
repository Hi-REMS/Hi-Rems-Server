const mysql = require('mysql2/promise');
require('dotenv').config();

const {
  MYSQL_HOST,
  MYSQL_PORT,
  MYSQL_DB,
  MYSQL_USER,
  MYSQL_PASS,
  MYSQL_SSL,
  MYSQL_CONN_LIMIT,
} = process.env;

const mysqlPool = mysql.createPool({
  host: MYSQL_HOST,
  port: Number(MYSQL_PORT),
  user: MYSQL_USER ,
  password: MYSQL_PASS,
  database: MYSQL_DB,
  connectionLimit: Number(MYSQL_CONN_LIMIT),
  waitForConnections: true,
  ssl: MYSQL_SSL === 'true' ? { rejectUnauthorized: true } : undefined,
  timezone: '+09:00',
  dateStrings: true,
});

mysqlPool.on('connection', (connection) => {
  connection.query('SET time_zone = "+09:00"');
});

module.exports = { mysqlPool };
