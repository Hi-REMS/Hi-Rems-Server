// src/db/db.mysql.js
// MySQL/MariaDB 연결 풀 생성 모듈
// - 환경변수(.env)를 통해 접속 정보 로드
// - mysql2/promise 기반 커넥션 풀 제공
// - 다른 서비스 모듈에서 mysqlPool을 import 하여 DB 쿼리 수행 가능

const mysql = require('mysql2/promise');

const {
  MYSQL_HOST,
  MYSQL_PORT = '3307',
  MYSQL_DB,
  MYSQL_USER,
  MYSQL_PASS,
  MYSQL_SSL = 'false',
  MYSQL_CONN_LIMIT = '10',
} = process.env;

const ssl =
  String(MYSQL_SSL).toLowerCase() === 'true'
    ? { rejectUnauthorized: false }
    : undefined;

const mysqlPool = mysql.createPool({
  host: MYSQL_HOST,
  port: Number(MYSQL_PORT),
  user: MYSQL_USER,
  password: MYSQL_PASS,
  database: MYSQL_DB,
  waitForConnections: true,
  connectionLimit: Number(MYSQL_CONN_LIMIT),
  queueLimit: 0,
  ssl,
  connectTimeout: 10000,
  enableKeepAlive: true,
  keepAliveInitialDelay: 10000,
  timezone: 'Z',
});

module.exports = { mysqlPool };
