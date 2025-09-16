// src/db/db.pg.js
// PostgreSQL 연결 풀 생성 모듈
// - 환경변수(.env)를 통해 접속 정보 로드
// - pg 라이브러리 기반 커넥션 풀 제공
// - 다른 서비스 모듈에서 pool을 import 하여 DB 쿼리 수행 가능

const { Pool } = require('pg');

const useSSL = String(process.env.DB_SSL || '').toLowerCase() === 'true';

const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 5432),
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  max: 10,
  connectionTimeoutMillis: 10000,
  idleTimeoutMillis: 30000,
  ssl: useSSL ? { rejectUnauthorized: false } : false,
});

module.exports = { pool };
