// src/db/db.routes.test.js
// DB 연결 테스트 라우트 모듈
// - PostgreSQL 연결 확인 (/ping-db)
// - MySQL/MariaDB 연결 확인 (/ping-mysql)
// - 실제 서비스 라우터와는 별도로 DB 연결 상태를 점검하거나 개발/테스트용으로 사용

const express = require('express');
const router = express.Router();

const { pool } = require('./db.pg');
const { mysqlPool } = require('./db.mysql');

// PostgreSQL 핑
router.get('/ping-db', async (_req, res, next) => {
  try {
    const { rows } = await pool.query(
      'SELECT 1 AS ok, NOW() AS now, version() AS version'
    );
    res.json(rows[0]);
  } catch (e) { next(e); }
});

// MySQL/MariaDB 핑
router.get('/ping-mysql', async (_req, res, next) => {
  try {
    const [rows] = await mysqlPool.query(
      'SELECT 1 AS ok, NOW() AS `now`, VERSION() AS `version`'
    );
    res.json(rows[0]);
  } catch (e) { next(e); }
});

module.exports = router;
