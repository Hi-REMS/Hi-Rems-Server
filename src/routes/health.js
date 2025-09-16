// src/routes/health.js
// 서버 및 데이터베이스 헬스체크 라우트
// - GET /api/health : 서버 응답 확인 및 PostgreSQL 연결 상태 점검
// - 응답: { ok: true, db_now: '현재 DB 시각' } 또는 { ok: false, error }

const express = require('express');
const router = express.Router();

// PostgreSQL 연결 풀
const { pool } = require('../db/db.pg');

// (선택) MySQL/MariaDB 연결 풀도 가져올 수 있음
const { mysqlPool } = require('../db/db.mysql');

/**
 * GET /api/health
 * - 서버와 DB 연결 여부를 확인
 * - PostgreSQL에서 NOW()를 조회하여 응답
 */
router.get('/', async (_req, res) => {
  try {
    const { rows } = await pool.query('SELECT NOW() AS now');
    res.json({ ok: true, db_now: rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

module.exports = router;
