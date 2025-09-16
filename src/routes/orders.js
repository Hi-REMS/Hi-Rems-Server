// src/routes/orders.js
// 주문 데이터 조회 API 라우트
// - GET /api/orders
//   • PostgreSQL 테이블 public.orders 에서 주문 목록 조회
//   • 페이징 지원: limit(기본 50, 최대 200), offset
//   • 반환 필드: id, product, qty, status
// - 응답: JSON 배열 [{ id, product, qty, status }]


// 지도 띄울 때 필요한 로직

const express = require('express');
const router = express.Router();
const { pool } = require('../db/db.pg');

/**
 * GET /api/orders
 * - 주문 목록을 조회
 * - 쿼리스트링:
 *   • limit (기본 50, 최대 200)
 *   • offset (기본 0)
 */

router.get('/', async (req, res, next) => {
  try {
    // --- 파라미터 처리 ---
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = Math.max(parseInt(req.query.offset || '0',  10), 0);

    // --- SQL 실행 ---
    const { rows } = await pool.query(
      'SELECT id, product, qty, status FROM orders ORDER BY id DESC LIMIT $1 OFFSET $2',
      [limit, offset]
    );

    // --- 응답 ---
    res.json(rows);
  } catch (e) {
    next(e);
  }
});

module.exports = router;
