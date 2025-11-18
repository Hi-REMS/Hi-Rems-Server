// src/routes/orders.js
// 주문 데이터 조회 API 라우트
// - GET /api/orders
//   • PostgreSQL 테이블 public.orders 에서 주문 목록 조회
//   • 페이징 지원: limit(기본 50, 최대 200), offset
//   • 반환 필드: id, product, qty, status
// - 응답: JSON 배열 [{ id, product, qty, status }]

const express = require('express');
const router = express.Router();
const { pool } = require('../db/db.pg');
const rateLimit = require('express-rate-limit');

const ordersLimiter = rateLimit({
  windowMs: 60 * 1000, 
  max: 40,            
  message: { error: 'Too many requests — try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});


router.get('/', ordersLimiter, async (req, res, next) => {
  try {
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = Math.max(parseInt(req.query.offset || '0',  10), 0);
    const { rows } = await pool.query(
      'SELECT id, product, qty, status FROM orders ORDER BY id DESC LIMIT $1 OFFSET $2',
      [limit, offset]
    );

    res.json(rows);
  } catch (e) {
    next(e);
  }
});

module.exports = router;
