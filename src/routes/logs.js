// src/routes/logs.js
// 로그 조회 API 라우트
// - GET /api/logs
//   • PostgreSQL 테이블 public."log_rtureceivelog" 에서 로그를 조회
//   • 필터링: imei, deviceModel, msgType
//   • 페이징: limit(최대 200), offset
//   • body는 최대 80자만 미리보기(body_preview)로 반환
// - 응답: JSON 배열 [{ id, time, deviceModel, msgType, ... }]

const express = require('express');
const router = express.Router();
const { pool } = require('../db/db.pg');
const rateLimit = require('express-rate-limit');
const logsLimiter = rateLimit({
  windowMs: 60 * 1000, // 1분
  max: 20,             // 1분당 최대 25회
  message: { error: 'Too many requests — try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});

/**
 * GET /api/logs
 * - 최근 로그를 조회
 * - 쿼리스트링:
 *   • limit (기본 50, 최대 200)
 *   • offset (기본 0)
 *   • imei (선택)
 *   • deviceModel (선택)
 *   • msgType (선택)
 */
router.get('/', logsLimiter, async (req, res, next) => {
  try {
    // --- 파라미터 처리 ---
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = Math.max(parseInt(req.query.offset || '0',  10), 0);
    const { imei, deviceModel, msgType } = req.query;

    // --- 조건절 빌드 ---
    const conds = [];
    const args  = [];
    if (imei)        { args.push(imei);        conds.push(`"rtuImei" = $${args.length}`); }
    if (deviceModel) { args.push(deviceModel); conds.push(`"deviceModel" = $${args.length}`); }
    if (msgType)     { args.push(msgType);     conds.push(`"msgType" = $${args.length}`); }
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';

    args.push(limit);  const limIdx = args.length;
    args.push(offset); const offIdx = args.length;

    const sql = `
      SELECT id, time, "deviceModel", "msgType", "opMode", "multiId", "rtuImei",
             "bodyLength",
             LEFT(body, 80) || CASE WHEN LENGTH(body) > 80 THEN '…' ELSE '' END AS body_preview
      FROM public."log_rtureceivelog"
      ${where}
      ORDER BY time DESC
      LIMIT $${limIdx} OFFSET $${offIdx}
    `;

    const { rows } = await pool.query(sql, args);
    res.json(rows);
  } catch (e) {
    next(e);
  }
});

module.exports = router;
