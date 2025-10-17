// src/routes/dashboard.js
// 대시보드 요약/건강 지표 API 라우트
// - GET /dashboard/basic
// - GET /dashboard/energy

const express = require('express');
const router = express.Router();
const { pool } = require('../db/db.pg');
const rateLimit = require('express-rate-limit');

const path = require('path');
const { getCache } = require(path.join(__dirname, '../jobs/energyRefresh'));
const { getNationwideEnergySummary } = require('../energy/summary');

// ---------------------
// Rate limiters
// ---------------------
const limiterBasic = rateLimit({
  windowMs: 60 * 1000,
  max: 20,
  message: { error: 'Too many requests — try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});

const limiterEnergy = rateLimit({
  windowMs: 60 * 1000,
  max: 10,
  message: { error: 'Too many requests — try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});

/**
 * 기본 대시보드 지표
 */
router.get('/basic', limiterBasic, async (req, res, next) => {
  try {
    const lookbackDays = Math.max(parseInt(req.query.lookbackDays || '3', 10), 1);

    const { rows: statusRows } = await pool.query(`
      WITH recent_latest AS (
        SELECT DISTINCT ON ("rtuImei")
               "rtuImei", "opMode", "time"
        FROM public."log_rtureceivelog"
        WHERE "time" >= NOW() - ($1::text || ' days')::interval
        ORDER BY "rtuImei", "time" DESC
      )
      SELECT
        COUNT(*)::int                                           AS total_plants,
        COUNT(*) FILTER (WHERE "opMode" = '0')::int             AS normal_plants,
        COUNT(*) FILTER (WHERE "opMode" <> '0')::int            AS abnormal_plants
      FROM recent_latest;
    `, [lookbackDays]);

    const { rows: todayRows } = await pool.query(`
      WITH bounds AS (
        SELECT
          (date_trunc('day', (now() AT TIME ZONE 'Asia/Seoul')) AT TIME ZONE 'Asia/Seoul') AS kst_start_utc,
          ((date_trunc('day', (now() AT TIME ZONE 'Asia/Seoul')) + interval '1 day') AT TIME ZONE 'Asia/Seoul') AS kst_end_utc
      )
      SELECT
        (SELECT COUNT(*)::int
           FROM public."log_rtureceivelog", bounds b
           WHERE "time" >= b.kst_start_utc AND "time" < b.kst_end_utc) AS total_messages,
        (SELECT COUNT(DISTINCT "rtuImei")::int
           FROM public."log_rtureceivelog", bounds b
           WHERE "time" >= b.kst_start_utc AND "time" < b.kst_end_utc) AS devices;
    `);

    res.json({
      totals: {
        total_plants:    statusRows[0]?.total_plants    ?? 0,
        normal_plants:   statusRows[0]?.normal_plants   ?? 0,
        abnormal_plants: statusRows[0]?.abnormal_plants ?? 0,
      },
      today: {
        total_messages: todayRows[0]?.total_messages ?? 0,
        devices:        todayRows[0]?.devices        ?? 0,
      },
    });
  } catch (e) {
    next(e);
  }
});

/**
 * 전국 에너지 요약 (크론 캐시 기반)
 */
router.get('/energy', limiterEnergy, async (_req, res, next) => {
  try {
    const cache = getCache();

    if (cache?.electric && cache?.thermal) {
      return res.json({
        ok: true,
        data: { electric: cache.electric, thermal: cache.thermal },
        cached: true,
        updatedAt: cache.updatedAt,
      });
    }

    const data = await getNationwideEnergySummary();
    res.json({ ok: true, data, cached: false });
  } catch (e) {
    next(e);
  }
});

module.exports = router;
