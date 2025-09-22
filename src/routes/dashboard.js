// src/routes/dashboard.js
// 대시보드 요약/건강 지표 API 라우트
// - GET /dashboard/basic           : 플랜트/장치 개수 및 금일 메시지/장치 통계
// - GET /dashboard/health-counters : 오프라인/비정상/저하 장치 카운트(빠른 집계 + 선택적 정밀 집계)
// 사용 테이블: public."log_rtureceivelog"
// 시간 기준: KST 경계(하루 시작/끝)를 UTC로 변환하여 집계

const express = require('express');
const router = express.Router();
const { pool } = require('../db/db.pg');
const { getNationwideEnergySummary } = require('../energy/summary');

/**
 * 기본 대시보드 지표
 * - lookbackDays(기본 3일) 동안 각 장치의 최신 opMode를 기준으로 정상/비정상 카운트
 * - 오늘(KST 00:00~24:00) 수신된 전체 메시지 수, 참여 장치 수
 */
router.get('/basic', async (req, res, next) => {
  try {
    // 조회 범위 일수 (최소 1일)
    const lookbackDays = Math.max(parseInt(req.query.lookbackDays || '3', 10), 1);

    // 최근 N일 동안 장치별 최신 상태(opMode) 뽑아서 정상/비정상 카운트
    const { rows: statusRows } = await pool.query(
      `
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
      `,
      [lookbackDays]
    );

    // 금일(KST)의 총 메시지/장치 수 (KST 경계를 UTC로 만들어 집계)
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
        total_plants:    statusRows[0]?.total_plants    ?? 0, // 장치/플랜트 총개수
        normal_plants:   statusRows[0]?.normal_plants   ?? 0, // opMode='0' (정상)
        abnormal_plants: statusRows[0]?.abnormal_plants ?? 0, // opMode!='0' (비정상)
      },
      today: {
        total_messages: todayRows[0]?.total_messages ?? 0, // 금일 메시지 수
        devices:        todayRows[0]?.devices        ?? 0, // 금일 참여 장치 수
      },
    });
  } catch (e) {
    next(e);
  }
});

let energyCache = { data: null, ts: 0 };
const ENERGY_CACHE_MS = Number(process.env.ENERGY_CACHE_MS || '60000'); // 60s 기본

router.get('/energy', async (_req, res, next) => {
  const now = Date.now();
  if (energyCache.data && (now - energyCache.ts < ENERGY_CACHE_MS)) {
    return res.json({ ok: true, data: energyCache.data, cached: true });
  }
  try {
    const data = await getNationwideEnergySummary();
    energyCache = { data, ts: now };
    res.json({ ok: true, data, cached: false });
  } catch (e) {
    next(e);
  }
});


module.exports = router;
