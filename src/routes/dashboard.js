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

/**
 * 건강 지표(오프라인/비정상/저하/회복 추정 등)
 * - 쿼리스트링:
 *   - lookbackDays: 최근 N일 기준 최신 상태를 판정(기본 1)
 *   - full=1       : 저하/회복 지표까지 정밀 집계 수행(비용↑)
 */
router.get('/health-counters', async (req, res, next) => {
  const lookbackDays = Math.max(parseInt(req.query.lookbackDays || '1', 10), 1);
  const full = String(req.query.full || '') === '1';

  try {
    // 최근 N일 기록에서 장치별 마지막 상태를 뽑아
    // - opMode 비정상 수
    // - 15분 이상 미수신(오프라인) 수
    // - (비정상 또는 오프라인) 수
    const { rows: base } = await pool.query(
      `
      WITH recent AS (
        SELECT "rtuImei", "opMode", "time"
        FROM public."log_rtureceivelog"
        WHERE "time" >= NOW() - ($1::text || ' days')::interval
      ),
      max_t AS (
        SELECT "rtuImei", MAX("time") AS max_time
        FROM recent
        GROUP BY "rtuImei"
      ),
      last AS (
        SELECT r."rtuImei", r."opMode", r."time" AS last_time
        FROM recent r
        JOIN max_t m
          ON m."rtuImei" = r."rtuImei"
         AND m.max_time   = r."time"
      )
      SELECT
        COUNT(*)::int                                                          AS total,
        COUNT(*) FILTER (WHERE "opMode" <> '0')::int                           AS opmode_abnormal,
        COUNT(*) FILTER (WHERE last_time < NOW() - INTERVAL '15 minutes')::int AS offline,
        COUNT(*) FILTER (WHERE "opMode" <> '0'
                         OR last_time < NOW() - INTERVAL '15 minutes')::int    AS status_abnormal
      FROM last
      `,
      [lookbackDays]
    );

    // 기본 응답(빠른 집계)
    let payload = {
      status_abnormal: base[0]?.status_abnormal ?? 0, // 비정상+오프라인 합계
      opmode_abnormal: base[0]?.opmode_abnormal ?? 0, // opMode 비정상
      offline:         base[0]?.offline ?? 0,         // 오프라인(15분 이상 미수신)
      degraded_devices: 0,                             // 아래 full=1에서 채움
      recovering:       0,                             // 아래 full=1에서 채움
      recovered_today:  0,                             // 아래 full=1에서 채움
      mode: 'fast',
      lookbackDays
    };

    // full=1 일 때만 비용 큰 추가 집계 수행
    if (full) {
      // 최근 3시간 이내 활동한 장치 중, 최근 1시간 메시지량이 과거 3일 평균의 70% 미만 → 저하로 간주
      const { rows: d } = await pool.query(
        `
        WITH last AS (
          SELECT "rtuImei", MAX("time") AS last_time
          FROM public."log_rtureceivelog"
          WHERE "time" >= NOW() - INTERVAL '3 days'
          GROUP BY "rtuImei"
        ),
        active AS (
          SELECT "rtuImei" FROM last
          WHERE last_time >= NOW() - INTERVAL '3 hours'
        ),
        today1h AS (
          SELECT r."rtuImei", COUNT(*) AS c_now
          FROM public."log_rtureceivelog" r
          JOIN active a ON a."rtuImei" = r."rtuImei"
          WHERE r."time" >= NOW() - INTERVAL '60 minutes'
          GROUP BY 1
        ),
        hist AS (
          SELECT r."rtuImei", COUNT(*)/3.0 AS c_avg
          FROM public."log_rtureceivelog" r
          JOIN active a ON a."rtuImei" = r."rtuImei"
          WHERE "time" >= NOW() - INTERVAL '3 days'
            AND "time" <  NOW() - INTERVAL '60 minutes'
          GROUP BY 1
        )
        SELECT COUNT(*)::int AS degraded_devices
        FROM today1h t
        JOIN hist h ON h."rtuImei" = t."rtuImei"
        WHERE h.c_avg > 0 AND t.c_now < h.c_avg * 0.7
        `
      );

      // 최근 1일 내 마지막 수신이 10분 이내 → 회복 중으로 간주
      const { rows: rcv } = await pool.query(
        `
        WITH last AS (
          SELECT "rtuImei", MAX("time") AS last_time
          FROM public."log_rtureceivelog"
          WHERE "time" >= NOW() - INTERVAL '1 day'
          GROUP BY "rtuImei"
        )
        SELECT COUNT(*) FILTER (WHERE last_time >= NOW() - INTERVAL '10 minutes')::int AS recovering
        FROM last
        `
      );

      // 오늘(KST) 안에 최초로 정상(opMode='0') 상태를 기록한 장치 수 → 금일 회복 수
      const { rows: rcv2 } = await pool.query(
        `
        WITH t AS (
          SELECT "rtuImei",
                 MIN("time") FILTER (WHERE "opMode" = '0') AS first_ok
          FROM public."log_rtureceivelog"
          WHERE ("time" AT TIME ZONE 'Asia/Seoul')::date = (NOW() AT TIME ZONE 'Asia/Seoul')::date
          GROUP BY "rtuImei"
        )
        SELECT COUNT(*)::int AS recovered_today FROM t WHERE first_ok IS NOT NULL
        `
      );

      payload = {
        ...payload,
        degraded_devices: d[0]?.degraded_devices ?? 0,
        recovering:       rcv[0]?.recovering ?? 0,
        recovered_today:  rcv2[0]?.recovered_today ?? 0,
        mode: 'full'
      };
    }

    res.json(payload);
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
