// src/routes/dashboard.js
// 대시보드 요약/건강 지표 API 라우트 (지역 위험도 집계 + 5분 캐시 포함)

const express = require('express');
const router = express.Router();
const { pool } = require('../db/db.pg');
const rateLimit = require('express-rate-limit');

// (선택) MySQL REMS 주소/업체 정보까지 묶고 싶다면 주석 해제
 const { mysqlPool } = require('../db/db.mysql');

const TTL_MS = 5 * 60 * 1000; // ✅ 5분 캐시
const cache = new Map();
const setCache = (key, data, ttl = TTL_MS) =>
  cache.set(key, { data, exp: Date.now() + ttl });
const getCache = (key) => {
  const v = cache.get(key);
  if (v && v.exp > Date.now()) return v.data;
  if (v) cache.delete(key);
  return null;
};

setInterval(() => {
  for (const [k, v] of cache.entries()) {
    if (!v || v.exp <= Date.now()) cache.delete(k);
  }
}, 15 * 60 * 1000).unref();
// ──────────────────────────────────────────────────────────────
// Rate limiters
// ──────────────────────────────────────────────────────────────
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

const limiterAbnormal = rateLimit({
  windowMs: 60 * 1000,
  max: 15,
  message: { error: 'Too many requests — try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});

// ──────────────────────────────────────────────────────────────
// 최신 상태 CTE (두 가지 버전)
// - WithFault: faultFlags/fault_flag/fault 같은 컬럼이 있을 때만 사용 가능
// - NoFault  : opMode만 사용 (fault 관련 컬럼 전혀 없을 때 사용)
// ──────────────────────────────────────────────────────────────
function latestStatusCteWithFault() {
  return `
    WITH recent_latest AS (
      SELECT DISTINCT ON ("rtuImei")
             "rtuImei",
             "opMode",
             COALESCE("faultFlags", "fault_flag", "fault", 0) AS fault_flags,
             public."log_rtureceivelog"."time" AS last_time
      FROM public."log_rtureceivelog"
      WHERE public."log_rtureceivelog"."time" >= NOW() - ($1::text || ' days')::interval
      ORDER BY "rtuImei", public."log_rtureceivelog"."time" DESC
    )
  `;
}

function latestStatusCteNoFault() {
  return `
    WITH recent_latest AS (
      SELECT DISTINCT ON ("rtuImei")
             "rtuImei",
             "opMode",
             0::int AS fault_flags,
             public."log_rtureceivelog"."time" AS last_time
      FROM public."log_rtureceivelog"
      WHERE public."log_rtureceivelog"."time" >= NOW() - ($1::text || ' days')::interval
      ORDER BY "rtuImei", public."log_rtureceivelog"."time" DESC
    )
  `;
}


// ──────────────────────────────────────────────────────────────
// 주소 파싱/조인 유틸
//  - parseKoreanAddress: 아주 단순히 "시/도 + 시/군/구"만 추출
//  - fetchAddressMap: IMEI → {address, sido, sigungu} 매핑을 Postgres 캐시 테이블
//    (public.imei_meta)에서 우선 시도. 없으면 (옵션) MySQL에서 조회.
//    imei_meta 테이블이 없다면 try/catch 안에서 자동 스킵.
//    imei_meta 스키마 예시:
//      CREATE TABLE public.imei_meta(
//        imei text PRIMARY KEY,
//        address text,
//        sido text,
//        sigungu text,
//        lat double precision,
//        lon double precision,
//        updated_at timestamptz default now()
//      );
// ──────────────────────────────────────────────────────────────
function parseKoreanAddress(addr = '') {
  const t = String(addr || '').replace(/\s*\(.*?\)\s*/g, '').trim();
  if (!t) return { sido: '미지정', sigungu: '' };
  const parts = t.split(/\s+/);
  const sidoRaw = parts[0] || '미지정';
  const sigungu = parts[1] || '';
  return { sido: normalizeSido(sidoRaw), sigungu };
}


function normalizeSido(sido) {
  const map = {
    '강원': '강원도',
    '강원특별자치도': '강원도',
    '제주특별자치도': '제주도',
    '경남': '경상남도',
    '경북': '경상북도',
    '전남': '전라남도',
    '전북': '전라북도',
    '충남': '충청남도',
    '충북': '충청북도',
    '서울특별시': '서울',
    '부산광역시': '부산',
    '대구광역시': '대구',
    '인천광역시': '인천',
    '광주광역시': '광주',
    '대전광역시': '대전',
    '울산광역시': '울산',
    '세종특별자치시': '세종'
  };
  return map[sido] || sido || '미지정';
}


async function fetchAddressMap(imeis) {
  const result = new Map();
  if (!imeis?.length) return result;

  // 1) Postgres 캐시 테이블 우선
  try {
    const placeholders = imeis.map((_, i) => `$${i + 1}`).join(',');
    const { rows } = await pool.query(
      `SELECT imei, address, sido, sigungu
         FROM public.imei_meta
        WHERE imei IN (${placeholders})`,
      imeis
    );
    for (const r of rows) {
      result.set(r.imei, {
        address: r.address || '',
        sido: r.sido || '',
        sigungu: r.sigungu || '',
      });
    }
  } catch (_) {
    // imei_meta 없음 → 무시
  }

  // 2) (옵션) MySQL 메타로 보완
  // if (result.size < imeis.length && mysqlPool) {
  //   const remain = imeis.filter((id) => !result.has(id));
  //   if (remain.length) {
  //     const [metaRows] = await mysqlPool.query(
  //       `SELECT rtu_id AS imei, address
  //          FROM rems_rems
  //         WHERE rtu_id IN (${remain.map(() => '?').join(',')})`,
  //       remain
  //     );
  //     for (const m of metaRows) {
  //       const { sido, sigungu } = parseKoreanAddress(m.address || '');
  //       result.set(m.imei, {
  //         address: m.address || '',
  //         sido,
  //         sigungu,
  //       });
  //     }
  //   }
  // }

  return result;
}

// ──────────────────────────────────────────────────────────────
// 1) 기본 대시보드 지표 (5분 캐시)
//  - Bit0(고장) 우선 사용, 실패 시 opMode 기준으로 폴백
//  - ?nocache=1 로 캐시 무시 가능
// ──────────────────────────────────────────────────────────────
// ──────────────────────────────────────────────────────────────
// 1) 기본 대시보드 지표 (5분 캐시)
//  - Bit0(고장) 우선 사용, 실패 시 opMode(정수) 기준으로 폴백
//  - ?nocache=1 로 캐시 무시 가능
// ──────────────────────────────────────────────────────────────
router.get('/basic', limiterBasic, async (req, res, next) => {
  try {
    const lookbackDays = Math.max(parseInt(req.query.lookbackDays || '30', 10), 1);
    const noCache = String(req.query.nocache || '') === '1';
    const cacheKey = `basic:${lookbackDays}`;

    if (!noCache) {
      const c = getCache(cacheKey);
      if (c) return res.json(c);
    }

    const tryFaultBitSql = `
      ${latestStatusCteWithFault()}
      SELECT
        COUNT(*)::int AS total_plants,
        COUNT(*) FILTER (
          WHERE (fault_flags & 1) = 0
            AND COALESCE(("opMode")::int, 0) = 0
        )::int AS normal_plants,
        COUNT(*) FILTER (
          WHERE (fault_flags & 1) = 1
             OR COALESCE(("opMode")::int, 0) <> 0
        )::int AS abnormal_plants
      FROM recent_latest;
    `;

    const fallbackSql = `
      ${latestStatusCteNoFault()}
      SELECT
        COUNT(*)::int AS total_plants,
        COUNT(*) FILTER (
          WHERE COALESCE(("opMode")::int, 0) = 0
        )::int AS normal_plants,
        COUNT(*) FILTER (
          WHERE COALESCE(("opMode")::int, 0) <> 0
        )::int AS abnormal_plants
      FROM recent_latest;
    `;

    let statusRows = [];
    try {
      const { rows } = await pool.query(tryFaultBitSql, [lookbackDays]);
      statusRows = rows;
    } catch {
      const { rows } = await pool.query(fallbackSql, [lookbackDays]);
      statusRows = rows;
    }

    // KST 당일 집계 (UTC 보정)
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

    const totalsRow = statusRows?.[0] || {};
    const payload = {
      totals: {
        total_plants:    totalsRow.total_plants    ?? 0,
        normal_plants:   totalsRow.normal_plants   ?? 0,
        abnormal_plants: totalsRow.abnormal_plants ?? 0,
      },
      today: {
        total_messages: todayRows?.[0]?.total_messages ?? 0,
        devices:        todayRows?.[0]?.devices        ?? 0,
      },
      cached: false,
    };

    setCache(cacheKey, { ...payload, cached: true });
    res.json(payload);
  } catch (e) {
    next(e);
  }
});


// ──────────────────────────────────────────────────────────────
// 2) 이상 발전소 목록 (상세)  — 리스트는 실시간성이 있어 캐시 X
//  - reason/priority/since 분류
//  - 정렬: severity DESC → minutes_since DESC
//  - 파라미터: lookbackDays, offlineMin, limit, offset
//  - fault* 컬럼이 없으면 자동으로 opMode-only 대안 쿼리 수행
//  - (옵션) IMEI→주소 메타 조인 가능 (아래 주석 참고)
// ──────────────────────────────────────────────────────────────
router.get('/abnormal/list', limiterAbnormal, async (req, res, next) => {
  try {
    const lookbackDays = Math.max(parseInt(req.query.lookbackDays || '3', 10), 1);
    const offlineMin   = Math.max(parseInt(req.query.offlineMin   || '90', 10), 10);
    const limit        = Math.min(parseInt(req.query.limit        || '50', 10), 200);
    const offset       = Math.max(parseInt(req.query.offset       || '0',  10), 0);

    const withFaultSql = `
      ${latestStatusCteWithFault()}
      , annotated AS (
        SELECT
          r."rtuImei"              AS imei,
          r."opMode"               AS op_mode,
          r.fault_flags            AS fault_flags,
          r.last_time,
          EXTRACT(EPOCH FROM (NOW() - r.last_time))/60.0 AS minutes_since,
          CASE
            WHEN (r.fault_flags & 1) = 1 THEN 'FAULT_BIT'
            WHEN EXTRACT(EPOCH FROM (NOW() - r.last_time))/60.0 >= $2 THEN 'OFFLINE'
            WHEN r."opMode" <> '0' THEN 'OPMODE_ABNORMAL'
            ELSE 'NORMAL'
          END AS reason,
          CASE
            WHEN (r.fault_flags & 1) = 1 THEN 3
            WHEN EXTRACT(EPOCH FROM (NOW() - r.last_time))/60.0 >= $2 THEN 2
            WHEN r."opMode" <> '0' THEN 1
            ELSE 0
          END AS severity
        FROM recent_latest r
      )
      , with_counts AS (
        SELECT
          a.*,
          (SELECT COUNT(*) FROM public."log_rtureceivelog" lr
            WHERE lr."rtuImei" = a.imei
              AND lr."time" >= NOW() - interval '24 hours')::int AS msgs_24h
        FROM annotated a
      )
      SELECT
        imei,
        op_mode,
        fault_flags,
        last_time,
        ROUND(minutes_since::numeric, 1) AS minutes_since,
        reason,
        severity,
        msgs_24h
      FROM with_counts
      WHERE reason <> 'NORMAL'
      ORDER BY severity DESC, minutes_since DESC
      LIMIT $3 OFFSET $4
    `;

    const noFaultSql = `
      ${latestStatusCteNoFault()}
      , annotated AS (
        SELECT
          r."rtuImei"              AS imei,
          r."opMode"               AS op_mode,
          r.fault_flags            AS fault_flags, -- 항상 0
          r.last_time,
          EXTRACT(EPOCH FROM (NOW() - r.last_time))/60.0 AS minutes_since,
          CASE
            WHEN EXTRACT(EPOCH FROM (NOW() - r.last_time))/60.0 >= $2 THEN 'OFFLINE'
            WHEN r."opMode" <> '0' THEN 'OPMODE_ABNORMAL'
            ELSE 'NORMAL'
          END AS reason,
          CASE
            WHEN EXTRACT(EPOCH FROM (NOW() - r.last_time))/60.0 >= $2 THEN 2
            WHEN r."opMode" <> '0' THEN 1
            ELSE 0
          END AS severity
        FROM recent_latest r
      )
      , with_counts AS (
        SELECT
          a.*,
          (SELECT COUNT(*) FROM public."log_rtureceivelog" lr
            WHERE lr."rtuImei" = a.imei
              AND lr."time" >= NOW() - interval '24 hours')::int AS msgs_24h
        FROM annotated a
      )
      SELECT
        imei,
        op_mode,
        fault_flags,
        last_time,
        ROUND(minutes_since::numeric, 1) AS minutes_since,
        reason,
        severity,
        msgs_24h
      FROM with_counts
      WHERE reason <> 'NORMAL'
      ORDER BY severity DESC, minutes_since DESC
      LIMIT $3 OFFSET $4
    `;

    let rows;
    try {
      ({ rows } = await pool.query(withFaultSql, [lookbackDays, offlineMin, limit, offset]));
    } catch (e1) {
      ({ rows } = await pool.query(noFaultSql, [lookbackDays, offlineMin, limit, offset]));
    }

    // ✅ (선택) 주소 메타 조인
    // const imeis = rows.map(r => r.imei);
    // const metaMap = await fetchAddressMap(imeis);
    // rows = rows.map(r => {
    //   const m = metaMap.get(r.imei);
    //   return m ? { ...r, address: m.address, sido: m.sido, sigungu: m.sigungu } : r;
    // });

    res.json({ items: rows, limit, offset, lookbackDays, offlineMin });
  } catch (e) {
    next(e);
  }
});

// ──────────────────────────────────────────────────────────────
// 3) 이상 발전소 요약 브레이크다운 (5분 캐시)
//  - reason별 카운트
//  - ?nocache=1 로 캐시 무시 가능
// ──────────────────────────────────────────────────────────────
router.get('/abnormal/summary', limiterAbnormal, async (req, res, next) => {
  try {
    const lookbackDays = Math.max(parseInt(req.query.lookbackDays || '3', 10), 1);
    const offlineMin   = Math.max(parseInt(req.query.offlineMin   || '90', 10), 10);
    const noCache = String(req.query.nocache || '') === '1';
    const cacheKey = `abn-summary:${lookbackDays}:${offlineMin}`;

    if (!noCache) {
      const c = getCache(cacheKey);
      if (c) return res.json(c);
    }

    const withFaultSql = `
      ${latestStatusCteWithFault()}
      SELECT reason, COUNT(*)::int AS count FROM (
        SELECT
          CASE
            WHEN (fault_flags & 1) = 1 THEN 'FAULT_BIT'
            WHEN EXTRACT(EPOCH FROM (NOW() - last_time))/60.0 >= $2 THEN 'OFFLINE'
            WHEN "opMode" <> '0' THEN 'OPMODE_ABNORMAL'
            ELSE 'NORMAL'
          END AS reason
        FROM recent_latest
      ) x
      WHERE reason <> 'NORMAL'
      GROUP BY reason
      ORDER BY count DESC;
    `;

    const noFaultSql = `
      ${latestStatusCteNoFault()}
      SELECT reason, COUNT(*)::int AS count FROM (
        SELECT
          CASE
            WHEN EXTRACT(EPOCH FROM (NOW() - last_time))/60.0 >= $2 THEN 'OFFLINE'
            WHEN "opMode" <> '0' THEN 'OPMODE_ABNORMAL'
            ELSE 'NORMAL'
          END AS reason
        FROM recent_latest
      ) x
      WHERE reason <> 'NORMAL'
      GROUP BY reason
      ORDER BY count DESC;
    `;

    let rows;
    try {
      ({ rows } = await pool.query(withFaultSql, [lookbackDays, offlineMin]));
    } catch (e1) {
      ({ rows } = await pool.query(noFaultSql, [lookbackDays, offlineMin]));
    }

    const summary = {
      FAULT_BIT:        rows.find(r => r.reason === 'FAULT_BIT')?.count ?? 0,
      OFFLINE:          rows.find(r => r.reason === 'OFFLINE')?.count ?? 0,
      OPMODE_ABNORMAL:  rows.find(r => r.reason === 'OPMODE_ABNORMAL')?.count ?? 0,
    };

    const payload = { summary, lookbackDays, offlineMin, cached: true };
    setCache(cacheKey, payload);
    res.json(payload);
  } catch (e) {
    next(e);
  }
});

// ──────────────────────────────────────────────────────────────
// 4) 이상 발전소 지역별 요약 (PostgreSQL + MySQL JOIN)
//    - level=sido / sigungu / both 지원
//    - fault_flags 비트 1=FAULT, opMode!=0=ABNORMAL, 미보고시간>=offlineMin=OFFLINE
//    - address → parseKoreanAddress + normalizeSido 적용
// ──────────────────────────────────────────────────────────────
// ──────────────────────────────────────────────────────────────
// 4) 이상 발전소 지역별 요약 (PostgreSQL + MySQL JOIN)
//    - level=sido / sigungu / both 지원
//    - fault_flags 비트 1=FAULT, opMode!=0=ABNORMAL, 미보고시간>=offlineMin=OFFLINE
//    - address → parseKoreanAddress + normalizeSido 적용
// ──────────────────────────────────────────────────────────────
router.get('/abnormal/by-region', async (req, res) => {
  try {
    const lookbackDays = Math.max(parseInt(req.query.lookbackDays || '3', 10), 1);
    const offlineMin   = Math.max(parseInt(req.query.offlineMin   || '120', 10), 10);
    const level        = (req.query.level || 'sido').toLowerCase();
    const filterSido   = req.query.sido ? req.query.sido.trim() : null;

    // ⚠️ MySQL 연결이 선택사항인 환경 대비
    if (!mysqlPool) {
      return res.status(503).json({ ok: false, error: 'MySQL unavailable for region join' });
    }

    // 1) 최신 상태: fault 컬럼이 있으면 사용, 없으면 opMode-only 폴백
    const withFaultCte = `
      WITH recent_latest AS (
        SELECT DISTINCT ON ("rtuImei")
               "rtuImei" AS imei,
               "opMode"  AS op_mode,
               COALESCE("faultFlags", "fault_flag", "fault", 0) AS fault_flags,
               "time"    AS last_time
        FROM public."log_rtureceivelog"
        WHERE "time" >= NOW() - make_interval(days => $1::int)
        ORDER BY "rtuImei", "time" DESC
      )
      SELECT imei, op_mode, fault_flags,
             EXTRACT(EPOCH FROM (NOW() - last_time))/60.0 AS minutes_since
      FROM recent_latest
    `;

    const noFaultCte = `
      WITH recent_latest AS (
        SELECT DISTINCT ON ("rtuImei")
               "rtuImei" AS imei,
               "opMode"  AS op_mode,
               0::int    AS fault_flags,
               "time"    AS last_time
        FROM public."log_rtureceivelog"
        WHERE "time" >= NOW() - make_interval(days => $1::int)
        ORDER BY "rtuImei", "time" DESC
      )
      SELECT imei, op_mode, fault_flags,
             EXTRACT(EPOCH FROM (NOW() - last_time))/60.0 AS minutes_since
      FROM recent_latest
    `;

    let latestRows;
    try {
      const { rows } = await pool.query(withFaultCte, [lookbackDays]);
      latestRows = rows;
    } catch (_) {
      const { rows } = await pool.query(noFaultCte, [lookbackDays]);
      latestRows = rows;
    }
    if (!latestRows.length) return res.json({ ok: true, items: [], count: 0, level, filterSido, lookbackDays, offlineMin });

    // 2) 주소 메타 (MySQL)
    const imeis = latestRows.map(r => r.imei);
    const chunkSize = 1000;
    const addrMap = new Map();

    for (let i = 0; i < imeis.length; i += chunkSize) {
      const batch = imeis.slice(i, i + chunkSize);
      const sql = `
        SELECT rtu.rtuImei AS imei,
               rems.address AS address
          FROM rtu_rtu AS rtu
          LEFT JOIN rems_rems AS rems
                 ON rems.rtu_id = rtu.id
         WHERE rtu.rtuImei IN (${batch.map(() => '?').join(',')})
      `;
      const [metaRows] = await mysqlPool.query(sql, batch);
      for (const row of metaRows) {
        const { sido, sigungu } = parseKoreanAddress(row.address);
        addrMap.set(row.imei, { sido: normalizeSido(sido), sigungu });
      }
    }

    // 3) 집계
    const regionAgg = new Map();
    const norm = (s) => (s || '').replace(/\s+/g, '').replace(/도|시|군|구|특별자치시|광역시/g, '');

    for (const r of latestRows) {
      const meta = addrMap.get(r.imei);
      const sido = normalizeSido(meta?.sido || '미지정');
      const sigungu = meta?.sigungu || '';

      // 시/도 필터 (정규화 비교)
      if (filterSido && norm(sido) !== norm(normalizeSido(filterSido))) continue;

      let reason = 'NORMAL';
      if ((r.fault_flags & 1) === 1) reason = 'FAULT_BIT';
      else if (r.minutes_since >= offlineMin) reason = 'OFFLINE';
      else if (r.op_mode !== '0') reason = 'OPMODE_ABNORMAL';
      if (reason === 'NORMAL') continue;

      const key = level === 'sido' ? `${sido}|` : `${sido}|${sigungu}`;
      const cur = regionAgg.get(key) || { sido, sigungu, OFFLINE: 0, OPMODE_ABNORMAL: 0, FAULT_BIT: 0, total: 0 };
      cur[reason]++; cur.total++;
      regionAgg.set(key, cur);
    }

    const items = [...regionAgg.values()];
    res.json({ ok: true, level, filterSido, lookbackDays, offlineMin, count: items.length, items });
  } catch (e) {
    console.error('❌ /abnormal/by-region error:', e);
    res.status(500).json({ ok: false, error: e.message });
  }
});





// ──────────────────────────────────────────────────────────────
// 5) 전국 에너지 요약 (크론 캐시 기반) — 기존
// ──────────────────────────────────────────────────────────────
router.get('/energy', limiterEnergy, async (_req, res, next) => {
  try {
    const { getCache } = require('../jobs/energyRefresh');
    const { getNationwideEnergySummary } = require('../energy/summary');

    const c = getCache();
    if (c?.electric && c?.thermal) {
      return res.json({
        ok: true,
        data: { electric: c.electric, thermal: c.thermal },
        cached: true,
        updatedAt: c.updatedAt,
      });
    }

    const data = await getNationwideEnergySummary();
    res.json({ ok: true, data, cached: false });
  } catch (e) {
    next(e);
  }
});

// 6) 이상 발전소 포인트 (지도 표시용)
//   - 필터: reason(ALL|OFFLINE|OPMODE_ABNORMAL|FAULT_BIT), sido, sigungu, offlineMin
//   - 좌표는 우선 Postgres public.imei_meta(lat,lon) → 없으면 프론트에서 /rems/geocode로 보완
// 이상 포인트 (지도용)
// - reason: ALL|OFFLINE|OPMODE_ABNORMAL|FAULT_BIT
// - sido/sigungu 필터 지원
router.get('/abnormal/points', async (req, res, next) => {
  try {
    // 운영 초반엔 넉넉히 보는 게 안전
    const lookbackDays = Math.max(parseInt(req.query.lookbackDays || '30', 10), 1);
    const offlineMin   = Math.max(parseInt(req.query.offlineMin   || '90',  10), 10);
    const reasonFilter = String(req.query.reason || 'ALL').toUpperCase();
    const filterSido   = (req.query.sido    || '').trim();
    const filterSigungu= (req.query.sigungu || '').trim();

    // 1) 최신 상태 (fault 컬럼 유무에 따라 자동 폴백)
    const withFaultSql = `
      ${latestStatusCteWithFault()}
      SELECT
        r."rtuImei" AS imei,
        r."opMode"  AS op_mode,
        r.fault_flags,
        r.last_time,
        EXTRACT(EPOCH FROM (NOW() - r.last_time))/60.0 AS minutes_since,
        CASE
          WHEN (r.fault_flags & 1) = 1 THEN 'FAULT_BIT'
          WHEN EXTRACT(EPOCH FROM (NOW() - r.last_time))/60.0 >= $2 THEN 'OFFLINE'
          WHEN r."opMode" <> '0' THEN 'OPMODE_ABNORMAL'
          ELSE 'NORMAL'
        END AS reason
      FROM recent_latest r
    `;
    const noFaultSql = `
      ${latestStatusCteNoFault()}
      SELECT
        r."rtuImei" AS imei,
        r."opMode"  AS op_mode,
        r.fault_flags,
        r.last_time,
        EXTRACT(EPOCH FROM (NOW() - r.last_time))/60.0 AS minutes_since,
        CASE
          WHEN EXTRACT(EPOCH FROM (NOW() - r.last_time))/60.0 >= $2 THEN 'OFFLINE'
          WHEN r."opMode" <> '0' THEN 'OPMODE_ABNORMAL'
          ELSE 'NORMAL'
        END AS reason
      FROM recent_latest r
    `;

    let baseRows;
    try {
      ({ rows: baseRows } = await pool.query(withFaultSql, [lookbackDays, offlineMin]));
    } catch {
      ({ rows: baseRows } = await pool.query(noFaultSql,   [lookbackDays, offlineMin]));
    }

    // NORMAL 제외 + reason 필터
    let rows = baseRows.filter(r => r.reason !== 'NORMAL');
    if (reasonFilter !== 'ALL') rows = rows.filter(r => r.reason === reasonFilter);

    if (!rows.length) return res.json({ ok: true, items: [] });

    // 2) 주소/좌표 매핑 (Postgres imei_meta 우선) — 배열 바인딩으로 안전하게
    const imeis = rows.map(r => r.imei);
    let metaMap = new Map();
    try {
      const { rows: metas } = await pool.query(
        `SELECT imei, address, sido, sigungu, lat, lon
           FROM public.imei_meta
          WHERE imei = ANY($1::text[])`,
        [imeis]
      );
      metaMap = new Map(metas.map(m => [m.imei, m]));
    } catch {
      // imei_meta 테이블이 없을 수도 있음 → 조용히 패스
    }

    // 3) MySQL 보강 (주소 누락분만 조회)
    if (mysqlPool) {
      const lacks = rows.filter(r => {
        const meta = metaMap.get(r.imei);
        return !meta || !meta.address;
      });
      // 배치 처리
      const CHUNK = 500;
      for (let i = 0; i < lacks.length; i += CHUNK) {
        const batchImeis = lacks.slice(i, i + CHUNK).map(r => r.imei);
        if (!batchImeis.length) break;

        const placeholders = batchImeis.map(() => '?').join(',');
        const sql = `
          SELECT
            COALESCE(rtu.rtuImei, rems.rtu_id) AS imei,
            COALESCE(rems.address, '')         AS address
          FROM rems_rems AS rems
          LEFT JOIN rtu_rtu AS rtu
            ON rtu.id = rems.rtu_id
          WHERE rtu.rtuImei IN (${placeholders})
             OR rems.rtu_id  IN (${placeholders})
        `;
        const [mrows] = await mysqlPool.query(sql, [...batchImeis, ...batchImeis]);

        for (const m of mrows) {
          const { sido, sigungu } = parseKoreanAddress(m.address || '');
          metaMap.set(m.imei, {
            imei: m.imei,
            address: m.address || '',
            sido,
            sigungu,
            lat: null,
            lon: null,
          });
        }
      }
    }

    // 4) 지역 필터(sido/sigungu) 적용 + 결과 구성
    const norm = s => (s || '').replace(/\s+/g, '').replace(/도|시|군|구|특별자치시|광역시/g, '');
    const wantSido = filterSido ? norm(normalizeSido(filterSido)) : null;
    const wantSigun = filterSigungu ? norm(filterSigungu) : null;

    const items = [];
    for (const r of rows) {
      const meta = metaMap.get(r.imei) || {};
      const sido = normalizeSido(meta.sido || '');
      const sigungu = meta.sigungu || '';

      if (wantSido && norm(sido) !== wantSido) continue;
      if (wantSigun && norm(sigungu) !== wantSigun) continue;

      items.push({
        imei: r.imei,
        reason: r.reason,
        op_mode: r.op_mode,
        last_time: r.last_time,
        minutes_since: Number(r.minutes_since?.toFixed?.(1) ?? r.minutes_since),
        sido,
        sigungu,
        address: meta.address || '',
        lat: meta.lat ?? null,
        lon: meta.lon ?? null, // 좌표 없으면 프론트에서 /rems/geocode로 보완
      });
    }

    res.json({ ok: true, items });
  } catch (e) {
    next(e);
  }
});




router.get('/normal/points', async (req, res) => {
  try {
    const lookbackDays = Number(req.query.lookbackDays || 3);

    // ───────────────────────────────────────────────
    // 1️⃣ 최신 정상 발전소만 조회 (최근 N일 이내)
    // ───────────────────────────────────────────────
    const sql = `
      WITH latest AS (
        SELECT DISTINCT ON (r."rtuImei")
          r."rtuImei" AS imei,
          r."opMode",
          r."time" AS last_time
        FROM public."log_rtureceivelog" r
        WHERE r."time" >= NOW() - make_interval(days => $1)
        ORDER BY r."rtuImei", r."time" DESC
      )
      SELECT l.imei, l."opMode" AS op_mode, l.last_time,
             m.sido, m.sigungu, m.address, m.lat, m.lon
      FROM latest l
      LEFT JOIN public.imei_meta m ON m.imei = l.imei
      WHERE l."opMode" = '0'
      ORDER BY l.last_time DESC;
    `;
    const { rows } = await pool.query(sql, [lookbackDays]);

    // ───────────────────────────────────────────────
    // 2️⃣ 좌표(lat/lon)가 있는 항목만 즉시 응답
    // ───────────────────────────────────────────────
    const items = rows.filter(r => r.lat && r.lon);
    const pending = rows.length - items.length;

    // ✅ 즉시 응답 (프론트는 이걸 바로 받아서 표시함)
    res.json({ ok: true, items, pending });

    // ───────────────────────────────────────────────
    // 3️⃣ 좌표 없는 IMEI → 백그라운드 비동기 갱신
    // ───────────────────────────────────────────────
    const noCoords = rows.filter(r => !r.lat || !r.lon);
    if (noCoords.length > 0) {
      console.log(`🛰️ Found ${noCoords.length} normal points without coords — background sync start...`);

      // ⚡ 프론트 응답 끝난 뒤 2초 후 백그라운드 작업 시작
      setTimeout(async () => {
try {
  if (typeof syncLatLon === 'function') {
    await syncLatLon();         // 원래 의도대로 함수일 때만 수행
  } else {
    console.warn('syncLatLon not available; skip background sync');
  }
} catch (e) {
  console.error('❌ Background syncLatLon() error:', e);
}
      }, 2000);
    }
  } catch (err) {
    console.error('normal/points error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});




module.exports = router;
