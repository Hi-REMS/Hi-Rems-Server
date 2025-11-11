// src/routes/export.monthCsv.js
// 월별 CSV 익스포트 (Open-Meteo 기반 일별 날씨 + 일별 발전량)
// GET /api/export/monthCsv?imei=...&year=YYYY&month=MM
//
// CSV 컬럼: date(YYYYMMDD), energy_kwh, t_min, t_max, t_mean, precip_mm
//
// ⚠️ 에너지 일합계는 아래 fetchDailyEnergyKwh()가
//    1) energy_daily → 2) energy_hourly SUM → 3) /api/energy/series 폴백(자동 에너지 추정/프로빙)
//    순으로 채웁니다.

const express = require('express');
const axios = require('axios');
const { LRUCache } = require('lru-cache');
const { pool } = require('../db/db.pg');
const { mysqlPool } = require('../db/db.mysql');

const router = express.Router();

/* ──────────────────────────────────────────────────────────────
 * 작은 유틸
 * ────────────────────────────────────────────────────────────── */
const pad2 = (n) => String(n).padStart(2, '0');
const isFiniteNum = (v) => Number.isFinite(Number(v));
const toNumOrNull = (v) => (isFiniteNum(v) ? Number(v) : null);

function monthStartEnd(year, month) {
  const y = Number(year);
  const m = Number(month);
  const start = new Date(Date.UTC(y, m - 1, 1, 0, 0, 0));
  const end = new Date(Date.UTC(y, m, 0, 23, 59, 59)); // day 0 => previous month last day
  const startStr = `${y}-${pad2(m)}-01`;
  const endStr = `${y}-${pad2(m)}-${pad2(end.getUTCDate())}`;
  return { start, end, startStr, endStr };
}

/* ──────────────────────────────────────────────────────────────
 * 캐시 (IMEI→CID, CID→주소/좌표, 지오코딩)
 * ────────────────────────────────────────────────────────────── */
const imeiCidCache = new LRUCache({ max: 2000, ttl: 5 * 60 * 1000 }); // 5m
const cidAddrCache = new LRUCache({ max: 2000, ttl: 10 * 60 * 1000 }); // 10m
const geocache     = new LRUCache({ max: 1000, ttl: 60 * 60 * 1000 }); // 1h

async function getLatestCidByImei(imei) {
  const ck = `imei:${imei}`;
  const c = imeiCidCache.get(ck);
  if (c) return c;

  const { rows } = await pool.query(
    `SELECT "cid"
       FROM public.log_remssendlog
      WHERE "rtuImei" = $1
      ORDER BY "time" DESC
      LIMIT 1`,
    [imei]
  );
  const cid = rows?.[0]?.cid || null;
  if (cid) imeiCidCache.set(ck, cid);
  return cid;
}

async function getLatestAddressLatLonByCid(cid) {
  const ck = `cid:${cid}`;
  const cached = cidAddrCache.get(ck);
  if (cached) return cached;

  // 현재 스키마: address만 확실 — 좌표는 Kakao로 보강
  const [rows] = await mysqlPool.query(
    `SELECT address
       FROM alliothub.rems_rems
      WHERE cid = ?
      ORDER BY createdDate DESC
      LIMIT 1`,
    [cid]
  );
  const r = rows?.[0] || {};
  const out = {
    address: r.address || null,
    lat: null,
    lon: null,
  };
  cidAddrCache.set(ck, out);
  return out;
}

async function geocodeByKakao(address) {
  if (!address) return null;
  const ck = `geo:${address}`;
  const cached = geocache.get(ck);
  if (cached) return cached;

  const key = process.env.KAKAO_REST_KEY;
  if (!key) return null;
  try {
    const resp = await axios.get('https://dapi.kakao.com/v2/local/search/address.json', {
      params: { query: address },
      headers: { Authorization: `KakaoAK ${key}` },
      timeout: 8000,
      validateStatus: () => true,
    });
    const doc = resp.data?.documents?.[0];
    const lat = toNumOrNull(doc?.y);
    const lon = toNumOrNull(doc?.x);
    if (lat !== null && lon !== null) {
      const out = { lat, lon, source: 'kakao:address' };
      geocache.set(ck, out);
      return out;
    }
  } catch (_) {}
  return null;
}

async function fetchOpenMeteoMonthly(lat, lon, year, month) {
  const { startStr, endStr } = monthStartEnd(year, month);
  const urlArchive = 'https://archive-api.open-meteo.com/v1/era5';
  const urlForecast = 'https://api.open-meteo.com/v1/forecast';
  const paramsBase = {
    latitude: lat,
    longitude: lon,
    daily: 'temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum',
    timezone: 'Asia/Seoul',
  };

  const now = new Date();
  const reqMonth = new Date(`${year}-${String(month).padStart(2, '0')}-01`);
  const isThisMonth =
    reqMonth.getFullYear() === now.getFullYear() &&
    reqMonth.getMonth() === now.getMonth();

  // ✅ 완전히 지난달이면 ERA5만 사용
  if (!isThisMonth) {
    const r = await axios.get(urlArchive, {
      params: { ...paramsBase, start_date: startStr, end_date: endStr },
      timeout: 20000,
      validateStatus: () => true,
    });
    if (r.status === 200 && r.data?.daily) return { ok: true, data: r.data, mode: 'archive' };
    return { ok: false, http: r.status };
  }

  // ✅ 이번달이면 ERA5 + forecast 병합
  const todayISO = now.toISOString().slice(0, 10); // YYYY-MM-DD
  const endMonthISO = `${year}-${String(month).padStart(2, '0')}-${String(
    new Date(year, month, 0).getDate()
  ).padStart(2, '0')}`;

  const results = { time: [], temperature_2m_max: [], temperature_2m_min: [], temperature_2m_mean: [], precipitation_sum: [] };

  // 1️⃣ 과거 부분 (이번달 1일 ~ 어제까지)
  try {
    const r1 = await axios.get(urlArchive, {
      params: { ...paramsBase, start_date: startStr, end_date: todayISO },
      timeout: 20000,
      validateStatus: () => true,
    });
    if (r1.status === 200 && r1.data?.daily) {
      const d = r1.data.daily;
      results.time.push(...d.time);
      results.temperature_2m_max.push(...d.temperature_2m_max);
      results.temperature_2m_min.push(...d.temperature_2m_min);
      results.temperature_2m_mean.push(...d.temperature_2m_mean);
      results.precipitation_sum.push(...d.precipitation_sum);
    }
  } catch (err) {
    console.warn('[OpenMeteo] ERA5 part fail', err.message);
  }

  // 2️⃣ 미래 부분 (오늘~말일까지)
  try {
    const r2 = await axios.get(urlForecast, {
      params: { ...paramsBase, start_date: todayISO, end_date: endMonthISO },
      timeout: 20000,
      validateStatus: () => true,
    });
    if (r2.status === 200 && r2.data?.daily) {
      const d = r2.data.daily;
      // 중복 제거 후 병합
      const seen = new Set(results.time);
      d.time.forEach((t, i) => {
        if (!seen.has(t)) {
          results.time.push(t);
          results.temperature_2m_max.push(d.temperature_2m_max[i]);
          results.temperature_2m_min.push(d.temperature_2m_min[i]);
          results.temperature_2m_mean.push(d.temperature_2m_mean[i]);
          results.precipitation_sum.push(d.precipitation_sum[i]);
        }
      });
    }
  } catch (err) {
    console.warn('[OpenMeteo] forecast part fail', err.message);
  }

  if (results.time.length) {
    return { ok: true, data: { daily: results }, mode: 'mixed' };
  } else {
    return { ok: false, http: 'empty' };
  }
}



/* ──────────────────────────────────────────────────────────────
 * 에너지 HEX 자동 추정 (최근 정상 프레임)
 * ────────────────────────────────────────────────────────────── */
async function guessEnergyHex(imei) {
  const sql = `
    SELECT split_part(body,' ',2) AS energy_hex
      FROM public.log_rtureceivelog
     WHERE "rtuImei" = $1
       AND left(body,2)='14'
       AND split_part(body,' ',5)='00'
       AND COALESCE("bodyLength",9999) >= 12
     ORDER BY "time" DESC
     LIMIT 1`;
  const { rows } = await pool.query(sql, [imei]);
  const hex = (rows?.[0]?.energy_hex || '').toLowerCase();
  if (['01','02','03','04','06','07'].includes(hex)) return hex;
  return null;
}

/* ──────────────────────────────────────────────────────────────
 * 에너지: 일별 합계 (DB → DB폴백 → 시리즈 폴백)
 * ──────────────────────────────────────────────────────────────
 * 반환: [{ date:'YYYYMMDD', energy_kwh:Number }, ...]
 */
async function fetchDailyEnergyKwh(imei, year, month) {
  const { start, end, startStr, endStr } = monthStartEnd(year, month);

  // 1) materialized 일집계 테이블
  try {
    const { rows } = await pool.query(
      `SELECT ymd AS date, kwh AS energy_kwh
         FROM public.energy_daily
        WHERE imei = $1
          AND ymd >= $2
          AND ymd <= $3
        ORDER BY ymd`,
      [
        `${imei}`,
        `${year}${pad2(month)}01`,
        `${year}${pad2(month)}31`,
      ]
    );
    if (rows?.length) return rows.map(r => ({ date: String(r.date), energy_kwh: Number(r.energy_kwh) || 0 }));
  } catch (_) {}

  // 2) 시간/분 테이블 SUM (KST 기준)
  try {
    const { rows } = await pool.query(
      `SELECT to_char((ts AT TIME ZONE 'Asia/Seoul')::date, 'YYYYMMDD') AS date,
              SUM(kwh)::float AS energy_kwh
         FROM public.energy_hourly
        WHERE imei = $1
          AND ts >= $2
          AND ts <  $3
        GROUP BY 1
        ORDER BY 1`,
      [imei, start.toISOString(), new Date(end.getTime() + 1000).toISOString()]
    );
    if (rows?.length) {
      return rows.map(r => ({ date: String(r.date), energy_kwh: Number(r.energy_kwh) || 0 }));
    }
  } catch (_) {}

  // 3) 시리즈 API 폴백 (장비가 누적Wh 없이 producedKwh만 주는 경우 등)
  const seriesBase = process.env.ENERGY_SERIES_URL || 'http://localhost:3000/api/energy/series';

  // 3-1) 에너지 HEX 자동 추정
  let energyHex = null;
  try { energyHex = await guessEnergyHex(imei); } catch (_) {}

  // 3-2) 호출 함수
  const callSeries = async (hex) => {
    const params = {
      imei,
      range: 'daily',
      start: startStr,
      end: endStr,
      energy: hex,
    };
    const r = await axios.get(seriesBase, { params, timeout: 12000, validateStatus: () => true });
    if (r.status !== 200 || !Array.isArray(r.data?.series)) return null;
    // series[].bucket: 'YYYY-MM-DD', kwh
    return r.data.series.map(s => ({
      date: String(s.bucket).replace(/-/g, ''), // YYYYMMDD
      energy_kwh: Number(s.kwh) || 0
    }));
  };

  // 3-3) 우선 자동 추정 → 빈값이면 프로빙(태양열/지열/태양광 순)
  const tryOrder = energyHex ? [energyHex, '02', '03', '01'] : ['02', '03', '01'];
  for (const hex of tryOrder) {
    try {
      const rows = await callSeries(hex);
      if (rows && rows.length) return rows;
    } catch (_) {}
  }

  return []; // 최후의 수단
}

/* ──────────────────────────────────────────────────────────────
 * 라우트: /api/export/monthCsv
 * ────────────────────────────────────────────────────────────── */
router.get('/monthCsv', async (req, res) => {
  try {
    const imei = String(req.query.imei || '').trim();
    const year = Number(req.query.year);
    const month = Number(req.query.month);

    if (!imei)   return res.status(400).json({ error: 'imei is required' });
    if (!year || !month || month < 1 || month > 12) {
      return res.status(400).json({ error: 'year/month are required (e.g. 2025 / 10)' });
    }

    // 1) IMEI → CID
    const cid = await getLatestCidByImei(imei);
    if (!cid) return res.status(404).json({ error: 'NO_CID_FOR_IMEI', imei });

    // 2) CID → 주소(+좌표)
    let { address, lat, lon } = await getLatestAddressLatLonByCid(cid);

    // 3) 좌표가 없으면 Kakao 지오코딩
    if ((lat === null || lon === null) && address) {
      const g = await geocodeByKakao(address);
      if (g) { lat = g.lat; lon = g.lon; }
    }
    if (lat === null || lon === null) {
      return res.status(502).json({ error: 'NO_GEO_FOR_FACILITY', imei, cid, address });
    }

    // 4) 날씨(Open-Meteo daily)
const om = await fetchOpenMeteoMonthly(lat, lon, year, month);
if (!om.ok) {
  console.error('[OPEN_METEO_FAIL]', { http: om.http, lat, lon, imei, cid });
  return res.status(502).json({
    error: 'OPEN_METEO_BAD_STATUS',
    http: om.http,
    meta: { lat, lon, imei, cid }
  });
}
    const daily = om.data.daily || {};
    const tArr  = daily.time || [];
    const tMax  = daily.temperature_2m_max || [];
    const tMin  = daily.temperature_2m_min || [];
    const tMean = daily.temperature_2m_mean || [];
    const prcp  = daily.precipitation_sum || [];

    // 5) 에너지(일별) — DB→DB폴백→시리즈 폴백
    const energyRows = await fetchDailyEnergyKwh(imei, year, month);
    const energyMap = new Map(energyRows.map(r => [String(r.date), Number(r.energy_kwh) || 0]));

    // 6) CSV 조립
    let csv = '날짜,발전량(kWh),최저기온(℃),최고기온(℃),평균기온(℃),강수량(mm)\n';
    for (let i = 0; i < tArr.length; i++) {
      const ymd = String(tArr[i]).replace(/-/g, ''); // YYYYMMDD
      const ekwh = (energyMap.has(ymd) ? energyMap.get(ymd) : '');
      const row = [
        ymd,
        ekwh,
        isFiniteNum(tMin[i])  ? tMin[i]  : '',
        isFiniteNum(tMax[i])  ? tMax[i]  : '',
        isFiniteNum(tMean[i]) ? tMean[i] : '',
        isFiniteNum(prcp[i])  ? prcp[i]  : '',
      ];
      csv += row.join(',') + '\n';
    }

    // 7) 전송
res.setHeader('Content-Type', 'text/csv; charset=utf-8');
res.setHeader(
  'Content-Disposition',
  `attachment; filename="month-${year}-${pad2(month)}-${imei}.csv"`
);
res.set('Cache-Control', 'no-store');

// ✅ 엑셀 한글 깨짐 방지: UTF-8 BOM 추가
const bom = '\uFEFF'; // ← 이 줄이 핵심
return res.send(bom + csv); // BOM을 csv 앞에 붙여서 전송
  } catch (e) {
    console.error('EXPORT CSV ERROR:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});

module.exports = router;
