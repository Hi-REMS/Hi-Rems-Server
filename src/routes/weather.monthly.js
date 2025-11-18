// src/routes/weather.monthly.csvBased.js
// CSV 생성 로직을 기반으로 만든 월간 비교 JSON API (DB 필요 없음)

const express = require('express');
const axios = require('axios');
const { LRUCache } = require('lru-cache');
const { pool } = require('../db/db.pg');
const { mysqlPool } = require('../db/db.mysql');

const router = express.Router();

const pad2 = (n) => String(n).padStart(2, '0');

// CSV 코드와 동일
function monthStartEnd(year, month) {
  const y = Number(year);
  const m = Number(month);
  const start = new Date(Date.UTC(y, m - 1, 1, 0, 0, 0));
  const end = new Date(Date.UTC(y, m, 0, 23, 59, 59));
  const startStr = `${y}-${pad2(m)}-01`;
  const endStr = `${y}-${pad2(m)}-${pad2(end.getUTCDate())}`;
  return { start, end, startStr, endStr };
}

/* ─────────────────────────────
 * 구름량 / 날씨코드 매핑 (CSV 동일)
 * ───────────────────────────── */
function cloudStatus(v) {
  const n = Number(v);
  if (isNaN(n)) return '';
  if (n <= 20) return '맑음';
  if (n <= 40) return '약간 흐림';
  if (n <= 70) return '구름많음';
  if (n <= 90) return '흐림';
  return '매우 흐림';
}

function weatherStatus(code) {
  const c = Number(code);
  if (c === 0) return '맑음';
  if (c === 1) return '대체로 맑음';
  if (c === 2) return '부분적 흐림';
  if (c === 3) return '흐림';
  if ([45,48].includes(c)) return '안개';
  if ([51,53,55].includes(c)) return '이슬비';
  if ([61,63,65].includes(c)) return '비';
  if ([66,67].includes(c)) return '얼어붙는 비';
  if ([71,73,75,77].includes(c)) return '눈';
  if ([80,81,82].includes(c)) return '소나기';
  if (c === 95) return '뇌우';
  if ([96,99].includes(c)) return '우박·뇌우';
  return '';
}

/* ─────────────────────────────
 * CID / Address / LatLon 로직 (CSV 동일)
 * ───────────────────────────── */
const imeiCidCache = new LRUCache({ max: 2000, ttl: 5 * 60 * 1000 });
const cidAddrCache = new LRUCache({ max: 2000, ttl: 10 * 60 * 1000 });

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

  const [rows] = await mysqlPool.query(
    `SELECT address
       FROM alliothub.rems_rems
      WHERE cid = ?
      ORDER BY createdDate DESC
      LIMIT 1`,
    [cid]
  );

  const r = rows?.[0] || {};
  const out = { address: r.address || null, lat: null, lon: null };
  cidAddrCache.set(ck, out);
  return out;
}

/* ─────────────────────────────
 * 지오코딩 (CSV 동일)
 * ───────────────────────────── */
async function geocodeByKakao(address) {
  if (!address) return null;
  try {
    const resp = await axios.get('https://dapi.kakao.com/v2/local/search/address.json', {
      params: { query: address },
      headers: { Authorization: `KakaoAK ${process.env.KAKAO_REST_KEY}` },
      timeout: 8000
    });

    const doc = resp.data?.documents?.[0];
    if (!doc) return null;

    return { lat: Number(doc.y), lon: Number(doc.x) };
  } catch {
    return null;
  }
}

/* ─────────────────────────────
 * OpenMeteo (CSV 동일 복붙)
 * ───────────────────────────── */
async function fetchOpenMeteoSolarDaily(lat, lon, year, month) {
  const { startStr, endStr } = monthStartEnd(year, month);

  const urlArchive = 'https://archive-api.open-meteo.com/v1/era5';
  const urlForecast = 'https://api.open-meteo.com/v1/forecast';

  const now = new Date();
  const reqMonth = new Date(`${year}-${pad2(month)}-01`);
  const isThisMonth =
    now.getFullYear() === reqMonth.getFullYear() &&
    now.getMonth() + 1 === month;

  const paramsBase = {
    latitude: lat,
    longitude: lon,
    timezone: 'Asia/Seoul',
    daily: [
      'weathercode',
      'cloudcover_mean',
      'sunshine_duration',
      'shortwave_radiation_sum'
    ].join(',')
  };

  // 지난달 → ERA5 만
  if (!isThisMonth) {
    const r = await axios.get(urlArchive, {
      params: { ...paramsBase, start_date: startStr, end_date: endStr }
    });
    return r.data?.daily || null;
  }

  // 이번달 → ERA5 + forecast 혼합
  const today = now.toISOString().slice(0, 10);
  const endMonthISO = endStr;

  const result = {
    time: [],
    weathercode: [],
    cloudcover_mean: [],
    sunshine_duration: [],
    shortwave_radiation_sum: []
  };

  // 1) Archive: 1~오늘
  try {
    const r1 = await axios.get(urlArchive, {
      params: { ...paramsBase, start_date: startStr, end_date: today }
    });
    if (r1.data?.daily) {
      const d = r1.data.daily;
      result.time.push(...d.time);
      result.weathercode.push(...d.weathercode);
      result.cloudcover_mean.push(...d.cloudcover_mean);
      result.sunshine_duration.push(...d.sunshine_duration);
      result.shortwave_radiation_sum.push(...d.shortwave_radiation_sum);
    }
  } catch {}

  // 2) Forecast: 내일~말일
  try {
    const r2 = await axios.get(urlForecast, {
      params: { ...paramsBase, start_date: today, end_date: endMonthISO }
    });

    if (r2.data?.daily) {
      const d = r2.data.daily;
      const seen = new Set(result.time);

      d.time.forEach((t, i) => {
        if (!seen.has(t)) {
          result.time.push(t);
          result.weathercode.push(d.weathercode[i]);
          result.cloudcover_mean.push(d.cloudcover_mean[i]);
          result.sunshine_duration.push(d.sunshine_duration[i]);
          result.shortwave_radiation_sum.push(d.shortwave_radiation_sum[i]);
        }
      });
    }
  } catch {}

  return result;
}

/* ─────────────────────────────
 * 발전량 (CSV 동일: 에너지 시리즈 fallback 사용)
 * DB 필요 없음!
 * ───────────────────────────── */
async function fetchDailyEnergyKwh(imei, year, month) {
  const { startStr, endStr } = monthStartEnd(year, month);

  const seriesBase =
    process.env.ENERGY_SERIES_URL || 'http://localhost:3000/api/energy/series';

  // 태양광 hex 추정
  let hex = '01';
  try {
    const r = await axios.get(seriesBase, {
      params: { imei, range: 'daily', start: startStr, end: endStr, energy: '01' }
    });
    if (Array.isArray(r.data?.series) && r.data.series.length) hex = '01';
  } catch {}

  const r = await axios.get(seriesBase, {
    params: { imei, range: 'daily', start: startStr, end: endStr, energy: hex }
  });

  return (r.data?.series || []).map(s => ({
    date: s.bucket.replace(/-/g, ''),
    energy_kwh: Number(s.kwh) || 0
  }));
}

/* ─────────────────────────────
 * 최종 API
 * ───────────────────────────── */
router.get('/monthly.csvBased', async (req, res) => {
  try {
    const imei = String(req.query.imei || '').trim();
    const year = Number(req.query.year);
    const month = Number(req.query.month);

    if (!imei) return res.status(400).json({ error: 'imei required' });

    const cid = await getLatestCidByImei(imei);
    if (!cid) return res.status(404).json({ error: 'NO_CID_FOR_IMEI' });

    let { address, lat, lon } = await getLatestAddressLatLonByCid(cid);

    if ((!lat || !lon) && address) {
      const g = await geocodeByKakao(address);
      if (g) {
        lat = g.lat;
        lon = g.lon;
      }
    }

    if (!lat || !lon)
      return res.status(502).json({ error: 'NO_GEO' });

    // CSV 로직 그대로
    const meteo = await fetchOpenMeteoSolarDaily(lat, lon, year, month);
    const energy = await fetchDailyEnergyKwh(imei, year, month);
    const energyMap = new Map(energy.map(r => [r.date, r.energy_kwh]));

    const out = [];

    meteo.time.forEach((t, i) => {
      const ymd = t.replace(/-/g, '');

      out.push({
        date: ymd,
        energy_kwh: energyMap.get(ymd) ?? 0,
        solar: meteo.shortwave_radiation_sum[i] ?? null,
        sunshine: (meteo.sunshine_duration[i] / 3600).toFixed(2),
        cloud: meteo.cloudcover_mean[i],
        cloudStatus: cloudStatus(meteo.cloudcover_mean[i]),
        weathercode: meteo.weathercode[i],
        weatherStatus: weatherStatus(meteo.weathercode[i])
      });
    });

    res.json({
      ok: true,
      imei,
      year,
      month,
      daily: out
    });

  } catch (e) {
    console.error('[weather.monthly.csvBased ERROR]', e);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
