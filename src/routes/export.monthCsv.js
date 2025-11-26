const express = require('express');
const axios = require('axios');
const { LRUCache } = require('lru-cache');
const { pool } = require('../db/db.pg');
const { mysqlPool } = require('../db/db.mysql');

const router = express.Router();
const pad2 = (n) => String(n).padStart(2, '0');
const isFiniteNum = (v) => Number.isFinite(Number(v));

function monthStartEnd(year, month) {
  const y = Number(year);
  const m = Number(month);
  const start = new Date(Date.UTC(y, m - 1, 1, 0, 0, 0));
  const end = new Date(Date.UTC(y, m, 0, 23, 59, 59));
  const startStr = `${y}-${pad2(m)}-01`;
  const endStr = `${y}-${pad2(m)}-${pad2(end.getUTCDate())}`;
  return { start, end, startStr, endStr };
}

function cloudStatus(v) {
  const n = Number(v);
  if (isNaN(n)) return '';
  if (n <= 20) return '맑음';
  if (n <= 40) return '약간 흐림';
  if (n <= 70) return '구름많음';
  if (n <= 90) return '흐림';
  return '매우 흐림';
}

const imeiCidCache = new LRUCache({ max: 2000, ttl: 5 * 60 * 1000 });
const cidAddrCache = new LRUCache({ max: 2000, ttl: 10 * 60 * 1000 });
const geocache = new LRUCache({ max: 1000, ttl: 60 * 60 * 1000 });

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
    const lat = Number(doc?.y);
    const lon = Number(doc?.x);

    if (!isNaN(lat) && !isNaN(lon)) {
      const out = { lat, lon, source: 'kakao:address' };
      geocache.set(ck, out);
      return out;
    }
  } catch {}

  return null;
}

async function fetchOpenMeteoSolarDaily(lat, lon, year, month) {
  const { startStr, endStr } = monthStartEnd(year, month);

  const urlArchive = 'https://archive-api.open-meteo.com/v1/era5';
  const urlForecast = 'https://api.open-meteo.com/v1/forecast';

  const now = new Date();
  const reqMonth = new Date(`${year}-${pad2(month)}-01`);
  const isThisMonth =
    now.getFullYear() === reqMonth.getFullYear() &&
    now.getMonth() === reqMonth.getMonth();

  const paramsBase = {
    latitude: lat,
    longitude: lon,
    timezone: 'Asia/Seoul',
    daily: [
      'cloudcover_mean',
      'sunshine_duration',
      'shortwave_radiation_sum'
    ].join(',')
  };

  if (!isThisMonth) {
    const r = await axios.get(urlArchive, {
      params: { ...paramsBase, start_date: startStr, end_date: endStr },
      timeout: 20000,
      validateStatus: () => true,
    });
    if (r.status === 200 && r.data?.daily) return { ok: true, daily: r.data.daily };
    return { ok: false, http: r.status };
  }

  const todayISO = now.toISOString().slice(0, 10);
  const endMonthISO = `${year}-${pad2(month)}-${pad2(new Date(year, month, 0).getDate())}`;

  const results = {
    time: [],
    cloudcover_mean: [],
    sunshine_duration: [],
    shortwave_radiation_sum: []
  };

  try {
    const r1 = await axios.get(urlArchive, {
      params: { ...paramsBase, start_date: startStr, end_date: todayISO },
      timeout: 20000,
      validateStatus: () => true,
    });

    if (r1.status === 200 && r1.data?.daily) {
      const d = r1.data.daily;
      results.time.push(...d.time);
      results.cloudcover_mean.push(...d.cloudcover_mean);
      results.sunshine_duration.push(...d.sunshine_duration);
      results.shortwave_radiation_sum.push(...d.shortwave_radiation_sum);
    }
  } catch {}

  try {
    const r2 = await axios.get(urlForecast, {
      params: { ...paramsBase, start_date: todayISO, end_date: endMonthISO },
      timeout: 20000,
      validateStatus: () => true,
    });

    if (r2.status === 200 && r2.data?.daily) {
      const d = r2.data.daily;
      const seen = new Set(results.time);

      d.time.forEach((t, i) => {
        if (!seen.has(t)) {
          results.time.push(t);
          results.cloudcover_mean.push(d.cloudcover_mean[i]);
          results.sunshine_duration.push(d.sunshine_duration[i]);
          results.shortwave_radiation_sum.push(d.shortwave_radiation_sum[i]);
        }
      });
    }
  } catch {}

  return { ok: true, daily: results };
}

async function fetchDailyEnergyKwh(imei, year, month, multiHex) {
  const { start, end, startStr, endStr } = monthStartEnd(year, month);

  try {
    const { rows } = await pool.query(
      `SELECT ymd AS date, kwh AS energy_kwh
         FROM public.energy_daily
        WHERE imei = $1
          AND ymd >= $2
          AND ymd <= $3
        ORDER BY ymd`,
      [`${imei}`, `${year}${pad2(month)}01`, `${year}${pad2(month)}31`]
    );
    if (rows?.length) {
      return rows.map(r => ({
        date: String(r.date),
        energy_kwh: Number(r.energy_kwh) || 0
      }));
    }
  } catch {}

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
      return rows.map(r => ({
        date: String(r.date),
        energy_kwh: Number(r.energy_kwh) || 0
      }));
    }
  } catch {}

  const seriesBase =
    process.env.ENERGY_SERIES_URL || 'http://localhost:3000/api/energy/series';

  let energyHex = null;
  try {
    energyHex = await guessEnergyHex(imei);
  } catch {}

const callSeries = async (hex) => {
  const params = {
    imei,
    range: 'daily',
    start: startStr,
    end: endStr,
    energy: hex,
  };

  if (multiHex) params.multi = multiHex;

  const r = await axios.get(seriesBase, {
    params,
    timeout: 12000,
    validateStatus: () => true,
  });

  if (r.status !== 200 || !Array.isArray(r.data?.series)) return null;

  return r.data.series.map((s) => ({
    date: String(s.bucket).replace(/-/g, ''),
    energy_kwh: Number(s.kwh) || 0,
  }));
};


  const tryOrder = energyHex ? [energyHex, '02', '03', '01'] : ['02', '03', '01'];

  for (const hex of tryOrder) {
    try {
      const rows = await callSeries(hex);
      if (rows && rows.length) return rows;
    } catch {}
  }

  return [];
}

router.get('/monthCsv', async (req, res) => {
 const multiHex = (req.query.multi || '').toLowerCase();
  try {
    const imei = String(req.query.imei || '').trim();
    const year = Number(req.query.year);
    const month = Number(req.query.month);

    if (!imei) return res.status(400).json({ error: 'imei is required' });
    if (!year || !month || month < 1 || month > 12) {
      return res.status(400).json({ error: 'year/month invalid' });
    }

    const cid = await getLatestCidByImei(imei);
    if (!cid) return res.status(404).json({ error: 'NO_CID_FOR_IMEI' });

    let { address, lat, lon } = await getLatestAddressLatLonByCid(cid);

    if ((lat === null || lon === null) && address) {
      const g = await geocodeByKakao(address);
      if (g) {
        lat = g.lat;
        lon = g.lon;
      }
    }

    if (lat === null || lon === null) {
      return res.status(502).json({ error: 'NO_GEO_FOR_FACILITY', imei });
    }

    const om = await fetchOpenMeteoSolarDaily(lat, lon, year, month);
    if (!om.ok) {
      return res.status(502).json({ error: 'OPEN_METEO_FAIL', http: om.http });
    }

    const daily = om.daily;
    const tArr = daily.time || [];
    const ccArr = daily.cloudcover_mean || [];
    const sunArr = daily.sunshine_duration || [];
    const radArr = daily.shortwave_radiation_sum || [];

    const energyRows = await fetchDailyEnergyKwh(imei, year, month, multiHex);
    const energyMap = new Map(energyRows.map(r => [String(r.date), r.energy_kwh]));

    let csv =
      '날짜,발전량(kWh),일사량(kWh/m²),일조시간(h),구름량(%),구름상태\n';

      const today = new Date();
      const todayYmd = Number(today.toISOString().slice(0, 10).replace(/-/g, ''));
      
      for (let i = 0; i < tArr.length; i++) {
        const ymdNum = Number(String(tArr[i]).replace(/-/g, ''));

        if (ymdNum > todayYmd) continue;

        const ymd = String(ymdNum);
        const kwh = energyMap.get(ymd) ?? '';
        const cloud = ccArr[i] ?? '';

        const row = [
          ymd,
          kwh,
          radArr[i] ?? '',
          sunArr[i] ? (sunArr[i] / 3600).toFixed(2) : '',
          cloud,
          cloudStatus(cloud)
        ];

        csv += row.join(',') + '\n';
      }

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="month-${year}-${pad2(month)}-${imei}.csv"`
    );
    const bom = '\uFEFF';
    res.send(bom + csv);

  } catch (e) {
    console.error('EXPORT CSV ERROR:', e);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
