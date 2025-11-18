// src/routes/weather.openMeteo.byImei.js
const express = require('express');
const axios = require('axios');
const router = express.Router();
const { pool } = require('../db/db.pg');
const { mysqlPool } = require('../db/db.mysql');
const KAKAO_REST_KEY = process.env.KAKAO_REST_KEY;

// IMEI → CID → 주소
async function resolveImeiMeta(imei) {
  const { rows } = await pool.query(
    `SELECT "cid"
       FROM public.log_remssendlog
      WHERE "rtuImei" = $1
      ORDER BY "time" DESC
      LIMIT 1`,
    [imei]
  );
  const cid = rows?.[0]?.cid || null;
  if (!cid) return { found: false, reason: 'NO_CID' };

  const [rows2] = await mysqlPool.query(
    `SELECT address, createdDate
       FROM alliothub.rems_rems
      WHERE cid = ?
      ORDER BY createdDate DESC
      LIMIT 1`,
    [cid]
  );
  const row = rows2?.[0] || {};
  return { found: true, cid, address: row.address || null };
}

// 주소 → 좌표 
async function geocodeAddress(addr) {
  if (!addr || !KAKAO_REST_KEY) return null;
  try {
    const r = await axios.get('https://dapi.kakao.com/v2/local/search/address.json', {
      params: { query: addr },
      headers: { Authorization: `KakaoAK ${KAKAO_REST_KEY}` },
      timeout: 8000,
      validateStatus: () => true
    });
    const doc = r.data?.documents?.[0];
    if (!doc) return null;
    const lat = Number(doc.y);
    const lon = Number(doc.x);
    return (Number.isFinite(lat) && Number.isFinite(lon)) ? { lat, lon } : null;
  } catch {
    return null;
  }
}

// GET /api/weather/openmeteo/by-imei
router.get('/by-imei', async (req, res) => {
  try {
    const imei = String(req.query.imei || '').trim();
    if (!imei) return res.status(400).json({ ok: false, error: 'IMEI_REQUIRED' });

    const meta = await resolveImeiMeta(imei);
    if (!meta.found) return res.json({ ok: false, imei, reason: meta.reason });

    let lat = parseFloat(req.query.lat);
    let lon = parseFloat(req.query.lon);

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      const geo = await geocodeAddress(meta.address);
      if (geo) { lat = geo.lat; lon = geo.lon; }
    }

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return res.json({
        ok: false,
        imei,
        cid: meta.cid,
        address: meta.address,
        reason: 'NO_GEO',
        note: KAKAO_REST_KEY ? '지오코딩 실패' : 'KAKAO_REST_KEY 미설정'
      });
    }

    const url = 'https://api.open-meteo.com/v1/forecast';
    const params = {
      latitude: lat,
      longitude: lon,
      hourly: [
        'temperature_2m',
        'apparent_temperature',
        'relative_humidity_2m',
        'pressure_msl',
        'cloud_cover',
        'windspeed_10m',
        'precipitation',
        'precipitation_probability',
        'weathercode'
      ].join(','),
      timezone: 'Asia/Seoul',
      forecast_days: 1,
    };

    const r = await axios.get(url, { params, timeout: 12000, validateStatus: () => true });
    if (r.status !== 200 || !r.data?.hourly)
      return res.status(502).json({ ok: false, error: 'OPEN_METEO_BAD_STATUS', http: r.status, lat, lon });

    const {
      time,
      temperature_2m,
      apparent_temperature,
      relative_humidity_2m,
      pressure_msl,
      cloud_cover,
      precipitation,
      precipitation_probability,
      windspeed_10m,
      weathercode
    } = r.data.hourly;

    const today = new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Seoul' }).replace(/-/g, '');
    const nowKst = new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' });
    const now = new Date(nowKst);
    const nowHour = now.getHours();
    const nowMinute = now.getMinutes();

    const hourly = time
      .map((t, i) => {
        const date = t.slice(0, 10).replace(/-/g, '');
        const hour = Number(t.slice(11, 13));
        const wc = weathercode?.[i];

        const SKY = wc === 0 ? '1' : [1, 2, 3].includes(wc) ? '3' : '4';
        const PTY =
          [51, 53, 55, 61, 63, 65, 80, 81, 82].includes(wc)
            ? '1'
            : [71, 73, 75, 77, 85, 86].includes(wc)
            ? '3'
            : '0';

        return {
          date,
          hour: `${String(hour).padStart(2, '0')}:00`,
          TA: Number(temperature_2m?.[i] ?? null),
          TAF: Number(apparent_temperature?.[i] ?? null),
          RH: Number(relative_humidity_2m?.[i] ?? null),
          PRESS: Number(pressure_msl?.[i] ?? null),
          CLOUD: Number(cloud_cover?.[i] ?? null),
          WSPD: Number(windspeed_10m?.[i] ?? null),
          PRECIP: Number(precipitation?.[i] ?? 0),
          POP: Number(precipitation_probability?.[i] ?? 0),
          SKY,
          PTY,
          hourNum: hour,
        };
      })
      .filter((r) => r.date === today && r.hourNum <= nowHour)
      .sort((a, b) => a.hourNum - b.hourNum);

    return res.json({
      ok: true,
      imei,
      cid: meta.cid,
      address: meta.address,
      base_date: today,
      base_time: `${String(nowHour).padStart(2, '0')}:${String(nowMinute).padStart(2, '0')}`,
      lat, lon,
      hourly,
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

module.exports = router;
