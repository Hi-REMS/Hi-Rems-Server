const express = require('express');
const axios = require('axios');
const router = express.Router();
const { pool } = require('../db/db.pg');
const { mysqlPool } = require('../db/db.mysql');
const { nearestAsos } = require('../utils/nearestAsos');
const KAKAO_REST_KEY = process.env.KAKAO_REST_KEY;

async function resolveImeiMeta(imei) {
  const { rows } = await pool.query(
    `SELECT "cid" FROM public.log_remssendlog
     WHERE "rtuImei" = $1
     ORDER BY "time" DESC LIMIT 1`,
    [imei]
  );
  const cid = rows?.[0]?.cid || null;
  if (!cid) return { found: false, reason: 'NO_CID' };

  const [rows2] = await mysqlPool.query(
    `SELECT address FROM alliothub.rems_rems
     WHERE cid = ?
     ORDER BY createdDate DESC LIMIT 1`,
    [cid]
  );
  const row = rows2?.[0] || {};
  return { found: true, cid, address: row.address || null };
}

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

function wcToSkyPty(wc) {
  if (wc === 0) return { sky: '맑음', pty: '없음' };
  if ([1, 2, 3].includes(wc)) return { sky: '구름많음', pty: '없음' };
  if ([51, 53, 55, 61, 63, 65, 80, 81, 82].includes(wc)) return { sky: '흐림', pty: '비' };
  if ([71, 73, 75, 77, 85, 86].includes(wc)) return { sky: '흐림', pty: '눈' };
  return { sky: '흐림', pty: '없음' };
}

router.get('/by-imei/daily', async (req, res) => {
  try {
    const imei = String(req.query.imei || '').trim();
    if (!imei) {
      return res.status(400).json({ ok: false, error: 'IMEI_REQUIRED', got: req.query });
    }

    const meta = await resolveImeiMeta(imei);
    if (!meta.found) {
      return res.status(200).json({ ok: false, imei, reason: meta.reason });
    }

    let lat = parseFloat(req.query.lat);
    let lon = parseFloat(req.query.lon);

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      const geo = await geocodeAddress(meta.address);
      if (geo) { lat = geo.lat; lon = geo.lon; }
    }

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return res.status(200).json({
        ok: false, imei, cid: meta.cid, address: meta.address, reason: 'NO_GEO',
        note: KAKAO_REST_KEY ? '지오코딩 실패' : 'KAKAO_REST_KEY 미설정'
      });
    }

    const asos = nearestAsos?.({ lat, lon }) || null;

    const daysReq = Math.max(1, Math.min(7, parseInt(req.query.days ?? '2', 10) || 2));
    const params = {
      latitude: lat,
      longitude: lon,
      timezone: 'Asia/Seoul',
      forecast_days: daysReq,
      daily: [
        'temperature_2m_max',
        'temperature_2m_min',
        'precipitation_sum',
        'precipitation_probability_max',
        'windspeed_10m_max',
        'weathercode',
        'sunrise',
        'sunset'
      ].join(',')
    };

    const om = await axios.get('https://api.open-meteo.com/v1/forecast', {
      params,
      timeout: 10000,
      validateStatus: () => true
    });

    if (om.status !== 200 || !om.data?.daily) {
      return res.status(502).json({
        ok: false, error: 'OPEN_METEO_BAD_STATUS', http: om.status,
        imei, cid: meta.cid, address: meta.address, lat, lon
      });
    }

    const d = om.data.daily;
    const todayYmd = new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Seoul' }).replace(/-/g, '');

    const daily = (d.time || []).map((iso, i) => {
      const date = iso.replace(/-/g, '');
      const wc = d.weathercode?.[i];
      const { sky, pty } = wcToSkyPty(wc);
      const tmin = d.temperature_2m_min?.[i] ?? null;
      const tmax = d.temperature_2m_max?.[i] ?? null;
      const pop = d.precipitation_probability_max?.[i] ?? null;
      const prcp = d.precipitation_sum?.[i] ?? null;
      const wind = d.windspeed_10m_max?.[i] ?? null;
      const sunrise = (d.sunrise?.[i] || '').slice(11, 16);
      const sunset = (d.sunset?.[i] || '').slice(11, 16);

      const parts = [
        sky,
        (pty && pty !== '없음') ? `${pty}${pop != null ? ` ${pop}%` : ''}` : (pop != null ? `강수확률 ${pop}%` : null),
        (tmin != null && tmax != null) ? `${tmin}~${tmax}℃` : null,
        (wind != null) ? `바람 최대 ${wind}m/s` : null,
        (prcp != null && prcp > 0) ? `${prcp}mm` : null
      ].filter(Boolean);

      return {
        date,
        is_today: date === todayYmd,
        sky, pty,
        tmin: tmin == null ? null : Number(tmin),
        tmax: tmax == null ? null : Number(tmax),
        precip_mm: prcp == null ? null : Number(prcp),
        pop_max: pop == null ? null : Number(pop),
        wind_max: wind == null ? null : Number(wind),
        sunrise, sunset,
        summary: parts.join(' · ')
      };
    });

    return res.json({
      ok: true,
      imei,
      cid: meta.cid,
      address: meta.address,
      base_date: todayYmd,
      days: daily.length,
      lat, lon,
      daily,
      asos
    });
  } catch (e) {
    return res.status(500).json({
      ok: false,
      error: String(e?.message || e),
      stack: (e && e.stack) ? String(e.stack).split('\n').slice(0, 3) : undefined
    });
  }
});

module.exports = router;
