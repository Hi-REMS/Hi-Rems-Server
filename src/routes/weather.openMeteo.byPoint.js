// src/routes/weather.openMeteo.byPoint.js
const express = require('express');
const axios = require('axios');
const router = express.Router();

router.get('/by-point', async (req, res) => {
  try {
    const lat = parseFloat(req.query.lat);
    const lon = parseFloat(req.query.lon);
    const fetch = String(req.query.fetch || '') === '1';
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return res.status(400).json({ ok: false, error: 'INVALID_PARAMS', hint: 'lat, lon 필요' });
    }

    if (!fetch) {
      return res.json({
        ok: true,
        method: 'by-point',
        query: { lat, lon },
        hourly: [],
        note: 'GRID ONLY (Open-Meteo는 grid 불필요)',
      });
    }

    const url = 'https://api.open-meteo.com/v1/forecast';
    const params = {
      latitude: lat,
      longitude: lon,
      // ✅ 다양한 데이터 추가 (풍속, 습도, 체감온도, 기압, 운량, 강수확률)
      hourly: [
        'temperature_2m',              // 기온
        'apparent_temperature',        // 체감온도
        'relative_humidity_2m',        // 습도(%)
        'pressure_msl',                // 해면기압(hPa)
        'cloud_cover',                 // 운량(%)
        'windspeed_10m',               // 풍속(m/s)
        'precipitation',               // 강수량(mm)
        'precipitation_probability',   // 강수확률(%)
        'weathercode'                  // 날씨 코드
      ].join(','),
      timezone: 'Asia/Seoul',
      forecast_days: 1,
    };

    const r = await axios.get(url, { params, timeout: 12000, validateStatus: () => true });
    if (r.status !== 200 || !r.data?.hourly) {
      return res.status(502).json({ ok: false, error: 'OPEN_METEO_BAD_STATUS', http: r.status });
    }

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
    const nowHour = new Date().getHours();

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
          TA: Number(temperature_2m?.[i] ?? null),                 // 기온
          TAF: Number(apparent_temperature?.[i] ?? null),          // 체감온도
          RH: Number(relative_humidity_2m?.[i] ?? null),           // 습도
          PRESS: Number(pressure_msl?.[i] ?? null),                // 기압
          CLOUD: Number(cloud_cover?.[i] ?? null),                 // 운량
          WSPD: Number(windspeed_10m?.[i] ?? null),                // 풍속
          PRECIP: Number(precipitation?.[i] ?? 0),                 // 강수량
          POP: Number(precipitation_probability?.[i] ?? 0),        // 강수확률
          SKY,
          PTY,
          hourNum: hour
        };
      })
      .filter(r => r.date === today && r.hourNum <= nowHour)
      .sort((a, b) => a.hourNum - b.hourNum);

    return res.json({
      ok: true,
      method: 'by-point',
      query: { lat, lon },
      base_date: today,
      base_time: `${String(nowHour).padStart(2, '0')}:00`,
      hourly
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

module.exports = router;
