// backend/src/routes/export.monthCsv.js
const express = require('express');
const router = express.Router();
const axios = require('axios');

// ✅ ASOS 관측소 목록 (stnId, name, lat, lon)
const ASOS_STATIONS = require('../utils/asosStations');

/* ───────────── utils ───────────── */
function daysInMonth(year, month) { return new Date(year, month, 0).getDate(); } // month: 1..12
const pad2 = (n) => String(n).padStart(2, '0');
const toRad = (deg) => deg * Math.PI / 180;
function haversineKm(a, b){
  const R = 6371;
  const dLat = toRad(b.lat - a.lat);
  const dLon = toRad(b.lon - a.lon);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const h = Math.sin(dLat/2)**2 + Math.cos(lat1)*Math.cos(lat2)*Math.sin(dLon/2)**2;
  return 2 * R * Math.asin(Math.sqrt(h));
}
const round2 = (v) => Math.round(Number(v || 0) * 100) / 100;

// ✅ 간단 라벨: 맑음/흐림/비/눈
function summarizeAsosSimple(d) {
  if (!d) return '';
  const rn = Number(d.sumRn || 0);               // 일강수량(mm)
  const hasSnow = String(d.iscs || '').includes('눈');
  if (hasSnow) return '눈';
  if (rn > 0)   return '비';
  const cloud = Number(d.avgTca || 0);           // 평균운량(0~10)
  if (cloud >= 6) return '흐림';
  return '맑음';
}

/* ───────────── 내부 호출: ASOS ───────────── */
async function fetchAsosDaily({ stnId, y, m, endClampYmd = null }) {
  const start = `${y}${pad2(m)}01`;
  const end   = endClampYmd || `${y}${pad2(m)}${pad2(daysInMonth(y, m))}`;
  const url   = `http://127.0.0.1:3000/api/weather/asos/daily?stnId=${encodeURIComponent(stnId)}&start=${start}&end=${end}`;
  try {
    const resp = await axios.get(url, { timeout: 15000, validateStatus: () => true });
    if (resp.status !== 200) return { ok: false, http: resp.status, items: [], raw: resp.data };
    const items = (resp.data && resp.data.items && Array.isArray(resp.data.items)) ? resp.data.items : [];
    return { ok: true, http: 200, items, raw: resp.data };
  } catch (e) {
    return { ok: false, http: 0, items: [], error: String(e?.message || e) };
  }
}

/* ───────────── 내부 호출: 월별 발전량(멀티 지원) ─────────────
   - multiCode 가 주어지면 그 멀티만, 없으면 전체 합계를 series에서 받아도 되지만
     CSV 분할 표시를 위해 멀티별로 각각 호출해 맵을 만든다.
*/
async function fetchMonthGeneration({ imei, y, m, multiCode = null }) {
  const startStr = `${y}-${pad2(m)}-01`;
  const endStr   = `${y}-${pad2(m)}-${pad2(daysInMonth(y, m))}`;
  const qs = new URLSearchParams({
    imei,
    range: 'monthly',
    start: startStr,
    end: endStr,
  });
  if (multiCode) qs.set('multi', multiCode);

  const url = `http://127.0.0.1:3000/api/energy/electric/series?${qs.toString()}`;

  const days = daysInMonth(y, m);
  const map = new Map();
  for (let d = 1; d <= days; d++) map.set(d, 0);

  try {
    const resp = await axios.get(url, { timeout: 20000, validateStatus: () => true });
    if (resp.status !== 200) return { ok: false, http: resp.status, used: 'none', map, raw: resp.data };

    const data = resp.data || {};

    // 1) series → [{ bucket:'YYYY-MM-DD', kwh }]
    if (Array.isArray(data.series) && data.series.length > 0) {
      const wantPrefix = `${y}-${pad2(m)}-`;
      for (const row of data.series) {
        const bucket = (row?.bucket ?? '') + '';
        if (bucket.startsWith(wantPrefix)) {
          const day = parseInt(bucket.slice(-2), 10);
          if (Number.isInteger(day) && map.has(day)) {
            map.set(day, round2(row.kwh ?? 0));
          }
        }
      }
      return { ok: true, http: 200, used: 'series', map, raw: data };
    }

    // 2) detail_hourly → 하루 합산 (비상시)
    if (data.detail_hourly?.day && Array.isArray(data.detail_hourly.rows)) {
      const dayKey = String(data.detail_hourly.day); // YYYY-MM-DD
      if (dayKey.startsWith(`${y}-${pad2(m)}-`)) {
        const day = parseInt(dayKey.slice(-2), 10);
        let sum = 0;
        for (const r of data.detail_hourly.rows) {
          const v = Number(r?.kwh ?? 0);
          if (!Number.isNaN(v)) sum += v;
        }
        if (map.has(day)) map.set(day, round2(sum));
        return { ok: true, http: 200, used: 'detail_hourly_sum1day', map, raw: data };
      }
    }

    // 3) fallback
    return { ok: true, http: 200, used: 'fallback_zero', map, raw: data };
  } catch (e) {
    return { ok: false, http: 0, used: 'error', map, error: String(e?.message || e) };
  }
}

/* ───────── 좌표 추출 + 가까운 ASOS 선택 ───────── */
function extractLatLonFromVilage(json){
  if (!json || typeof json !== 'object') return null;
  if (json.point && Number.isFinite(json.point.lat) && Number.isFinite(json.point.lon)) {
    return { lat: Number(json.point.lat), lon: Number(json.point.lon), from: 'point' };
  }
  if (json.approx && Number.isFinite(json.approx.lat) && Number.isFinite(json.approx.lon)) {
    return { lat: Number(json.approx.lat), lon: Number(json.approx.lon), from: 'approx' };
  }
  if (json.geo && Number.isFinite(json.geo.lat) && Number.isFinite(json.geo.lon)) {
    return { lat: Number(json.geo.lat), lon: Number(json.geo.lon), from: 'geo' };
  }
  if (json.debug && json.debug.geo && Number.isFinite(json.debug.geo.lat) && Number.isFinite(json.debug.geo.lon)) {
    return { lat: Number(json.debug.geo.lat), lon: Number(json.debug.geo.lon), from: 'debug.geo' };
  }
  return null;
}

function pickNearestAsos(lat, lon){
  let best = null;
  for (const s of ASOS_STATIONS) {
    const d = haversineKm({lat,lon}, {lat: s.lat, lon: s.lon});
    if (!best || d < best.distKm) best = { stnId: s.stnId, name: s.name, distKm: Math.round(d*10)/10, stnLat:s.lat, stnLon:s.lon };
  }
  return best;
}

/* ───────────── 라우트 ───────────── */
router.get('/month-csv', async (req, res) => {
  try {
    const imei  = req.query.imei;
    const year  = parseInt(req.query.year, 10);
    const month = parseInt(req.query.month, 10);
    const debug = String(req.query.debug || '');
    const multiParam = (req.query.multi || '').toString().toLowerCase(); // '' | '00'|'01'|'02'|'03'

    if (!imei || !year || !month) {
      return res.status(400).json({ error: 'MISSING_PARAMS', hint: 'imei, year, month 필요' });
    }

    // IMEI → 좌표
    const wxByImeiUrl = `http://127.0.0.1:3000/api/weather/vilageFcst/by-imei?imei=${encodeURIComponent(imei)}`;
    const wxByImeiResp = await axios.get(wxByImeiUrl, { timeout: 15000, validateStatus: () => true });
    const wxByImei = (wxByImeiResp.status === 200) ? (wxByImeiResp.data || {}) : {};
    const pt = extractLatLonFromVilage(wxByImei);

    const stationFallbackId = String(process.env.KMA_ASOS_FALLBACK_STNID || '108'); // 서울
    let chosenStation = pt ? pickNearestAsos(pt.lat, pt.lon) : null;

    // ====== 발전량 (멀티 지원) ======
    const wantSpecificMulti = ['00','01','02','03'].includes(multiParam);
    const multis = wantSpecificMulti ? [multiParam] : ['00','01','02','03'];

    const genPromises = multis.map(m => fetchMonthGeneration({ imei, y: year, m: month, multiCode: m }));
    const genResults  = await Promise.all(genPromises);

    const days = daysInMonth(year, month);
    // perMultiMaps[multi] = Map(day -> kWh)
    const perMultiMaps = {};
    multis.forEach((m, i) => { perMultiMaps[m] = genResults[i]?.map || new Map(); });

    // ====== ASOS 일자료 ======
    const now = new Date();
    const ymdYesterdayDate = new Date(now.getFullYear(), now.getMonth(), now.getDate()-1);
    const ymdYesterday = `${ymdYesterdayDate.getFullYear()}${pad2(ymdYesterdayDate.getMonth()+1)}${pad2(ymdYesterdayDate.getDate())}`;
    const reqEnd   = `${year}${pad2(month)}${pad2(days)}`;
    const endClamp = (reqEnd > ymdYesterday) ? ymdYesterday : reqEnd;

    const stnIdToUse = chosenStation ? chosenStation.stnId : stationFallbackId;
    const asosRes = await fetchAsosDaily({ stnId: stnIdToUse, y: year, m: month, endClampYmd: endClamp });

    const weatherByDay = new Map();
    if (asosRes?.ok && Array.isArray(asosRes.items)) {
      for (const it of asosRes.items) {
        const tm = (it?.tm ?? '') + ''; // YYYY-MM-DD
        const d  = parseInt(tm.slice(-2), 10);
        if (Number.isInteger(d)) weatherByDay.set(d, it);
      }
    }

    // ====== CSV 생성 ======
    const headerCommon = wantSpecificMulti
      ? '일자,발전량[kWh],날씨,온도[°C]'
      : '일자,합계[kWh],M00[kWh],M01[kWh],M02[kWh],M03[kWh],날씨,온도[°C]';

    const rows = [headerCommon];

    for (let d = 1; d <= days; d++) {
      const wx  = weatherByDay.get(d);
      const label = summarizeAsosSimple(wx);
      const temp  = (wx?.avgTa ?? '') + '';

      if (wantSpecificMulti) {
        const v = perMultiMaps[multis[0]]?.get(d);
        rows.push(`${d}일,${v ?? ''},${label},${temp}`);
      } else {
        // 합계 = 멀티별 값의 합(빈 값은 0으로)
        const v00 = perMultiMaps['00']?.get(d) ?? '';
        const v01 = perMultiMaps['01']?.get(d) ?? '';
        const v02 = perMultiMaps['02']?.get(d) ?? '';
        const v03 = perMultiMaps['03']?.get(d) ?? '';

        const sum = round2(
          (Number.isFinite(+v00) ? +v00 : 0) +
          (Number.isFinite(+v01) ? +v01 : 0) +
          (Number.isFinite(+v02) ? +v02 : 0) +
          (Number.isFinite(+v03) ? +v03 : 0)
        );

        rows.push(`${d}일,${sum},${v00},${v01},${v02},${v03},${label},${temp}`);
      }
    }

    const csvBody = rows.join('\r\n');

    // debug 모드
    if (debug === '1' || debug === '2') {
      return res.json({
        ok: true, imei, year, month,
        usedMultis: multis,
        energy: genResults.map(g => ({ used: g?.used, ok: g?.ok })),
        station: chosenStation || { stnId: stationFallbackId },
        weatherCount: weatherByDay.size,
        note: 'debug=1/2 → JSON 반환'
      });
    }

    // ✅ BOM 붙여서 전송
    const fname =
      wantSpecificMulti
        ? `월별_${imei}_${year}-${pad2(month)}_multi-${multis[0]}.csv`
        : `월별_${imei}_${year}-${pad2(month)}.csv`;

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(fname)}`);

    const bom = Buffer.from([0xEF, 0xBB, 0xBF]);
    const buf = Buffer.concat([bom, Buffer.from(csvBody, 'utf8')]);
    return res.status(200).send(buf);
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
});

module.exports = router;
