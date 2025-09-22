// backend/src/routes/export.monthCsv.js
const express = require('express');
const router = express.Router();
const axios = require('axios');

// 내부 API 베이스 URL (환경변수로 분리: 기본 127.0.0.1:3000)
const API_BASE = process.env.INTERNAL_API_BASE || 'http://127.0.0.1:3000';

// ASOS 관측소 목록 (stnId, name, lat, lon)
const ASOS_STATIONS = require('../utils/asosStations');

/* ───────────── utils ───────────── */
function daysInMonth(year, month) { return new Date(year, month, 0).getDate(); } // month: 1..12
const pad2 = (n) => String(n).padStart(2, '0');
const toRad = (deg) => deg * Math.PI / 180;
const round2 = (v) => Math.round(Number(v || 0) * 100) / 100;
const toNumOr0 = (v) => (Number.isFinite(Number(v)) ? Number(v) : 0);
const noop = (_) => _; // 디버깅 훅

function haversineKm(a, b) {
  const R = 6371;
  const dLat = toRad(b.lat - a.lat);
  const dLon = toRad(b.lon - a.lon);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}

// 간단 라벨: 맑음/흐림/비/눈
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

// YYYYMMDD 문자열 비교용
function ymd(date) {
  const y = date.getFullYear();
  const m = pad2(date.getMonth() + 1);
  const d = pad2(date.getDate());
  return `${y}${m}${d}`;
}

/* ───────────── 내부 호출: ASOS ───────────── */
async function fetchAsosDaily({ stnId, y, m, endClampYmd = null }) {
  const start = `${y}${pad2(m)}01`;
  const end = endClampYmd || `${y}${pad2(m)}${pad2(daysInMonth(y, m))}`;
  const url = `${API_BASE}/api/weather/asos/daily?stnId=${encodeURIComponent(stnId)}&start=${start}&end=${end}`;
  try {
    const resp = await axios.get(url, { timeout: 15000 });
    const ok = resp.status === 200;
    const items = ok && resp.data && Array.isArray(resp.data.items) ? resp.data.items : [];
    return { ok, http: resp.status, items, raw: resp.data };
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
  const endStr = `${y}-${pad2(m)}-${pad2(daysInMonth(y, m))}`;
  const qs = new URLSearchParams({ imei, range: 'monthly', start: startStr, end: endStr });
  if (multiCode) qs.set('multi', multiCode);
  const url = `${API_BASE}/api/energy/electric/series?${qs.toString()}`;

  const days = daysInMonth(y, m);
  const map = new Map();
  for (let d = 1; d <= days; d++) map.set(d, 0);

  try {
    const resp = await axios.get(url, { timeout: 20000 });
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
        for (const r of data.detail_hourly.rows) sum += toNumOr0(r?.kwh);
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
function extractLatLonFromVilage(json) {
  if (!json || typeof json !== 'object') return null;
  const cands = [
    json.point, json.approx, json.geo,
    json.debug && json.debug.geo
  ].filter(Boolean);

  for (const o of cands) {
    const lat = Number(o.lat), lon = Number(o.lon);
    if (Number.isFinite(lat) && Number.isFinite(lon)) return { lat, lon };
  }
  return null;
}

function pickNearestAsos(lat, lon) {
  let best = null;
  for (const s of ASOS_STATIONS) {
    const d = haversineKm({ lat, lon }, { lat: s.lat, lon: s.lon });
    if (!best || d < best.distKm) {
      best = { stnId: s.stnId, name: s.name, distKm: Math.round(d * 10) / 10, stnLat: s.lat, stnLon: s.lon };
    }
  }
  return best;
}

/* ───────────── 라우트 ───────────── */
router.get('/month-csv', async (req, res) => {
  try {
    const imei = req.query.imei;
    const year = parseInt(req.query.year, 10);
    const month = parseInt(req.query.month, 10);
    const debug = String(req.query.debug || '');
    const multiParam = (req.query.multi || '').toString().toLowerCase(); // '' | '00'|'01'|'02'|'03'

    if (!imei || !year || !month) {
      return res.status(400).json({ error: 'MISSING_PARAMS', hint: 'imei, year, month 필요' });
    }

    // IMEI → 좌표
    const wxByImeiUrl = `${API_BASE}/api/weather/vilageFcst/by-imei?imei=${encodeURIComponent(imei)}`;
    let chosenStation = null;
    try {
      const wxByImeiResp = await axios.get(wxByImeiUrl, { timeout: 15000 });
      const wxByImei = (wxByImeiResp.status === 200) ? (wxByImeiResp.data || {}) : {};
      const pt = extractLatLonFromVilage(wxByImei);
      if (pt) chosenStation = pickNearestAsos(pt.lat, pt.lon);
    } catch (e) {
      noop(e); // 좌표 실패는 fallback 사용
    }

    const stationFallbackId = String(process.env.KMA_ASOS_FALLBACK_STNID || '108'); // 서울

    // ====== 발전량 (멀티 지원) ======
    const wantSpecificMulti = ['00', '01', '02', '03'].includes(multiParam);
    const multis = wantSpecificMulti ? [multiParam] : ['00', '01', '02', '03'];

    const genResults = await Promise.all(
      multis.map(m => fetchMonthGeneration({ imei, y: year, m: month, multiCode: m }))
    );

    const days = daysInMonth(year, month);

    // perMultiMaps[multi] = Map(day -> kWh)
    const perMultiMaps = {};
    multis.forEach((m, i) => { perMultiMaps[m] = genResults[i]?.map || new Map(); });

    // ====== ASOS 일자료 ======
    const today = new Date();
    const ymdYesterday = ymd(new Date(today.getFullYear(), today.getMonth(), today.getDate() - 1));
    const reqEnd = `${year}${pad2(month)}${pad2(days)}`;
    const endClamp = (reqEnd > ymdYesterday) ? ymdYesterday : reqEnd;

    const stnIdToUse = chosenStation ? chosenStation.stnId : stationFallbackId;
    const asosRes = await fetchAsosDaily({ stnId: stnIdToUse, y: year, m: month, endClampYmd: endClamp });

    const weatherByDay = new Map();
    if (asosRes?.ok && Array.isArray(asosRes.items)) {
      for (const it of asosRes.items) {
        const tm = (it?.tm ?? '') + ''; // YYYY-MM-DD
        const d = parseInt(tm.slice(-2), 10);
        if (Number.isInteger(d)) weatherByDay.set(d, it);
      }
    }

    // ====== debug 응답 (단일화) ======
    if (debug === '1' || debug === '2') {
      return res.json({
        ok: true,
        imei, year, month,
        usedMultis: multis,
        energy: genResults.map(g => ({ used: g?.used, ok: g?.ok, http: g?.http })),
        station: chosenStation || { stnId: stationFallbackId },
        weatherCount: weatherByDay.size,
        note: 'debug=1/2 → JSON 반환(동일동작)'
      });
    }

    // ====== CSV 생성 ======
    const headerCommon = wantSpecificMulti
      ? '일자,발전량[kWh],날씨,온도[°C]'
      : '일자,합계[kWh],M00[kWh],M01[kWh],M02[kWh],M03[kWh],날씨,온도[°C]';

    const rows = [headerCommon];

    for (let d = 1; d <= days; d++) {
      const wx = weatherByDay.get(d);
      const label = summarizeAsosSimple(wx);
      // 소수 1자리 정리 (숫자 아니면 공란)
      const temp = Number.isFinite(Number(wx?.avgTa)) ? (Math.round(Number(wx.avgTa) * 10) / 10) : '';

      if (wantSpecificMulti) {
        const v = perMultiMaps[multis[0]]?.get(d);
        rows.push(`${d}일,${v ?? ''},${label},${temp}`);
      } else {
        const v00 = perMultiMaps['00']?.get(d) ?? '';
        const v01 = perMultiMaps['01']?.get(d) ?? '';
        const v02 = perMultiMaps['02']?.get(d) ?? '';
        const v03 = perMultiMaps['03']?.get(d) ?? '';

        const sum = round2(toNumOr0(v00) + toNumOr0(v01) + toNumOr0(v02) + toNumOr0(v03));
        rows.push(`${d}일,${sum},${v00},${v01},${v02},${v03},${label},${temp}`);
      }
    }

    const csvBody = rows.join('\r\n');

    // BOM 붙여서 전송
    const fname = wantSpecificMulti
      ? `월별_${imei}_${year}-${pad2(month)}_multi-${multis[0]}.csv`
      : `월별_${imei}_${year}-${pad2(month)}.csv`;

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(fname)}`);

    const bom = Buffer.from([0xEF, 0xBB, 0xBF]);
    const buf = Buffer.concat([bom, Buffer.from(csvBody, 'utf8')]);
    return res.status(200).send(buf);
  } catch (e) {
    // 내부 에러 메시지는 로그로만 남기고, 응답은 일반화
    console.error('[export.monthCsv] error:', e);
    return res.status(500).json({ error: 'Internal error' });
  }
});

module.exports = router;
