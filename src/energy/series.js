// src/energy/series.js
// 에너지 시리즈 데이터 집계 (전기/열/풍력/연료전지/ESS 포함, 멀티 설비 대응: 태양광만 멀티)

const express = require('express');
const router = express.Router();
const rateLimit = require('express-rate-limit');
const { pool } = require('../db/db.pg');
const { parseFrame } = require('./parser');
const { TZ, getRangeUtc, bucketKeyKST, whDeltaToKwh } = require('./timeutil');
const { resolveOneImeiOrThrow } = require('./devices');

// CO₂ 계수
const ELECTRIC_CO2 = Number(process.env.ELECTRIC_CO2_PER_KWH || '0.4747');
const THERMAL_CO2  = Number(process.env.THERMAL_CO2_PER_KWH  || '0.198');
const TREE_KG      = 6.6;
const round2 = (v) => Math.round(v * 100) / 100;

// 누적Wh 포함된 프레임만
const MIN_BODYLEN_WITH_WH = 12;
const LEN_WITH_WH_COND = `COALESCE("bodyLength", 9999) >= ${MIN_BODYLEN_WITH_WH}`;

// 최근 N일만 스캔하도록 강제 (지열 03 → 최근 7일만)
const RECENT_WINDOW_BY_ENERGY = {
  '01': 30, // 태양광
  '02': 30, // 태양열
  '03': 7,  // 지열 (대용량 → 7일만 스캔)
  '04': 14, // 풍력
  '06': 14, // 연료전지
  '07': 14, // ESS
};

function okClause(req) {
  const ok = String(req.query.ok || '1').toLowerCase();
  return (ok === 'any' || ok === '0')
    ? ''
    : "AND split_part(body,' ',5)='00'";
}

const seriesLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 100,
  message: { error: 'Too many requests — try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});

// CO2 계수
const CO2_FOR = (energyHex) => {
  const e = (energyHex || '').toLowerCase();
  if (e === '02' || e === '03') return THERMAL_CO2;
  return ELECTRIC_CO2;
};

// 태양광만 멀티
const MULTI_SUPPORTED = (energyHex) => (energyHex || '').toLowerCase() === '01';

// HEX header 일부 파싱
function headerFromHex(hex) {
  const parts = (hex || '').trim().split(/\s+/);
  return {
    energyHex: (parts[1] || '').toLowerCase(),
    type: parts[2] ? parseInt(parts[2],16) : null,
    multi: parts[3] ?? null
  };
}

function kstDayKey(d) {
  const [y, m, day] = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Seoul',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(d).filter(p => p.type !== 'literal').map(p=>p.value);
  return `${y}-${m}-${day}`;
}

function kstHourKey(d) {
  const z = new Date(d.toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
  return `${z.getFullYear()}-${String(z.getMonth()+1).padStart(2,'0')}-${String(z.getDate()).padStart(2,'0')} ${String(z.getHours()).padStart(2,'0')}`;
}

function parseYmd(s) {
  if (!s) return null;
  let m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/) || s.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (!m) return null;
  return { y: +m[1], M: +m[2], d: +m[3] };
}

function kstStartUtc({ y, M, d }) {
  return new Date(Date.UTC(y, M - 1, d, -9));
}

function kstEndExclusiveUtc({ y, M, d }) {
  return new Date(Date.UTC(y, M - 1, d + 1, -9));
}

function buildTypeCondsForEnergy(energyHex, typeHex, params) {
  const e = (energyHex || '').toLowerCase();
  const t = (typeHex || '').toLowerCase();
  if (e === '04' && (!t || t === 'auto')) {
    return { sql: `split_part(body,' ',3) IN ('00','01')`, added:false };
  }
  if (t) {
    params.push(t);
    return { sql:`split_part(body,' ',3) = $${params.length}`, added:true };
  }
  return { sql:null, added:false };
}

// ─────────────────────────────
//        ENDPOINT
// ─────────────────────────────
router.get('/series', seriesLimiter, async (req, res, next) => {
  try {
    const q = req.query.rtuImei || req.query.imei || req.query.name || req.query.q;
    if (!q) return res.status(400).json({error:'imei 필요'});

    const imei = await resolveOneImeiOrThrow(q);

    const range = (req.query.range || 'weekly').toLowerCase();
    const energyHex = (req.query.energy || '01').toLowerCase();
    const typeHex   = (req.query.type || '').toLowerCase() || null;
    const wantHourly = String(req.query.detail || '').toLowerCase()==='hourly';
    const multiParam = (req.query.multi || '').toLowerCase();
    const wantMulti = ['00','01','02','03'].includes(multiParam) ? multiParam : null;

    // 기본 기간
    const startQ = parseYmd(req.query.start);
    const endQ = parseYmd(req.query.end);
    let startUtc, endUtc, bucket;

    if (startQ && endQ) {
      startUtc = kstStartUtc(startQ);
      endUtc   = kstEndExclusiveUtc(endQ);
      bucket   = 'day';
    } else {
      const r = getRangeUtc(range);
      startUtc = r.startUtc;
      endUtc = r.endUtc;
      bucket = r.bucket;
    }

    // 🔥 yearly → 최근 N일로 강제 축소
    if (range === 'yearly') {
      const recentDays = RECENT_WINDOW_BY_ENERGY[energyHex] || 30;
      const recentUtc = new Date(Date.now() - recentDays * 86400 * 1000);

      startUtc = recentUtc;
      bucket = 'day';
    }

    // WHERE 조건 구성
    const conds = [
      `"rtuImei" = $1`,
      `"time" >= $2`,
      `"time" < $3`,
      "left(body,2)='14'",
      LEN_WITH_WH_COND
    ];

    const okFilter = okClause(req);
    if (okFilter) conds.push(okFilter.replace(/^AND\s+/,''));

    const params = [imei, startUtc, endUtc];

    if (energyHex) conds.push(`body LIKE '14 ${energyHex} %'`);

    const tc = buildTypeCondsForEnergy(energyHex, typeHex, params);
    if (tc.sql) conds.push(tc.sql);

    if (wantMulti && MULTI_SUPPORTED(energyHex)) {
      conds.push(`body LIKE '14 ${energyHex} __ ${wantMulti} %'`);
    }

    const sql = `
      SELECT "time", "bodyLength", body
      FROM public.log_rtureceivelog
      WHERE ${conds.join(' AND ')}
      ORDER BY "time" ASC
    `;

    const { rows } = await pool.query(sql, params);

    // ------------------------
    // 누적Wh → kWh 일/월 집계
    // ------------------------
    const perKey = new Map();

    for (const r of rows) {
      const p = parseFrame(r.body);
      const head = headerFromHex(r.body);
      const wh = p?.metrics?.cumulativeWh ?? null;
      if (wh == null) continue;

      const m = MULTI_SUPPORTED(head.energyHex)
        ? (wantMulti || (head.multi || '00'))
        : '00';

      const bkey = bucketKeyKST(new Date(r.time), bucket);
      const key = `${bkey}|${m}`;

      const rec = perKey.get(key) || { firstWh:null, lastWh:null, firstTs:null, lastTs:null };
      if (rec.firstWh == null) {
        rec.firstWh = wh;
        rec.firstTs = r.time;
      }
      rec.lastWh = wh;
      rec.lastTs = r.time;
      perKey.set(key, rec);
    }

    const co2Factor = CO2_FOR(energyHex);
    const bucketAgg = new Map();

    for (const [key, rec] of perKey.entries()) {
      const [bkey] = key.split('|');
      const kwh = Math.max(0, whDeltaToKwh(rec.firstWh, rec.lastWh));

      const cur = bucketAgg.get(bkey) || { kwh:0, firstAt:rec.firstTs, lastAt:rec.lastTs };
      cur.kwh += kwh;
      cur.firstAt = cur.firstAt ?? rec.firstTs;
      cur.lastAt = rec.lastTs;
      bucketAgg.set(bkey, cur);
    }

    let series = [...bucketAgg.entries()].map(([bucket, agg]) => {
      const kwh = round2(agg.kwh);
      const co2_kg = round2(kwh * co2Factor);
      const trees = Math.round(co2_kg / TREE_KG);

      return { bucket, kwh, co2_kg, trees, firstAt: agg.firstAt, lastAt: agg.lastAt };
    });

    series.sort((a,b)=>a.bucket.localeCompare(b.bucket));

    // ------------------------
    // yearly → month 집계
    // ------------------------
    if (range === 'yearly') {
      const monthAgg = new Map();
      for (const row of series) {
        const mk = row.bucket.slice(0,7);
        const t = monthAgg.get(mk) || {
          bucket: mk, kwh:0, co2_kg:0, trees:0,
          firstAt: row.firstAt, lastAt: row.lastAt
        };
        t.kwh += row.kwh;
        t.co2_kg += row.co2_kg;
        t.trees += row.trees;
        if (row.firstAt < t.firstAt) t.firstAt = row.firstAt;
        if (row.lastAt > t.lastAt) t.lastAt = row.lastAt;
        monthAgg.set(mk, t);
      }
      series = [...monthAgg.values()].sort((a,b)=>a.bucket.localeCompare(b.bucket));
      bucket = 'month';
    }

    const total_kwh = round2(series.reduce((s,x)=>s + (x.kwh||0),0));
    const total_co2_kg = round2(total_kwh * co2Factor);
    const total_trees = Math.round(total_co2_kg / TREE_KG);

    // ------------------------
    // detail_hourly
    // ------------------------
    let detail_hourly = null;

    if (wantHourly && rows.length && range !== 'yearly') {
      const lastRowTime = rows[rows.length-1].time;
      const lastDay = kstDayKey(new Date(lastRowTime));
      const perHourMap = new Map();

      for (const r of rows) {
        const t = new Date(r.time);
        if (kstDayKey(t) !== lastDay) continue;

        const p = parseFrame(r.body);
        const head = headerFromHex(r.body);
        const wh = p?.metrics?.cumulativeWh ?? null;
        if (wh == null) continue;

        const m = MULTI_SUPPORTED(head.energyHex) ? (head.multi || '00') : '00';
        if (wantMulti && MULTI_SUPPORTED(head.energyHex) && m !== wantMulti) continue;

        const hh = kstHourKey(t).slice(11,13);
        const key = `${hh}|${m}`;
        const rec = perHourMap.get(key) || { firstWh:null, lastWh:null };
        if (rec.firstWh == null) rec.firstWh = wh;
        rec.lastWh = wh;
        perHourMap.set(key, rec);
      }

      const hourAgg = new Map();
      for (const [key, rec] of perHourMap.entries()) {
        const hh = key.split('|')[0];
        const kwh = Math.max(0, whDeltaToKwh(rec.firstWh, rec.lastWh));
        hourAgg.set(hh, (hourAgg.get(hh)||0) + kwh);
      }

      const rowsHourly = Array.from({length:24},(_,i)=>String(i).padStart(2,'0')).map(hh=>{
        const kwh = round2(hourAgg.get(hh)||0);
        return {
          hour: `${hh}:00`,
          kwh,
          eff_pct:null,
          weather:null,
          co2_kg: round2(kwh * co2Factor)
        };
      });

      detail_hourly = { day:lastDay, rows: rowsHourly };
    }

    // 응답
    res.json({
      deviceInfo:{rtuImei:imei, tz:TZ},
      params:{
        range,
        energy_hex: energyHex,
        type_hex: typeHex,
        multi: (wantMulti && MULTI_SUPPORTED(energyHex)) ? wantMulti : 'all',
        detail: wantHourly ? 'hourly' : undefined,
        ok: req.query.ok || '00'
      },
      bucket,
      range_utc:{start:startUtc, end:endUtc},
      series,
      detail_hourly,
      summary:{total_kwh, total_co2_kg, total_trees}
    });

  } catch (e) { next(e); }
});

module.exports = router;
