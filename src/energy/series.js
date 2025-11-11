// src/energy/series.js
// 에너지 시리즈 데이터 집계 (전기/열/풍력/연료전지/ESS 포함, 멀티 설비 대응: 태양광만 멀티)
// - log_rtureceivelog에서 Hex 프레임 조회
// - (bucket, multi)별 first/last 누적Wh 차분 → kWh
// - ?multi=00|01|02|03 지정 시 해당 설비만, 미지정/['', 'all']이면 전체 합계
// - range=weekly/monthly/yearly + detail=hourly 지원(24시간 skeleton 포함)
// - CO₂ 계수는 energy에 따라 자동 적용
//   · 전기 : 태양광(0x01), 풍력(0x04), 연료전지(0x06), ESS(0x07) → 0.4747 kg/kWh
//   · 열   : 태양열(0x02), 지열(0x03) → 0.198 kg/kWh

const express = require('express');
const router = express.Router();
const rateLimit = require('express-rate-limit');
const { pool } = require('../db/db.pg');
const { parseFrame } = require('./parser');
const { TZ, getRangeUtc, bucketKeyKST, whDeltaToKwh } = require('./timeutil');
const { resolveOneImeiOrThrow } = require('./devices');

// ─────────────────────────────────────────────
// 상수
// ─────────────────────────────────────────────
const ELECTRIC_CO2 = Number(process.env.ELECTRIC_CO2_PER_KWH || '0.4747'); // kg/kWh
const THERMAL_CO2  = Number(process.env.THERMAL_CO2_PER_KWH  || '0.198');  // kg/kWh
const TREE_KG      = 6.6;
const round2 = (v) => Math.round(v * 100) / 100;

// heartbeat(짧은 프레임) 배제: 누적Wh가 실릴 만한 길이 추정(바디 바이트 수)
const MIN_BODYLEN_WITH_WH = 12;
const LEN_WITH_WH_COND = `COALESCE("bodyLength", 9999) >= ${MIN_BODYLEN_WITH_WH}`;

// ✅ ok 파라미터에 따라 err 필터를 제어 (ok=any → 필터 해제)
function okClause(req) {
  const ok = String(req.query.ok || '1').toLowerCase();
  return (ok === 'any' || ok === '0')
    ? '' // 필터 해제
    : "AND split_part(body,' ',5)='00'";
}

// 요청 빈도 제한
const seriesLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 100,
  message: { error: 'Too many requests — try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});

// ─────────────────────────────────────────────
// CO₂ 계수 결정
// ─────────────────────────────────────────────
const CO2_FOR = (energyHex) => {
  const e = (energyHex || '').toLowerCase();
  if (e === '02' || e === '03') return THERMAL_CO2; // 열원
  if (['01', '04', '06', '07'].includes(e)) return ELECTRIC_CO2; // 전기원
  return ELECTRIC_CO2;
};

// 멀티 슬롯 지원 여부(문서 기준: 태양광만 멀티 사용)
const MULTI_SUPPORTED = (energyHex) => (energyHex || '').toLowerCase() === '01';

// ───────────────────── helpers ─────────────────────
function headerFromHex(hex) {
  const parts = (hex || '').trim().split(/\s+/);
  return {
    energy: parts[1] ? parseInt(parts[1], 16) : null,
    type:   parts[2] ? parseInt(parts[2], 16) : null,
    multi:  parts[3] ?? null,              // '00'|'01'|'02'|'03'
    energyHex: (parts[1] || '').toLowerCase(),
  };
}

function pickMetrics(hex) {
  const p = parseFrame(hex);
  const head = headerFromHex(hex);
  if (!p || !p.ok || !p.metrics) {
    return { wh: null, energy: p?.energy ?? head.energy, type: p?.type ?? head.type, multi: head.multi };
  }
  return { wh: p.metrics.cumulativeWh, energy: p.energy, type: p.type, multi: head.multi };
}

function kstDayKey(d) {
  const [y, m, day] = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Seoul', year: 'numeric', month: '2-digit', day: '2-digit' })
    .formatToParts(d).filter(p => p.type !== 'literal').map(p => p.value);
  return `${y}-${m}-${day}`;
}

function kstHourKey(d) {
  const z = new Date(d.toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
  const y = z.getFullYear();
  const m = String(z.getMonth()+1).padStart(2, '0');
  const dd = String(z.getDate()).padStart(2, '0');
  const hh = String(z.getHours()).padStart(2, '0');
  return `${y}-${m}-${dd} ${hh}`;
}

function parseYmd(s) {
  if (!s) return null;
  const t = String(s).trim();
  let m = t.match(/^(\d{4})-(\d{2})-(\d{2})$/) || t.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (!m) return null;
  return { y: +m[1], M: +m[2], d: +m[3] };
}
function kstStartUtc({ y, M, d }) { return new Date(Date.UTC(y, M - 1, d, -9, 0, 0, 0)); }
function kstEndExclusiveUtc({ y, M, d }) { return new Date(Date.UTC(y, M - 1, d + 1, -9, 0, 0, 0)); }

// 풍력 type 자동 유연화: type 미지정/auto → IN('00','01')
function buildTypeCondsForEnergy(energyHex, typeHex, params) {
  const e = (energyHex || '').toLowerCase();
  const t = (typeHex || '').toLowerCase();
  if (e === '04' && (!t || t === 'auto')) {
    return { sql: `split_part(body,' ',3) IN ('00','01')`, added: false };
  }
  if (t) {
    params.push(t);
    return { sql: `split_part(body,' ',3) = $${params.length}`, added: true };
  }
  return { sql: null, added: false };
}

// ───────────────────── endpoint ─────────────────────
router.get('/series', seriesLimiter, async (req, res, next) => {
  try {
    const q = req.query.rtuImei || req.query.imei || req.query.name || req.query.q;
    if (!q) { const e = new Error('rtuImei/imei/name/q 중 하나가 필요합니다.'); e.status = 400; throw e; }
    const imei = await resolveOneImeiOrThrow(q);

    const range = (req.query.range || 'weekly').toLowerCase();  // weekly | monthly | yearly
    const energyHex = (req.query.energy || '01').toLowerCase(); // 01=태양광, 02=태양열, 03=지열, 04=풍력, 06=연료전지, 07=ESS
    const typeHex   = (req.query.type || '').toLowerCase() || null;
    const wantHourly = String(req.query.detail || '').toLowerCase() === 'hourly';
    const multiParam = (req.query.multi || '').toString().toLowerCase();
    const wantMulti = ['00','01','02','03'].includes(multiParam) ? multiParam : null;

    const startQ = parseYmd(req.query.start);
    const endQ   = parseYmd(req.query.end);
    let startUtc, endUtc, bucket;

    if (startQ && endQ) {
      startUtc = kstStartUtc(startQ);
      endUtc   = kstEndExclusiveUtc(endQ);
      bucket   = 'day';
    } else {
      const r = getRangeUtc(range);
      startUtc = r.startUtc;
      endUtc   = r.endUtc;
      bucket   = r.bucket;
      if (range === 'yearly') {
        const now = new Date();
        const nowKst = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
        const y = nowKst.getFullYear();
        startUtc = new Date(Date.UTC(y, 0, 1, -9, 0, 0, 0));
        endUtc = now;
        bucket = 'month';
      }
    }

    // ✅ command=0x14 + heartbeat 배제 + optional err 필터 (okClause)
    const conds = [
      '"rtuImei" = $1',
      '"time" >= $2',
      '"time" < $3',
      "left(body,2)='14'",
      LEN_WITH_WH_COND,
    ];
    const okFilter = okClause(req);
    if (okFilter) conds.push(okFilter.replace(/^AND\s+/, ''));

    const params = [imei, startUtc, endUtc];
    if (energyHex) { conds.push(`split_part(body,' ',2) = $${params.length+1}`); params.push(energyHex); }

    // 풍력 자동 type 유연화
    const tc = buildTypeCondsForEnergy(energyHex, typeHex, params);
    if (tc.sql) conds.push(tc.sql);

    if (wantMulti && MULTI_SUPPORTED(energyHex)) {
      conds.push(`split_part(body,' ',4) = $${params.length+1}`); params.push(wantMulti);
    }

    const sql = `
      SELECT "time", "bodyLength", body
      FROM public.log_rtureceivelog
      WHERE ${conds.join(' AND ')}
      ORDER BY "time" ASC
    `;
    const { rows } = await pool.query(sql, params);

    const perKey = new Map();
    for (const r of rows) {
      const p = parseFrame(r.body);
      const head = headerFromHex(r.body);
      const wh = p?.metrics?.cumulativeWh ?? null;

      if (wh == null) {
        if (head.energyHex === '04')
          console.warn(`[series] 풍력(${imei}) 누적Wh 없음 (time=${r.time}, len=${r.body.split(' ').length}B)`);
        continue;
      }

      const m = MULTI_SUPPORTED(head.energyHex)
        ? (wantMulti || (head.multi || '00'))
        : '00';
      const bkey = bucketKeyKST(new Date(r.time), bucket);
      const key = `${bkey}|${m}`;

      const rec = perKey.get(key) || { firstWh: null, lastWh: null, firstTs: null, lastTs: null };
      if (rec.firstWh == null) { rec.firstWh = wh; rec.firstTs = r.time; }
      rec.lastWh = wh; rec.lastTs = r.time;
      perKey.set(key, rec);
    }

    const co2Factor = CO2_FOR(energyHex);

    const bucketAgg = new Map();
    for (const [key, rec] of perKey.entries()) {
      const [bkey] = key.split('|');
      const kwhRaw = whDeltaToKwh(rec.firstWh, rec.lastWh);
      const kwh = Math.max(0, kwhRaw);
      const cur = bucketAgg.get(bkey) || { kwh: 0, firstAt: null, lastAt: null };
      cur.kwh += kwh;
      cur.firstAt = cur.firstAt == null ? rec.firstTs : cur.firstAt;
      cur.lastAt  = rec.lastTs;
      bucketAgg.set(bkey, cur);
    }

    const keys = Array.from(bucketAgg.keys()).sort();
    const series = keys.map(k => {
      const agg = bucketAgg.get(k);
      const kwh = round2(agg.kwh);
      const co2_kg = round2(kwh * co2Factor);
      const trees  = Math.round(co2_kg / TREE_KG);
      return { bucket: k, kwh, co2_kg, trees, firstAt: agg.firstAt, lastAt: agg.lastAt };
    });

    const total_kwh = round2(series.reduce((s,x)=>s + (x.kwh||0), 0));
    const total_co2_kg = round2(total_kwh * co2Factor);
    const total_trees  = Math.round(total_co2_kg / TREE_KG);

    // ───────── detail_hourly
    let detail_hourly = null;
    if (wantHourly && rows.length) {
      const lastRowTime = rows[rows.length - 1].time;
      const lastDay = kstDayKey(new Date(lastRowTime));
      const perHourMulti = new Map();

      for (const r of rows) {
        const t = new Date(r.time);
        if (kstDayKey(t) !== lastDay) continue;
        const p = parseFrame(r.body);
        const head = headerFromHex(r.body);
        const wh = p?.metrics?.cumulativeWh ?? null;
        if (wh == null) continue;

        // 🔥 멀티 필터링: wantMulti가 있으면 해당 멀티만 반영
        const m = MULTI_SUPPORTED(head.energyHex) ? (head.multi || '00') : '00';
        if (wantMulti && MULTI_SUPPORTED(head.energyHex) && m !== wantMulti) {
          continue;
        }

        const hk = kstHourKey(t).slice(11, 13); // 'HH'
        const key = `${hk}|${m}`;

        const rec = perHourMulti.get(key) || { firstWh: null, lastWh: null };
        if (rec.firstWh == null) rec.firstWh = wh;
        rec.lastWh = wh;
        perHourMulti.set(key, rec);
      }

      const hourAgg = new Map();
      for (const [key, rec] of perHourMulti.entries()) {
        const [hh] = key.split('|');
        const kwh = Math.max(0, whDeltaToKwh(rec.firstWh, rec.lastWh));
        hourAgg.set(hh, (hourAgg.get(hh) || 0) + kwh);
      }

      const rowsHourly = Array.from({ length: 24 }, (_, i) => String(i).padStart(2, '0')).map(hh => {
        const kwh = round2(hourAgg.get(hh) || 0);
        return { hour: `${hh}:00`, kwh, eff_pct: null, weather: null, co2_kg: round2(kwh * co2Factor) };
      });
      detail_hourly = { day: lastDay, rows: rowsHourly };
    }

    res.json({
      deviceInfo: { rtuImei: imei, tz: TZ },
      params: {
        range,
        energy_hex: energyHex,
        type_hex: typeHex,
        multi: (wantMulti && MULTI_SUPPORTED(energyHex)) ? wantMulti : 'all',
        detail: wantHourly ? 'hourly' : undefined,
        ok: req.query.ok || '00',
      },
      bucket,
      range_utc: { start: startUtc, end: endUtc },
      series,
      detail_hourly,
      summary: { total_kwh, total_co2_kg, total_trees },
    });
  } catch (e) { next(e); }
});

module.exports = router;
