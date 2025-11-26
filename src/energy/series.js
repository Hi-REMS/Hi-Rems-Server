const express = require('express');
const router = express.Router();
const rateLimit = require('express-rate-limit');
const { pool } = require('../db/db.pg');
const { parseFrame } = require('./parser');
const { TZ, getRangeUtc, bucketKeyKST, whDeltaToKwh } = require('./timeutil');
const { resolveOneImeiOrThrow } = require('./devices');

const ELECTRIC_CO2 = Number(process.env.ELECTRIC_CO2_PER_KWH || '0.4747');
const THERMAL_CO2  = Number(process.env.THERMAL_CO2_PER_KWH  || '0.198');
const TREE_KG      = 6.6;
const round2 = (v) => Math.round(v * 100) / 100;

const MIN_BODYLEN_WITH_WH = 12;
const LEN_WITH_WH_COND = `COALESCE("bodyLength", 9999) >= ${MIN_BODYLEN_WITH_WH}`;

const RECENT_WINDOW_BY_ENERGY = {
  '01': 365,
  '02': 365,
  '03': 365,
  '04': 14,
  '06': 14,
  '07': 14,
};

function okClause(req) {
  const ok = String(req.query.ok || '1').toLowerCase();
  return (ok === 'any' || ok === '0') ? '' : "AND split_part(body,' ',5)='00'";
}

const seriesLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 100,
  message: { error: 'Too many requests — try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});

const CO2_FOR = (energyHex) => {
  const e = (energyHex || '').toLowerCase();
  if (e === '02' || e === '03') return THERMAL_CO2;
  return ELECTRIC_CO2;
};

const MULTI_SUPPORTED = (energyHex) => (energyHex || '').toLowerCase() === '01';

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
    timeZone: 'Asia/Seoul', year: 'numeric', month: '2-digit', day: '2-digit'
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

function kstStartUtc({ y, M, d }) { return new Date(Date.UTC(y, M - 1, d, -9)); }
function kstEndExclusiveUtc({ y, M, d }) { return new Date(Date.UTC(y, M - 1, d + 1, -9)); }

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

    const startQ = parseYmd(req.query.start);
    const endQ = parseYmd(req.query.end);
    let startUtc, endUtc, bucket;

    if (startQ && endQ) {
      startUtc = kstStartUtc(startQ);
      endUtc   = kstEndExclusiveUtc(endQ);
      bucket   = 'day';
    } else if (range === 'yearly') {
      // KST 기준 지금 시각
      const nowKST = new Date(
        new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' })
      );

      // 조회 연도: 예) 2024, 2025 (없으면 현재 연도)
      const year = Number(req.query.year) || nowKST.getFullYear();

      // 조회 연도 1월 1일 00:00 KST → UTC로 변환(-9h)
      startUtc = new Date(Date.UTC(year, 0, 1, -9));

      // [수정] 과거 연도(2023 등) 조회 시 종료 시점은 그 해 12월 31일로 고정
      if (year < nowKST.getFullYear()) {
        // 해당 연도 12월 31일 23:59:59 KST
        endUtc = new Date(Date.UTC(year, 11, 31, 23, 59, 59, -9));
      } else {
        // 올해라면 현재 시각까지
        endUtc = nowKST;
      }

      bucket = 'day';  // 일 단위 집계
    } else {
      const r = getRangeUtc(range);
      startUtc = r.startUtc;
      endUtc = r.endUtc;
      bucket = r.bucket;
    }

    // [수정] yearly 모드일 때는 365일 제한 로직을 건너뜀 (null 처리)
    const limitDays = (range === 'yearly') ? null : RECENT_WINDOW_BY_ENERGY[energyHex];
    
    if (limitDays) {
      const maxWindowMs = limitDays * 86400 * 1000;
      const requestedDiff = endUtc.getTime() - startUtc.getTime();
      if (requestedDiff > maxWindowMs) {
        startUtc = new Date(endUtc.getTime() - maxWindowMs);
        if (bucket === 'month') bucket = 'day';
      }
    }

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

    // [수정] 인덱스(split_part)를 활용하기 위해 주석 해제!
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
    const perKey = new Map();

    // [샘플링 설정] 태양광(01)은 0(모두 처리), 그 외는 10분 단위
    let SAMPLE_INTERVAL_MS = (energyHex === '01') ? 0 : 10 * 60 * 1000;
    let lastProcessedTime = 0;

    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const currentTime = new Date(r.time).getTime();

      const isFirst = (i === 0);
      const isLast  = (i === rows.length - 1);

      // 1. [수정] 에너지원 검증을 가장 먼저 수행
      const head = headerFromHex(r.body);
      if (energyHex && head.energyHex !== energyHex) {
         continue; 
      }

      // 2. [수정] 그 다음 샘플링 체크
      if (!isFirst && !isLast && SAMPLE_INTERVAL_MS > 0) {
        if (currentTime - lastProcessedTime < SAMPLE_INTERVAL_MS) {
          continue; 
        }
      }

      // 3. 파싱
      const p = parseFrame(r.body); 
      const wh = p?.metrics?.cumulativeWh ?? null;
      if (wh == null) continue;

      // 4. [수정] 모든 관문을 통과했을 때만 시간 갱신
      lastProcessedTime = currentTime;

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

    let detail_hourly = null;

    if (wantHourly && rows.length && range !== 'yearly') {
      const lastRowTime = rows[rows.length-1].time;
      const lastDay = kstDayKey(new Date(lastRowTime));
      const perHourMap = new Map();

      let lastHourlyProcessedTime = 0;
      let HOURLY_SAMPLE_MS = (energyHex === '01') ? 0 : 60 * 1000;

      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const t = new Date(r.time);
        if (kstDayKey(t) !== lastDay) continue;

        const currentTime = t.getTime();
        const isLast = (i === rows.length - 1);
        
        // [수정] 상세 데이터도 에너지 검증을 먼저 수행
        const head = headerFromHex(r.body);
        if (energyHex && head.energyHex !== energyHex) continue;

        // [수정] 그 다음 샘플링
        if (!isLast && HOURLY_SAMPLE_MS > 0 && (currentTime - lastHourlyProcessedTime < HOURLY_SAMPLE_MS)) {
             continue;
        }

        const p = parseFrame(r.body);
        const wh = p?.metrics?.cumulativeWh ?? null;
        if (wh == null) continue;
        
        // [수정] 통과 후 시간 갱신
        lastHourlyProcessedTime = currentTime;

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
        return { hour: `${hh}:00`, kwh, eff_pct:null, weather:null, co2_kg: round2(kwh * co2Factor) };
      });
      detail_hourly = { day:lastDay, rows: rowsHourly };
    }

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