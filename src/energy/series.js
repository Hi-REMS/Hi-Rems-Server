// src/energy/series.js
// 전기 에너지 시리즈 데이터 집계 (멀티 설비 대응)
// - log_rtureceivelog에서 Hex 프레임 조회
// - (bucket, multi)별 first/last 누적Wh 차분 → kWh
// - ?multi=00|01|02|03 지정 시 해당 설비만, 미지정/['', 'all']이면 전체 합계
// - range=weekly/monthly/yearly + detail=hourly 지원
// - 응답: { deviceInfo, params, bucket, range_utc, series, detail_hourly, summary }
// - 각 버킷 마다 Co2 저감량, kWh, 식수 그루, firstAt / lastAt

const express = require('express');
const router = express.Router();
const rateLimit = require('express-rate-limit');
const { pool } = require('../db/db.pg');
const { parseFrame } = require('./parser');
const { TZ, getRangeUtc, bucketKeyKST, whDeltaToKwh } = require('./timeutil');

const CO2_FACTOR = 0.4747; // kg/kWh
const TREE_KG = 6.6;
const round2 = v => Math.round(v * 100) / 100;

const isImeiLike = s => typeof s === 'string' && s.length >= 8;
const ONLY_OK = `AND split_part(body,' ',5) = '00'`;

// ★ 이 엔드포인트 전용 레이트 리미터 (1분에 최대 10회)
const seriesLimiter = rateLimit({
  windowMs: 60 * 1000, // 1분
  max: 10,
  message: { error: 'Too many requests — try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});

// 헤더 파트 파싱
function headerFromHex(hex) {
  const parts = (hex || '').trim().split(/\s+/);
  return {
    energy: parts[1] ? parseInt(parts[1], 16) : null,
    type:   parts[2] ? parseInt(parts[2], 16) : null,
    multi:  parts[3] ?? null, // '00'|'01'|'02'|'03'
  };
}

// 누적Wh / 에너지타입만 안전 추출
function pickMetrics(hex) {
  const p = parseFrame(hex);
  const head = headerFromHex(hex);
  if (!p || !p.ok || !p.metrics) {
    return { wh: null, energy: p?.energy ?? head.energy, type: p?.type ?? head.type, multi: head.multi };
  }
  return { wh: p.metrics.cumulativeWh, energy: p.energy, type: p.type, multi: head.multi };
}

// YYYY-MM-DD (KST)
function kstDayKey(d) {
  const [y, m, day] = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Seoul', year: 'numeric', month: '2-digit', day: '2-digit' })
    .formatToParts(d).filter(p => p.type !== 'literal').map(p => p.value);
  return `${y}-${m}-${day}`;
}

// YYYY-MM-DD HH (KST)
function kstHourKey(d) {
  const z = new Date(d.toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
  const y = z.getFullYear();
  const m = String(z.getMonth()+1).padStart(2, '0');
  const dd = String(z.getDate()).padStart(2, '0');
  const hh = String(z.getHours()).padStart(2, '0');
  return `${y}-${m}-${dd} ${hh}`;
}

// yyyymmdd / yyyy-mm-dd → {y,M,d}
function parseYmd(s) {
  if (!s) return null;
  const t = String(s).trim();
  let m = t.match(/^(\d{4})-(\d{2})-(\d{2})$/) || t.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (!m) return null;
  return { y: +m[1], M: +m[2], d: +m[3] };
}
// KST 00:00 → UTC
function kstStartUtc({ y, M, d }) {
  return new Date(Date.UTC(y, M - 1, d, -9, 0, 0, 0));
}
// KST 다음날 00:00(exclusive) → UTC
function kstEndExclusiveUtc({ y, M, d }) {
  return new Date(Date.UTC(y, M - 1, d + 1, -9, 0, 0, 0));
}

// ★ seriesLimiter를 미들웨어로 추가
router.get('/series', seriesLimiter, async (req, res, next) => {
  try {
    // ===== 파라미터 =====
    const imei = req.query.rtuImei || req.query.imei;
    if (!isImeiLike(imei)) {
      const e = new Error('rtuImei(또는 imei) 파라미터가 필요합니다.');
      e.status = 400; throw e;
    }
    const range = (req.query.range || 'weekly').toLowerCase();  // weekly | monthly | yearly
    const energyHex = (req.query.energy || '01').toLowerCase();
    const typeHex   = (req.query.type || '').toLowerCase() || null;
    const wantHourly = String(req.query.detail || '').toLowerCase() === 'hourly';

    // 멀티 필터: '00'|'01'|'02'|'03'|''|'all'
    const multiParam = (req.query.multi || '').toString().toLowerCase();
    const wantMulti = ['00','01','02','03'].includes(multiParam) ? multiParam : null; // null이면 전체 합산

    // ===== 기간 산정 =====
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
      bucket   = r.bucket; // 'day' | 'month'
      // ✅ 연간은 YTD 고정 (올해 1/1 00:00 KST ~ 현재), 월 버킷
      if (range === 'yearly') {
        const now = new Date();
        const nowKst = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
        const y = nowKst.getFullYear();
        startUtc = new Date(Date.UTC(y, 0, 1, -9, 0, 0, 0));
        endUtc = now;
        bucket = 'month';
      }
    }

    // ===== 데이터 조회 =====
    const conds = [`"rtuImei" = $1`, `"time" >= $2`, `"time" < $3`, ONLY_OK.replace(/^AND\s+/, '')];
    const params = [imei, startUtc, endUtc];
    if (energyHex) { conds.push(`left(body,2)='14' AND split_part(body,' ',2) = $${params.length+1}`); params.push(energyHex); }
    if (typeHex)   { conds.push(`split_part(body,' ',3) = $${params.length+1}`);                       params.push(typeHex);   }
    if (wantMulti) { conds.push(`split_part(body,' ',4) = $${params.length+1}`);                       params.push(wantMulti); }

    const sql = `
      SELECT "time", body
      FROM public.log_rtureceivelog
      WHERE ${conds.join(' AND ')}
      ORDER BY "time" ASC
    `;
    const { rows } = await pool.query(sql, params);

    // ===== (bucket, multi)별 first/last 누적Wh 기록 =====
    const perKey = new Map(); // key: `${bkey}|${multi}`
    for (const r of rows) {
      const pm = pickMetrics(r.body);
      if (pm.wh == null) continue;

      const m = wantMulti || (headerFromHex(r.body).multi || '00'); // 합산 모드면 프레임의 멀티 사용
      const bkey = bucketKeyKST(new Date(r.time), bucket);          // YYYY-MM or YYYY-MM-DD
      const key = `${bkey}|${m}`;

      const rec = perKey.get(key) || { firstWh: null, lastWh: null, firstTs: null, lastTs: null };
      if (rec.firstWh == null) { rec.firstWh = pm.wh; rec.firstTs = r.time; }
      rec.lastWh = pm.wh; rec.lastTs = r.time;
      perKey.set(key, rec);
    }

    // ===== 버킷별 합산(kWh/CO₂/식수) =====
    const bucketAgg = new Map(); // bkey -> { kwh, firstAt?, lastAt? }
    for (const [key, rec] of perKey.entries()) {
      const [bkey] = key.split('|');
      const kwhRaw = whDeltaToKwh(rec.firstWh, rec.lastWh);
      const kwh = Math.max(0, kwhRaw); // 장비 리셋 등 음수 방지
      const cur = bucketAgg.get(bkey) || { kwh: 0, firstAt: null, lastAt: null };
      cur.kwh += kwh;
      // 대표 타임스탬프(정보성): first는 최초 1회, last는 최신으로 갱신
      cur.firstAt = cur.firstAt == null ? rec.firstTs : cur.firstAt;
      cur.lastAt  = rec.lastTs;
      bucketAgg.set(bkey, cur);
    }

    const keys = Array.from(bucketAgg.keys()).sort();
    const series = keys.map(k => {
      const agg = bucketAgg.get(k);
      const kwh = round2(agg.kwh);
      const co2_kg = round2(kwh * CO2_FACTOR);
      const trees  = Math.round(co2_kg / TREE_KG);
      return {
        bucket: k,
        kwh,
        co2_kg,
        trees,
        firstAt: agg.firstAt,
        lastAt:  agg.lastAt,
      };
    });

    // 총합
    const total_kwh = round2(series.reduce((s,x)=>s + (x.kwh||0), 0));
    const total_co2_kg = round2(total_kwh * CO2_FACTOR);
    const total_trees  = Math.round(total_co2_kg / TREE_KG);

    // ===== 시간대 상세 =====
    let detail_hourly = null;
    if (wantHourly && rows.length) {
      const lastRowTime = rows[rows.length - 1].time;
      const lastDay = kstDayKey(new Date(lastRowTime));

      const perHourMulti = new Map(); // `${HH}|${multi}` -> { firstWh, lastWh }
      for (const r of rows) {
        const t = new Date(r.time);
        if (kstDayKey(t) !== lastDay) continue;
        const pm = pickMetrics(r.body);
        if (pm.wh == null) continue;

        const m = wantMulti || (headerFromHex(r.body).multi || '00');
        const hk = kstHourKey(t).slice(11, 13); // 'HH'
        const key = `${hk}|${m}`;

        const rec = perHourMulti.get(key) || { firstWh: null, lastWh: null };
        if (rec.firstWh == null) rec.firstWh = pm.wh;
        rec.lastWh = pm.wh;
        perHourMulti.set(key, rec);
      }

      // 시간별 합산
      const hourAgg = new Map(); // 'HH' -> kWh
      for (const [key, rec] of perHourMulti.entries()) {
        const [hh] = key.split('|');
        const kwh = Math.max(0, whDeltaToKwh(rec.firstWh, rec.lastWh));
        hourAgg.set(hh, (hourAgg.get(hh) || 0) + kwh);
      }

      const hkeys = Array.from(hourAgg.keys()).sort();
      const rowsHourly = hkeys.map(hh => {
        const kwh = round2(hourAgg.get(hh) || 0);
        return { hour: `${hh}:00`, kwh, eff_pct: null, weather: null, co2_kg: round2(kwh * CO2_FACTOR) };
      });

      detail_hourly = { day: lastDay, rows: rowsHourly };
    }

    res.json({
      deviceInfo: { rtuImei: imei, tz: TZ },
      params: {
        range,
        energy_hex: energyHex,
        type_hex: typeHex,
        multi: wantMulti || 'all',
        detail: wantHourly ? 'hourly' : undefined
      },
      bucket,                  
      range_utc: { start: startUtc, end: endUtc },
      series,
      detail_hourly,
      summary: { total_kwh, total_co2_kg, total_trees }
    });
  } catch (e) { next(e); }
});

module.exports = router;
