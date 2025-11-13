// src/energy/service.js
// ──────────────────────────────────────────────────────────────
// Hi-REMS Energy Service Router
// - 전기(태양광/풍력/연료전지/ESS) + 열(태양열/지열) 포함
// - 공통 KPI/preview/debug/instant/instant/multi/hourly 제공
// - parser.js가 에너지원/타입별 파싱을 자동 처리
// - 성능개선:
//   · 최근 윈도우 하한(기본 14일)로 latest 조회 범위 제한
//   · KPI의 /series 병합 비차단화(짧은 타임아웃, 병렬 시작)
//   · 풍력 type 자동 유연화 + heartbeat 배제
// ──────────────────────────────────────────────────────────────
const express = require('express');
const router = express.Router();
const rateLimit = require('express-rate-limit');
const { pool } = require('../db/db.pg');
const { parseFrame } = require('./parser');
const { TZ } = require('./timeutil');
const { resolveOneImeiOrThrow } = require('./devices');
const { DateTime } = require('luxon');
const axios = require('axios');

// ──────────────────────────────────────────────────────────────
// 상수
// ──────────────────────────────────────────────────────────────
const ELECTRIC_CO2 = Number(process.env.ELECTRIC_CO2_PER_KWH || '0.4747'); // kg/kWh
const THERMAL_CO2  = Number(process.env.THERMAL_CO2_PER_KWH  || '0.198');  // kg/kWh
const TREE_CO2_KG  = 6.6;

const RECENT_WINDOW_BY_ENERGY = {
  '01': 14,  // 태양광
  '02': 7,   // 태양열
  '03': 7,   // 지열
  '04': 14,  // 풍력
  '06': 14,  // 연료전지
  '07': 14,  // ESS
};

function recentSqlFor(energyHex) {
  const days = RECENT_WINDOW_BY_ENERGY[energyHex] || 14;
  return `"time" >= (now() - interval '${days} days')`;
}

// CO₂ 계수: 태양열/지열 → 열 계수, 그 외 → 전기 계수
const CO2_FOR = (energyHex) => {
  const e = (energyHex || '').toLowerCase();
  if (e === '02' || e === '03') return THERMAL_CO2;
  return ELECTRIC_CO2;
};

// 멀티 지원 여부(문서상 태양광만 멀티)
const MULTI_SUPPORTED = (energyHex='') => (energyHex || '').toLowerCase() === '01';

// --------------------------------------------------
// Rate limiters
// --------------------------------------------------
const makeLimiter = (maxPerMin) =>
  rateLimit({
    windowMs: 60 * 1000,
    max: maxPerMin,
    message: { error: 'Too many requests — try again later.' },
    standardHeaders: true,
    legacyHeaders: false,
  });

const limiterKPI       = makeLimiter(15);
const limiterPreview   = makeLimiter(30);
const limiterDebug     = makeLimiter(10);
const limiterInstant   = makeLimiter(30);
const limiterInstantM  = makeLimiter(30);
const limiterHourly    = makeLimiter(10);

// BigInt 안전 직렬화
const jsonSafe = (obj) =>
  JSON.parse(JSON.stringify(obj, (_, v) => (typeof v === 'bigint' ? v.toString() : v)));

// ──────────────────────────────────────────────────────────────
// SQL 공통 상수/조건
// ──────────────────────────────────────────────────────────────
const ERR_EQ_OK        = `split_part(body,' ',5) = '00'`;
const ERR_EQ_OK_OR_02  = `(split_part(body,' ',5) = '00' OR split_part(body,' ',5) = '02')`;

// heartbeat(짧은 프레임) 배제
const MIN_BODYLEN_WITH_WH = 12;
const LEN_WITH_WH_COND    = `COALESCE("bodyLength", 9999) >= ${MIN_BODYLEN_WITH_WH}`;

// 항상 command=0x14
const CMD_IS_14 = `left(body,2)='14'`;

/* ---------- 공통 유틸 ---------- */
const mapByMulti = (rows) => {
  const m = new Map();
  for (const r of rows || []) {
    if (r && r.multi_hex) m.set(r.multi_hex, r);
  }
  return m;
};

// 풍력(type 자동 유연화): type 미지정/auto → IN('00','01')
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

/* ====== 항상 command=0x14 & err=00 & (길이 필터) 를 걸고
         최신 조회에 "최근 N일" 하한선을 추가 ====== */

// 최신 1건(단일)
async function lastBeforeNow(imei, energyHex = null, typeHex = null) {
  const params = [imei];
  const conds = [
    `"rtuImei" = $1`,
    recentSqlFor(energyHex),
    CMD_IS_14,
    ERR_EQ_OK,
    LEN_WITH_WH_COND,
  ];
  if (energyHex) { params.push(energyHex); conds.push(`split_part(body,' ',2) = $${params.length}`); }
  const tc = buildTypeCondsForEnergy(energyHex, typeHex, params);
  if (tc.sql) conds.push(tc.sql);

  const sql = `
    SELECT "time", body
    FROM public.log_rtureceivelog
    WHERE ${conds.join(' AND ')}
    ORDER BY "time" DESC
    LIMIT 1`;
  const r = await pool.query(sql, params);
  return r.rows[0] || null;
}

// 최신 1건(멀티)
async function lastBeforeNowByMulti(imei, { energyHex=null, typeHex=null, multiHex=null } = {}) {
  const params = [imei];
  const conds = [
    `"rtuImei" = $1`,
    recentSqlFor(energyHex),
    CMD_IS_14,
    ERR_EQ_OK,
    LEN_WITH_WH_COND,
  ];
  if (energyHex) { params.push(energyHex); conds.push(`split_part(body,' ',2) = $${params.length}`); }
  const tc = buildTypeCondsForEnergy(energyHex, typeHex, params);
  if (tc.sql) conds.push(tc.sql);
  if (multiHex)  { params.push(multiHex);  conds.push(`split_part(body,' ',4) = $${params.length}`); }

  const sql = `
    SELECT "time", body
    FROM public.log_rtureceivelog
    WHERE ${conds.join(' AND ')}
    ORDER BY "time" DESC
    LIMIT 1`;
  const r = await pool.query(sql, params);
  return r.rows[0] || null;
}

// 멀티별 최신
async function latestPerMulti(imei, { energyHex=null, typeHex=null } = {}) {
  const params = [imei];
  const conds = [
    `"rtuImei" = $1`,
    recentSqlFor(energyHex),
    CMD_IS_14,
    ERR_EQ_OK,
    LEN_WITH_WH_COND,
  ];
  if (energyHex) { params.push(energyHex); conds.push(`split_part(body,' ',2) = $${params.length}`); }
  const tc = buildTypeCondsForEnergy(energyHex, typeHex, params);
  if (tc.sql) conds.push(tc.sql);

  const sql = `
    SELECT DISTINCT ON (split_part(body,' ',4))
      split_part(body,' ',4) AS multi_hex, "time", body
    FROM public.log_rtureceivelog
    WHERE ${conds.join(' AND ')}
    ORDER BY split_part(body,' ',4), "time" DESC
  `;
  const { rows } = await pool.query(sql, params);
  return rows.map(r => ({ multi_hex: r.multi_hex, time: r.time || null, body: r.body || null }));
}

async function firstAfterPerMulti(imei, tsUtc, { energyHex=null, typeHex=null } = {}) {
  const params = [imei, tsUtc];
  const conds = [
    `"rtuImei" = $1`,
    `"time" >= $2`,
    CMD_IS_14,
    ERR_EQ_OK,
    LEN_WITH_WH_COND,
  ];
  if (energyHex) { params.push(energyHex); conds.push(`split_part(body,' ',2) = $${params.length}`); }
  const tc = buildTypeCondsForEnergy(energyHex, typeHex, params);
  if (tc.sql) conds.push(tc.sql);

  const sql = `
    SELECT DISTINCT ON (split_part(body,' ',4))
      split_part(body,' ',4) AS multi_hex, "time", body
    FROM public.log_rtureceivelog
    WHERE ${conds.join(' AND ')}
    ORDER BY split_part(body,' ',4), "time" ASC
  `;
  const { rows } = await pool.query(sql, params);
  return rows.map(r => ({ multi_hex: r.multi_hex, time: r.time || null, body: r.body || null }));
}

/* ---------- parsing helpers ---------- */
function headerFromHex(hex) {
  const parts = (hex || '').trim().split(/\s+/);
  return {
    energy: parts[1] ? parseInt(parts[1], 16) : null,
    type:   parts[2] ? parseInt(parts[2], 16) : null,
    multi:  parts[3] || null,
    energyHex: (parts[1] || '').toLowerCase(),
  };
}

// 효율 계산 전용 헬퍼: 태양광 기준 (AC출력 / DC입력 * 100)
function computeInverterEfficiency(m) {
  const inputW = (() => {
    if (Number.isFinite(Number(m.pvPowerW)))   return Number(m.pvPowerW);
    if (Number.isFinite(Number(m.pvOutputW)))  return Number(m.pvOutputW);
    if (m.pvVoltage != null && m.pvCurrent != null) {
      return Number(m.pvVoltage) * Number(m.pvCurrent);
    }
    return null;
  })();

  const outputW = (() => {
    if (Number.isFinite(Number(m.currentOutputW)))  return Number(m.currentOutputW);
    if (Number.isFinite(Number(m.inverterOutputW))) return Number(m.inverterOutputW);
    if (m.systemVoltage != null && m.systemCurrent != null && m.powerFactor != null) {
      return Number(m.systemVoltage) * Number(m.systemCurrent) * Number(m.powerFactor);
    }
    if (
      m.systemR_V != null && m.systemS_V != null && m.systemT_V != null &&
      m.systemR_I != null && m.systemS_I != null && m.systemT_I != null &&
      m.powerFactor != null
    ) {
      const sumVI =
        (Number(m.systemR_V) * Number(m.systemR_I)) +
        (Number(m.systemS_V) * Number(m.systemS_I)) +
        (Number(m.systemT_V) * Number(m.systemT_I));
      return sumVI * Number(m.powerFactor);
    }
    return null;
  })();

  if (!Number.isFinite(inputW) || inputW <= 0)  return null;
  if (!Number.isFinite(outputW) || outputW <= 0) return null;

  let eff = (outputW / inputW) * 100;
  if (eff < 0 || eff > 120) return null;
  return Math.round(eff * 100) / 100;
}

function pickMetrics(hex) {
  const p = parseFrame(hex);
  const head = headerFromHex(hex);
  if (!p || !p.ok || !p.metrics) {
    return { wh:null, w:null, eff:null, energy: p?.energy ?? head.energy, type: p?.type ?? head.type };
  }
  const m = p.metrics;
  const wh = Object.prototype.hasOwnProperty.call(m, 'cumulativeWh') ? m.cumulativeWh : null;
  const wCand = [ m.currentOutputW, m.postOutputW, m.outputW, m.inverterOutputW ]
    .find(v => Number.isFinite(Number(v)));
  const w = Number.isFinite(Number(wCand)) ? Number(wCand) : null;
  const eff = computeInverterEfficiency(m);
  return { wh, w, eff, energy: p.energy, type: p.type };
}

/* ---------- 상태/동작 추론 ---------- */
function geoStateTextFrom(m = {}) {
  if (typeof m.state === 'string' && m.state.trim()) return m.state.trim();
  const raw = (m.stateRaw != null) ? Number(m.stateRaw) : null;
  switch (raw) {
    case 0: return '미작동';
    case 1: return '냉방';
    case 2: return '난방';
    default: return null;
  }
}
function inferIsOperating(m = {}) {
  if (typeof m.isOperating === 'boolean') return m.isOperating;
  if (m.stateRaw != null) return Number(m.stateRaw) > 0;
  return null;
}

/* ---------- /series 병합 헬퍼 (비차단) ---------- */
async function fetchSeriesHourlyNonBlocking({ imei, energyHex, typeHex, multiHex }) {
  try {
    const baseKst = DateTime.now().setZone(TZ).toFormat('yyyy-LL-dd');
    const url = new URL('http://localhost:3000/api/energy/series');
    url.searchParams.set('imei', imei);
    url.searchParams.set('energy', energyHex);
    url.searchParams.set('start', baseKst);
    url.searchParams.set('end', baseKst);
    url.searchParams.set('detail', 'hourly');
    if (typeHex) url.searchParams.set('type', typeHex);
    if (multiHex && MULTI_SUPPORTED(energyHex)) url.searchParams.set('multi', multiHex);

    // ⏱️ 더 짧은 타임아웃(2.5s) — 실패해도 KPI는 바로 응답
    const { data } = await axios.get(url.toString(), { timeout: 2500 });
    return data?.detail_hourly || null;
  } catch (e) {
    return null; // 병합 실패 시 null
  }
}

/* ---------- 공용 핸들러 ---------- */

// KPI 요약 (멀티 합산) + detail_hourly 병합(비차단)
async function handleKPI(req, res, next, defaultEnergyHex = '01') {
  try {
    const q = req.query.rtuImei || req.query.imei || req.query.name || req.query.q;
    if (!q) { const e = new Error('rtuImei/imei/name/q 중 하나가 필요합니다.'); e.status = 400; throw e; }
    const imei = await resolveOneImeiOrThrow(q);

let energyHex = (req.query.energy || defaultEnergyHex).toLowerCase();
    const typeHex   = (req.query.type   || '').toLowerCase() || null;
    const multiHexQ = (req.query.multi  || '').toLowerCase() || null;
    const selectedMulti =
      typeof multiHexQ === 'string' && /^[0-9a-f]{2}$/.test(multiHexQ) ? multiHexQ : null;

    const co2Factor = CO2_FOR(energyHex);

    // 📌 /series 병합을 가능한 빨리 시작(병렬)
    const seriesPromise = fetchSeriesHourlyNonBlocking({
      imei, energyHex, typeHex, multiHex: selectedMulti
    });

    // 기준 시각들 (KST 경계)
    const dayB = await pool.query(`
      SELECT
        (date_trunc('day', (now() AT TIME ZONE '${TZ}')) AT TIME ZONE '${TZ}') AS start_utc,
        now() AS now_utc
    `);
    const monthB = await pool.query(`
      SELECT
        (date_trunc('month', ((now() AT TIME ZONE '${TZ}') - interval '1 month')) AT TIME ZONE '${TZ}') AS prev_month_utc,
        (date_trunc('month',  (now() AT TIME ZONE '${TZ}'))                         AT TIME ZONE '${TZ}') AS this_month_utc
    `);
    const { start_utc } = dayB.rows[0];
    const { prev_month_utc, this_month_utc } = monthB.rows[0];

    const [latestRows, todayFirstRows, prevMonthFirstRows, thisMonthFirstRows] = await Promise.all([
      latestPerMulti(imei, { energyHex, typeHex }),
      firstAfterPerMulti(imei, start_utc,      { energyHex, typeHex }),
      firstAfterPerMulti(imei, prev_month_utc, { energyHex, typeHex }),
      firstAfterPerMulti(imei, this_month_utc, { energyHex, typeHex }),
    ]);

    const anyLatest = latestRows.some(r => r?.body);
    if (!anyLatest) {
      return res.status(422).json({
        error:'no_frames_for_energy',
        message:`해당 IMEI에서 energy=0x${energyHex}${typeHex?` type=0x${typeHex}`:''} 정상(0x00) 프레임이 없습니다.`
      });
    }

    // ── 누적/현재/효율(최신 멀티 전체 기준)
    let totalWhSum = 0n; let haveWh = false; let nowWSum = 0; const effList = [];
    for (const r of latestRows) {
      if (!r?.body) continue;
      const p = pickMetrics(r.body);
      if (p.wh != null) { totalWhSum += p.wh; haveWh = true; }
      if (p.w  != null) nowWSum += p.w;
      if (typeof p.eff === 'number') effList.push(p.eff);
    }
    const total_kwh = haveWh ? Number(totalWhSum) / 1000 : null;
    const total_mwh = haveWh ? Number(totalWhSum) / 1_000_000 : null;
    const now_kw    = nowWSum ? Math.round((nowWSum/1000)*100)/100 : null;
    const inverter_efficiency_pct = effList.length
      ? Math.round((effList.reduce((a,b)=>a+b,0)/effList.length)*10)/10
      : null;

    // ── 금일 발전량(today_kwh) — 멀티 선택 시 해당 멀티만
    const latestMap     = mapByMulti(latestRows);
    const todayFirstMap = mapByMulti(todayFirstRows);
    let todayWhSum = 0n; let haveToday = false;
    for (const [multi, Lrow] of latestMap.entries()) {
      if (selectedMulti && multi !== selectedMulti) continue;
      const Frow = todayFirstMap.get(multi);
      if (!Lrow?.body || !Frow?.body) continue;
      const L = pickMetrics(Lrow.body);
      const F = pickMetrics(Frow.body);
      if (L?.wh != null && F?.wh != null && L.wh >= F.wh) {
        todayWhSum += (L.wh - F.wh);
        haveToday = true;
      }
    }
    const today_kwh = haveToday ? Math.max(0, Number(todayWhSum)/1000) : null;

    // ── 지난달~이번달 평균출력(전체 멀티 기준)
    const thisMonthMap = mapByMulti(thisMonthFirstRows);
    const prevMonthMap = mapByMulti(prevMonthFirstRows);
    let monthDiffWhSum = 0n; let haveMonth = false;
    for (const [multi, Arow] of thisMonthMap.entries()) {
      const Brow = prevMonthMap.get(multi);
      if (!Arow?.body || !Brow?.body) continue;
      const A = pickMetrics(Arow.body);
      const B = pickMetrics(Brow.body);
      if (A?.wh != null && B?.wh != null && A.wh >= B.wh) {
        monthDiffWhSum += (A.wh - B.wh);
        haveMonth = true;
      }
    }
    let last_month_avg_kw = null;
    if (haveMonth) {
      const hours = (new Date(this_month_utc) - new Date(prev_month_utc)) / 3600_000;
      if (hours > 0) last_month_avg_kw = Math.round(((Number(monthDiffWhSum)/1000)/hours)*100)/100;
    }

    // CO2/나무 변환
    const co2_kg  = total_kwh != null ? Math.round(total_kwh*co2Factor*100)/100 : null;
    const co2_ton = co2_kg   != null ? Math.round((co2_kg/1000)*100)/100 : null;
    const trees   = co2_kg   != null ? Math.floor(co2_kg / TREE_CO2_KG) : null;

    // 최신 시각
    const latestAt = latestRows
      .map(r => r?.time ? new Date(r.time).getTime() : 0)
      .reduce((a,b)=>Math.max(a,b),0);
    const latestAtIso = latestAt ? new Date(latestAt).toISOString() : null;

    // ✅ 병렬 시작해둔 /series 병합 결과 수집(실패 시 null)
    const detail_hourly = await seriesPromise;

    // 응답
    res.json({
      deviceInfo: { rtuImei: imei, latestAt: latestAtIso },
      kpis: {
        now_kw,
        today_kwh,
        total_kwh: total_kwh != null ? Math.round(total_kwh*100)/100 : null,
        total_mwh: total_mwh != null ? Math.round(total_mwh*1000)/1000 : null,
        co2_kg, co2_ton, trees, last_month_avg_kw,
        inverter_efficiency_pct,
      },
      detail_hourly,
      meta: {
        table:'public.log_rtureceivelog',
        tz: TZ,
        emission_factor_kg_per_kwh: co2Factor,
        energy_hex: energyHex,
        type_hex: typeHex,
        multi: selectedMulti || 'all',
        recent_window_days: RECENT_WINDOW_BY_ENERGY[energyHex] || 14,
      }
    });
  } catch (e) {
    next(e);
  }
}

// 프리뷰 (최근 프레임 시계열) — 디버깅 뷰이므로 길이 필터 적용 안 함
async function handlePreview(req, res, next, defaultEnergyHex = '') {
  try {
    const q = req.query.rtuImei || req.query.imei || req.query.name || req.query.q;
    let limit = Math.min(parseInt(req.query.limit || '200', 10), 2000);
    if (!q) { const e = new Error('rtuImei/imei/name/q 중 하나가 필요합니다.'); e.status = 400; throw e; }
    const imei = await resolveOneImeiOrThrow(q);
let energyHex = (req.query.energy || defaultEnergyHex).toLowerCase();
    const typeHex   = (req.query.type   || '').toLowerCase();
    const onlyOk    = String(req.query.ok || '') === '1';
    const multiHex  = (req.query.multi  || '').toLowerCase();

    const conds = ['"rtuImei" = $1', CMD_IS_14];
    const params = [imei];
    if (energyHex) { conds.push(`split_part(body,' ',2) = $${params.length+1}`); params.push(energyHex); }

    const tc = buildTypeCondsForEnergy(energyHex, typeHex, params);
    if (tc.sql) conds.push(tc.sql);

    if (multiHex && MULTI_SUPPORTED(energyHex)) {
      conds.push(`split_part(body,' ',4) = $${params.length+1}`); params.push(multiHex);
    }
    if (onlyOk)    { conds.push(ERR_EQ_OK); }

    const qsql = `
      SELECT "time", body
      FROM public.log_rtureceivelog
      WHERE ${conds.join(' AND ')}
      ORDER BY "time" DESC
      LIMIT $${params.length+1}`;
    params.push(limit);

    const { rows } = await pool.query(qsql, params);

    const points = rows.map(r => {
      const p = parseFrame(r.body);
      const head = headerFromHex(r.body);
      const whBig = p?.metrics?.cumulativeWh ?? null;
      const whNum = (whBig!=null) ? Number(whBig) : null;
      return {
        ts: r.time,
        kw: p?.metrics?.currentOutputW!=null ? Math.round((p.metrics.currentOutputW/1000)*100)/100 : null,
        wh: whNum,
        wh_str: whBig!=null ? String(whBig) : null,
        energy: p?.energy ?? head.energy,
        type:   p?.type   ?? head.type,
        multi:  head.multi,
      };
    });
    res.json(points);
  } catch (e) { next(e); }
}

// 디버그 (원본+파싱상태) — /wind/debug?ok=any 지원
async function handleDebug(req, res, next, defaultEnergyHex = '') {
  try {
    const q = req.query.rtuImei || req.query.imei || req.query.name || req.query.q;
    const limit = Math.min(parseInt(req.query.limit || '5', 10), 50);
    if (!q) return res.status(400).json({ error: 'rtuImei/imei/name/q is required' });
    const imei = await resolveOneImeiOrThrow(q);

let energyHex = (req.query.energy || defaultEnergyHex).toLowerCase();

    const typeHex   = (req.query.type   || '').toLowerCase();
    const okParam   = String(req.query.ok || ''); // '', '1'|'true', 'any'
    let errCond = null;

    if (okParam === '1' || okParam === 'true') {
      errCond = ERR_EQ_OK;
    } else if (okParam === 'any') {
      errCond = ERR_EQ_OK_OR_02;
    }
    const multiHex  = (req.query.multi  || '').toLowerCase();

    const conds = ['"rtuImei" = $1', CMD_IS_14];
    const params = [imei];
    if (energyHex) { conds.push(`split_part(body,' ',2) = $${params.length+1}`); params.push(energyHex); }

    const tc = buildTypeCondsForEnergy(energyHex, typeHex, params);
    if (tc.sql) conds.push(tc.sql);

    if (multiHex && MULTI_SUPPORTED(energyHex)) {
      conds.push(`split_part(body,' ',4) = $${params.length+1}`); params.push(multiHex);
    }
    if (errCond) conds.push(errCond);

    const sql = `
      SELECT "time","bodyLength",body
      FROM public.log_rtureceivelog
      WHERE ${conds.join(' AND ')}
      ORDER BY "time" DESC
      LIMIT $${params.length+1}`;
    params.push(limit);

    const { rows } = await pool.query(sql, params);

    const out = rows.map(r => {
      const parts = (r.body || '').trim().split(/\s+/);
      const head = {
        cmd:    parts[0] ? parseInt(parts[0],16) : null,
        energy: parts[1] ? parseInt(parts[1],16) : null,
        type:   parts[2] ? parseInt(parts[2],16) : null,
        multi:  parts[3] ? parseInt(parts[3],16) : null,
        err:    parts[4] ? parseInt(parts[4],16) : null,
      };
      const p = parseFrame(r.body);
      return {
        ts: r.time,
        bodyLength: r.bodyLength ?? null,
        head,
        parsed: {
          ok: p?.ok ?? false,
          reason: p?.reason || null,
          energyName: p?.energyName || null,
          typeName: p?.typeName || null,
          metrics: p?.metrics ?? null,
        },
        raw: r.body
      };
    });
    res.json(jsonSafe(out));
  } catch (e) { next(e); }
}

// 최신 프레임 즉시 조회 (옵션: multi)
async function handleInstant(req, res, next, defaultEnergyHex = '01') {
  try {
    const q = req.query.rtuImei || req.query.imei || req.query.name || req.query.q;
    if (!q) { const e = new Error('rtuImei/imei/name/q 중 하나가 필요합니다.'); e.status = 400; throw e; }
    const imei = await resolveOneImeiOrThrow(q);

let energyHex = (req.query.energy || defaultEnergyHex).toLowerCase();

    const typeHex   = (req.query.type   || '').toLowerCase() || null;
    const multiHex  = (req.query.multi  || '').toLowerCase() || null;

    const useMulti = (multiHex && MULTI_SUPPORTED(energyHex)) ? multiHex : null;

    const row = await (useMulti
      ? lastBeforeNowByMulti(imei, { energyHex, typeHex, multiHex: useMulti })
      : lastBeforeNow(imei, energyHex, typeHex));
    if (!row)
      return res.status(422).json({ error: 'no_frame', message: '정상 프레임이 없습니다.' });

    const p = parseFrame(row.body);
    if (!p?.ok || !p?.metrics) {
      return res.status(422).json({
        error: 'parse_fail',
        message: p?.reason || '파싱 실패',
        raw: row.body,
      });
    }

    const m = p.metrics;
    const parts = (row.body || '').trim().split(/\s+/);
    const multiFromFrame = parts[3] || null;
    const isGeo = p.energy === 3;

    const producedByWh = (m.cumulativeWh!=null) ? Number(m.cumulativeWh)/1000 : null;

    const payload = {
      ts: row.time,
      energy: p.energy,
      type: p.type,
      multi: useMulti ?? (MULTI_SUPPORTED(parts[1]) ? multiFromFrame : null),

      // 공통(전기/열/풍력/연료전지/ESS)
      pv_voltage_v: m.pvVoltage ?? null,
      pv_current_a: m.pvCurrent ?? null,
      pv_power_w: (() => {
        if (m.pvPowerW != null) return m.pvPowerW;
        if (m.pvOutputW != null) return m.pvOutputW;
        if (m.pvVoltage != null && m.pvCurrent != null)
          return m.pvVoltage * m.pvCurrent;
        return null;
      })(),
      system_voltage_v: m.systemVoltage ?? m.voltageV ?? null,
      system_current_a: m.systemCurrent ?? m.currentA ?? null,
      power_factor: m.powerFactor ?? null,
      frequency_hz: m.frequencyHz ?? null,
      current_output_w:
        m.currentOutputW ??
        (isGeo ? m.outputW ?? m.inverterOutputW ?? null : null),
      cumulative_wh: m.cumulativeWh ?? null,

      // 삼상 보조
      system_r_voltage_v: m.systemR_V ?? null,
      system_s_voltage_v: m.systemS_V ?? null,
      system_t_voltage_v: m.systemT_V ?? null,
      system_r_current_a: m.systemR_I ?? null,
      system_s_current_a: m.systemS_I ?? null,
      system_t_current_a: m.systemT_I ?? null,

      // 열원/지열 보조
      inlet_temp_c: m.inletTempC ?? m.sourceInTempC ?? m.loadInTempC ?? null,
      outlet_temp_c: m.outletTempC ?? m.sourceOutTempC ?? m.loadOutTempC ?? null,
      load_in_temp_c:  m.loadInTempC  ?? null,
      load_out_temp_c: m.loadOutTempC ?? null,
      tank_top_temp_c: m.tankTopTempC ?? null,
      tank_bottom_temp_c: m.tankBottomTempC ?? null,
      cold_temp_c: m.coldTempC ?? m.tapFeedTempC ?? null,
      hot_temp_c: m.hotTempC ?? m.tapHotTempC ?? null,
      flow_lpm: m.flowLpm ?? m.loadFlowLpm ?? m.tapFlowLpm ?? null,
      consumed_flow_lpm: m.consumedFlowLpm ?? null,

      // 에너지량
      produced_kwh: m.producedKwh ?? producedByWh,
      used_kwh:
        m.usedKwh ?? m.usedElectricKwh ?? m.usedElecKwh ??
        m.loadUsedKwh ?? m.tapUsedKwh ?? null,

      // 상태/고장
      status_flags: m.statusFlags ?? null,
      status_list: m.statusList ?? null,
      fault_code: m.faultCode ?? m.faultFlags ?? null,
      fault_flags: m.faultFlags ?? null,
      fault_list: m.faultList ?? null,
      state: m.state ?? null,
      state_raw: m.stateRaw ?? null,
      state_text: geoStateTextFrom(m),
      is_operating: inferIsOperating(m),

      // 지열 전용
      heat_production_w: m.heatProductionW ?? m.heatW ?? null,
      inverter_output_w: m.inverterOutputW ?? m.outputW ?? null,
      load_flow_lpm: m.loadFlowLpm ?? null,
      tap_flow_lpm: m.tapFlowLpm ?? null,
    };

    // 풍력(energy=04) 보조 계측
    if (String(p.energy) === '4') {
      payload.pre_voltage_v  = m.preVoltageV  ?? null;
      payload.pre_current_a  = m.preCurrentA  ?? null;
      payload.pre_power_w    = m.prePowerW    ?? (
        (m.preVoltageV!=null && m.preCurrentA!=null) ? m.preVoltageV*m.preCurrentA : null
      );

      payload.post_voltage_v = m.postVoltageV ?? null;
      payload.post_current_a = m.postCurrentA ?? null;
      payload.post_output_w  = m.postOutputW  ?? (
        (m.postVoltageV!=null && m.postCurrentA!=null && m.powerFactor!=null)
          ? m.postVoltageV*m.postCurrentA*m.powerFactor : null
      );
    }

    res.json(jsonSafe(payload));
  } catch (e) { next(e); }
}

// 멀티(설비 슬롯)별 최신값 + 합계/평균
async function handleInstantMulti(req, res, next, defaultEnergyHex = '01') {
  try {
    const q = req.query.rtuImei || req.query.imei || req.query.name || req.query.q;
    if (!q) { const e = new Error('rtuImei/imei/name/q 중 하나가 필요합니다.'); e.status = 400; throw e; }
    const imei = await resolveOneImeiOrThrow(q);

let energyHex = (req.query.energy || defaultEnergyHex).toLowerCase();

    const typeHex   = (req.query.type   || '').toLowerCase() || null;

    const rows = await latestPerMulti(imei, { energyHex, typeHex });

    const units = rows.map(r => {
      const multi = r.multi_hex;
      const p = parseFrame(r.body);
      const m = p?.metrics || {};
      const pvPowerW = (m.pvPowerW ?? m.pvOutputW ??
        ((m.pvVoltage!=null && m.pvCurrent!=null) ? m.pvVoltage*m.pvCurrent : null));

      const producedByWh = (m.cumulativeWh!=null) ? Number(m.cumulativeWh)/1000 : null;

      const o = {
        multi,
        ts: r.time,
        pv_voltage_v: m.pvVoltage ?? null,
        pv_current_a: m.pvCurrent ?? null,
        pv_power_w:   pvPowerW,
        system_voltage_v: m.systemVoltage ?? m.voltageV ?? null,
        system_current_a: m.systemCurrent ?? m.currentA ?? null,
        power_factor:    m.powerFactor ?? null,
        frequency_hz:    m.frequencyHz ?? null,
        current_output_w: m.currentOutputW ?? m.outputW ?? m.inverterOutputW ?? null,
        cumulative_wh:    m.cumulativeWh ?? null,
        // 삼상 보조
        system_r_voltage_v: m.systemR_V ?? null,
        system_s_voltage_v: m.systemS_V ?? null,
        system_t_voltage_v: m.systemT_V ?? null,
        system_r_current_a: m.systemR_I ?? null,
        system_s_current_a: m.systemS_I ?? null,
        system_t_current_a: m.systemT_I ?? null,
        // 열원 보조
        inlet_temp_c: m.inletTempC ?? m.sourceInTempC ?? m.loadInTempC ?? null,
        outlet_temp_c: m.outletTempC ?? m.sourceOutTempC ?? m.loadOutTempC ?? null,
        load_in_temp_c:  m.loadInTempC  ?? null,
        load_out_temp_c: m.loadOutTempC ?? null,
        tank_top_temp_c: m.tankTopTempC ?? null,
        tank_bottom_temp_c: m.tankBottomTempC ?? null,
        cold_temp_c: m.coldTempC ?? m.tapFeedTempC ?? null,
        hot_temp_c: m.hotTempC ?? m.tapHotTempC ?? null,
        flow_lpm: m.flowLpm ?? m.loadFlowLpm ?? m.tapFlowLpm ?? null,
        consumed_flow_lpm: m.consumedFlowLpm ?? null,

        produced_kwh: m.producedKwh ?? producedByWh,
        used_kwh: m.usedKwh ?? m.usedElectricKwh ?? m.usedElecKwh ?? m.loadUsedKwh ?? m.tapUsedKwh ?? null,

        status_flags: m.statusFlags ?? null,
        status_list: m.statusList ?? null,
        fault_code: m.faultCode ?? m.faultFlags ?? null,
        fault_flags: m.faultFlags ?? null,
        fault_list: m.faultList ?? null,
        state: m.state ?? null,
        state_raw: m.stateRaw ?? null,
        state_text: geoStateTextFrom(m),
        is_operating: inferIsOperating(m),
      };

      if (String(p?.energy) === '4') {
        o.pre_voltage_v  = m.preVoltageV  ?? null;
        o.pre_current_a  = m.preCurrentA  ?? null;
        o.pre_power_w    = m.prePowerW    ?? (
          (m.preVoltageV!=null && m.preCurrentA!=null) ? m.preVoltageV*m.preCurrentA : null
        );
        o.post_voltage_v = m.postVoltageV ?? null;
        o.post_current_a = m.postCurrentA ?? null;
        o.post_output_w  = m.postOutputW  ?? (
          (m.postVoltageV!=null && m.postCurrentA!=null && m.powerFactor!=null)
            ? m.postVoltageV*m.postCurrentA*m.powerFactor : null
        );
      }
      return o;
    });

    const sum = (arr, k) => arr.reduce((s, x) => s + (Number(x[k]) || 0), 0);
    const avg = (arr, k) => {
      const vals = arr.map(x => Number(x[k])).filter(v => Number.isFinite(v));
      return vals.length ? Math.round((vals.reduce((a,b)=>a+b,0)/vals.length)*100)/100 : null;
    };

    const aggregate = {
      ts: units[0]?.ts || null,
      pv_power_w_sum:       sum(units, 'pv_power_w') || null,
      current_output_w_sum: sum(units, 'current_output_w') || null,
      pv_voltage_v_avg:     avg(units, 'pv_voltage_v'),
      pv_current_a_sum:     sum(units, 'pv_current_a') || null,
      power_factor_avg:     avg(units, 'power_factor'),
      frequency_hz_avg:     avg(units, 'frequency_hz'),
    };

    res.json(jsonSafe({
      deviceInfo: { rtuImei: imei, energy_hex: energyHex, type_hex: typeHex },
      units,
      aggregate,
    }));
  } catch (e) { next(e); }
}

// 시간대별 발전/생산량 (kWh) — 하루 단일 스캔(boundary, heartbeat 제외)
// ✅ ?multi=XX 지정 시 해당 멀티만, 미지정 시 전체 합산
async function handleHourly(req, res, next, defaultEnergyHex = '01') {
  try {
    const q = req.query.rtuImei || req.query.imei || req.query.name || req.query.q;
    if (!q) { const e = new Error('rtuImei/imei/name/q 중 하나가 필요합니다.'); e.status = 400; throw e; }
    const imei = await resolveOneImeiOrThrow(q);

let energyHex = (req.query.energy || defaultEnergyHex).toLowerCase();

    const typeHexRaw = (req.query.type || '').toLowerCase();
    const typeHex    = typeHexRaw || null;
    const multiHex   = (req.query.multi || '').toLowerCase();

    // 날짜 경계(KST)
    const dateStr = req.query.date; // YYYY-MM-DD
    const baseKST = dateStr
      ? DateTime.fromFormat(dateStr, 'yyyy-LL-dd', { zone: TZ })
      : DateTime.now().setZone(TZ);
    const startUtc = baseKST.startOf('day').toUTC().toJSDate();
    const endUtc   = baseKST.plus({ days: 1 }).startOf('day').toUTC().toJSDate();

    // 하루치 SELECT
    const params = [imei, startUtc, endUtc];
    const conds = [
      `"rtuImei" = $1`,
      `"time" >= $2`,
      `"time" <  $3`,
      CMD_IS_14,
      ERR_EQ_OK,
      LEN_WITH_WH_COND,
    ];

    if (energyHex) {
      params.push(energyHex);
      conds.push(`split_part(body,' ',2) = $${params.length}`);
    }
    const tc = buildTypeCondsForEnergy(energyHex, typeHex, params);
    if (tc.sql) conds.push(tc.sql);

    const useMulti = (multiHex && MULTI_SUPPORTED(energyHex)) ? multiHex : null;
    if (useMulti) {
      params.push(useMulti);
      conds.push(`split_part(body,' ',4) = $${params.length}`);
    }

    const sql = `
      SELECT "time", body
      FROM public.log_rtureceivelog
      WHERE ${conds.join(' AND ')}
      ORDER BY "time" ASC
    `;
    const { rows } = await pool.query(sql, params);

    const hourKey = (jsDate) => DateTime.fromJSDate(jsDate).setZone(TZ).toFormat('HH');

    const perHourMulti = new Map(); // key='HH|MM' → { firstWh, lastWh }
    for (const r of rows) {
      const p = pickMetrics(r.body);
      const wh = p?.wh ?? null;
      if (wh == null) continue;

      let m = '00';
      if (MULTI_SUPPORTED(energyHex)) {
        const parts = (r.body || '').trim().split(/\s+/);
        m = (parts[3] || '00').toLowerCase();
        if (useMulti && m !== useMulti) continue;
      }

      const hh = hourKey(new Date(r.time));
      const key = `${hh}|${m}`;
      const rec = perHourMulti.get(key) || { firstWh: null, lastWh: null };

      if (rec.firstWh == null) rec.firstWh = wh; // ASC
      rec.lastWh = wh;
      perHourMulti.set(key, rec);
    }

    const hours = Array.from({ length: 24 }, (_, i) => {
      const hh = String(i).padStart(2, '0');
      let sumWh = 0n; let have = false;

      for (const [key, rec] of perHourMulti.entries()) {
        if (!key.startsWith(hh + '|')) continue;
        if (rec.firstWh != null && rec.lastWh != null && rec.lastWh >= rec.firstWh) {
          sumWh += (rec.lastWh - rec.firstWh);
          have = true;
        }
      }
      const kwh = have ? Number(sumWh) / 1000 : 0;
      return { hour: hh, kwh };
    });

    return res.json({
      date: baseKST.toFormat('yyyy-LL-dd'),
      imei,
      energy: energyHex,
      type: typeHexRaw || null,
      multi: useMulti || (MULTI_SUPPORTED(energyHex) ? 'all' : null),
      mode: 'boundary-single-scan',
      hours
    });
  } catch (e) {
    next(e);
  }
}

/* ---------- 라우터 바인딩 ---------- */

// 전기(태양광 등, 기본 energy=01)
router.get('/electric',                limiterKPI,      (req,res,n)=>handleKPI(req,res,n,'01'));
router.get('/electric/preview',        limiterPreview,  (req,res,n)=>handlePreview(req,res,n,'01'));
router.get('/electric/debug',          limiterDebug,    (req,res,n)=>handleDebug(req,res,n,'01'));
router.get('/electric/instant',        limiterInstant,  (req,res,n)=>handleInstant(req,res,n,'01'));
router.get('/electric/instant/multi',  limiterInstantM, (req,res,n)=>handleInstantMulti(req,res,n,'01'));
router.get('/electric/hourly',         limiterHourly,   (req,res,n)=>handleHourly(req,res,n,'01'));

// 태양열(energy=02)
router.get('/thermal',                 limiterKPI,      (req,res,n)=>handleKPI(req,res,n,'02'));
router.get('/thermal/preview',         limiterPreview,  (req,res,n)=>handlePreview(req,res,n,'02'));
router.get('/thermal/debug',           limiterDebug,    (req,res,n)=>handleDebug(req,res,n,'02'));
router.get('/thermal/instant',         limiterInstant,  (req,res,n)=>handleInstant(req,res,n,'02'));
router.get('/thermal/instant/multi',   limiterInstantM, (req,res,n)=>handleInstantMulti(req,res,n,'02'));
router.get('/thermal/hourly',          limiterHourly,   (req,res,n)=>handleHourly(req,res,n,'02'));

// 지열(energy=03)
router.get('/geothermal',              limiterKPI,      (req,res,n)=>handleKPI(req,res,n,'03'));
router.get('/geothermal/preview',      limiterPreview,  (req,res,n)=>handlePreview(req,res,n,'03'));
router.get('/geothermal/debug',        limiterDebug,    (req,res,n)=>handleDebug(req,res,n,'03'));
router.get('/geothermal/instant',      limiterInstant,  (req,res,n)=>handleInstant(req,res,n,'03'));
router.get('/geothermal/instant/multi',limiterInstantM, (req,res,n)=>handleInstantMulti(req,res,n,'03'));
router.get('/geothermal/hourly',       limiterHourly,   (req,res,n)=>handleHourly(req,res,n,'03'));

// 풍력(energy=04)
router.get('/wind',                    limiterKPI,      (req,res,n)=>handleKPI(req,res,n,'04'));
router.get('/wind/preview',            limiterPreview,  (req,res,n)=>handlePreview(req,res,n,'04'));
router.get('/wind/debug',              limiterDebug,    (req,res,n)=>handleDebug(req,res,n,'04'));  // ok=any 지원
router.get('/wind/instant',            limiterInstant,  (req,res,n)=>handleInstant(req,res,n,'04')); // pre/post 노출
router.get('/wind/instant/multi',      limiterInstantM, (req,res,n)=>handleInstantMulti(req,res,n,'04'));
router.get('/wind/hourly',             limiterHourly,   (req,res,n)=>handleHourly(req,res,n,'04'));

// 연료전지(energy=06)
router.get('/fuelcell',                limiterKPI,      (req,res,n)=>handleKPI(req,res,n,'06'));
router.get('/fuelcell/preview',        limiterPreview,  (req,res,n)=>handlePreview(req,res,n,'06'));
router.get('/fuelcell/debug',          limiterDebug,    (req,res,n)=>handleDebug(req,res,n,'06'));
router.get('/fuelcell/instant',        limiterInstant,  (req,res,n)=>handleInstant(req,res,n,'06'));
router.get('/fuelcell/instant/multi',  limiterInstantM, (req,res,n)=>handleInstantMulti(req,res,n,'06'));
router.get('/fuelcell/hourly',         limiterHourly,   (req,res,n)=>handleHourly(req,res,n,'06'));

// ESS(energy=07)
router.get('/ess',                     limiterKPI,      (req,res,n)=>handleKPI(req,res,n,'07'));
router.get('/ess/preview',             limiterPreview,  (req,res,n)=>handlePreview(req,res,n,'07'));
router.get('/ess/debug',               limiterDebug,    (req,res,n)=>handleDebug(req,res,n,'07'));
router.get('/ess/instant',             limiterInstant,  (req,res,n)=>handleInstant(req,res,n,'07'));
router.get('/ess/instant/multi',       limiterInstantM, (req,res,n)=>handleInstantMulti(req,res,n,'07'));
router.get('/ess/hourly',              limiterHourly,   (req,res,n)=>handleHourly(req,res,n,'07'));

module.exports = router;
