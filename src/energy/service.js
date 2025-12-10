// src/energy/service.js
const express = require('express');
const router = express.Router();
const rateLimit = require('express-rate-limit');
const { pool } = require('../db/db.pg');
const { parseFrame } = require('./parser');
const { TZ } = require('./timeutil');
const { resolveOneImeiOrThrow } = require('./devices');
const { DateTime } = require('luxon');
const axios = require('axios').create({
  timeout: 30000,
  validateStatus: () => true,
});

const ELECTRIC_CO2 = Number(process.env.ELECTRIC_CO2_PER_KWH || '0.4747');
const THERMAL_CO2  = Number(process.env.THERMAL_CO2_PER_KWH  || '0.198');
const TREE_CO2_KG  = 6.6;

const RECENT_WINDOW_BY_ENERGY = {
  '01': 60,
  '02': 60,
  '03': 60,
  '04': 60,
  '06': 60,
  '07': 60,
};

function recentSqlFor(energyHex) {
  const days = RECENT_WINDOW_BY_ENERGY[energyHex] || 14;
  return `"time" >= (now() - interval '${days} days')`;
}

const CO2_FOR = (energyHex) => {
  const e = (energyHex || '').toLowerCase();
  if (e === '02' || e === '03') return THERMAL_CO2;
  return ELECTRIC_CO2;
};

const MULTI_SUPPORTED = (energyHex='') => (energyHex || '').toLowerCase() === '01';

const makeLimiter = (maxPerMin) =>
  rateLimit({
    windowMs: 60 * 1000,
    max: maxPerMin,
    message: { error: 'Too many requests — try again later.' },
    standardHeaders: true, 
    legacyHeaders: false,
  });

const limiterKPI       = makeLimiter(50);
const limiterPreview   = makeLimiter(50);
const limiterDebug     = makeLimiter(50);
const limiterInstant   = makeLimiter(50);
const limiterInstantM  = makeLimiter(50);
const limiterHourly    = makeLimiter(300);

const jsonSafe = (obj) =>
  JSON.parse(JSON.stringify(obj, (_, v) => (typeof v === 'bigint' ? v.toString() : v)));

const ERR_EQ_OK        = `split_part(body,' ',5) = '00'`;

const ERR_EQ_OK_OR_02  = `(split_part(body,' ',5) = '00' OR split_part(body,' ',5) = '02' OR split_part(body,' ',5) = '39')`;

const MIN_BODYLEN_WITH_WH = 12;
const LEN_WITH_WH_COND    = `COALESCE("bodyLength", 9999) >= ${MIN_BODYLEN_WITH_WH}`;

const CMD_IS_14 = `left(body,2)='14'`;

const mapByMulti = (rows) => {
  const m = new Map();
  for (const r of rows || []) {
    if (r && r.multi_hex) m.set(r.multi_hex, r);
  }
  return m;
};

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

async function lastBeforeNow(imei, energyHex = null, typeHex = null) {
  const params = [imei];
  const conds = [
    `"rtuImei" = $1`,
    recentSqlFor(energyHex),
    CMD_IS_14,
    ERR_EQ_OK_OR_02,
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

async function lastBeforeNowByMulti(imei, { energyHex=null, typeHex=null, multiHex=null } = {}) {
  const params = [imei];
  const conds = [
    `"rtuImei" = $1`,
    recentSqlFor(energyHex),
    CMD_IS_14,
    ERR_EQ_OK_OR_02,
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

async function latestPerMulti(imei, { energyHex=null, typeHex=null } = {}) {
  const params = [imei];
  
  // 조건절 구성
  const conds = [
    `"rtuImei" = $1`,
    // recentSqlFor(60일) 조건은 LIMIT가 있으므로 성능에 영향을 주지 않지만 안전장치로 유지
    recentSqlFor(energyHex), 
    `split_part(body, ' ', 5) = '00'`, // 정상 데이터만 (ERR_EQ_OK)
    `left(body, 2) = '14'`,            // CMD_IS_14
    `COALESCE("bodyLength", 9999) >= 12` // LEN_WITH_WH_COND
  ];

  if (energyHex) { 
    params.push(energyHex); 
    conds.push(`split_part(body,' ',2) = $${params.length}`); 
  }
  
  // Type 필터 (buildTypeCondsForEnergy 로직 통합)
  const e = (energyHex || '').toLowerCase();
  const t = (typeHex || '').toLowerCase();
  
  if (e === '04' && (!t || t === 'auto')) {
     conds.push(`split_part(body,' ',3) IN ('00','01')`);
  } else if (t) {
     params.push(t);
     conds.push(`split_part(body,' ',3) = $${params.length}`);
  }

  // [최적화 핵심]
  // 복잡한 윈도우 함수(ROW_NUMBER) 대신, 단순히 최신순으로 넉넉하게(LIMIT 300) 가져옵니다.
  // 인덱스("rtuImei", "time" DESC)가 걸려 있으므로 이 쿼리는 0.01초도 안 걸립니다.
  const sql = `
    SELECT "time", body, split_part(body, ' ', 4) AS multi_hex
    FROM public.log_rtureceivelog
    WHERE ${conds.join(' AND ')}
    ORDER BY "time" DESC
    LIMIT 300
  `;

  const { rows } = await pool.query(sql, params);

  // [Node.js 후처리]
  // 가져온 최신 300개 중에서 각 멀티(multi_hex)별로 '가장 먼저 나온 것(최신)' 하나씩만 남깁니다.
  const latestMap = new Map();
  for (const r of rows) {
    const m = r.multi_hex || '00';
    if (!latestMap.has(m)) {
      latestMap.set(m, r);
    }
  }

  return Array.from(latestMap.values());
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

function headerFromHex(hex) {
  const parts = (hex || '').trim().split(/\s+/);
  return {
    energy: parts[1] ? parseInt(parts[1], 16) : null,
    type:   parts[2] ? parseInt(parts[2], 16) : null,
    multi:  parts[3] || null,
    energyHex: (parts[1] || '').toLowerCase(),
  };
}

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

function pickWhOnly (hex) {
  const p = parseFrame(hex);
  if (!p || !p.ok || !p.metrics) {
    return { wh: null };
  }
  const m = p.metrics;
  const wh = Object.prototype.hasOwnProperty.call(m, 'cumulativeWh')
    ? m.cumulativeWh
    : null;
  return { wh };
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

    const { data } = await axios.get(url.toString(), { timeout: 2500 });
    return data?.detail_hourly || null;
  } catch (e) {
    return null;
  }
}

async function handleKPI (req, res, next, defaultEnergyHex = '01') {
  try {
    const q = req.query.rtuImei || req.query.imei || req.query.name || req.query.q;
    if (!q) {
      const e = new Error('rtuImei/imei/name/q 중 하나가 필요합니다.');
      e.status = 400;
      throw e;
    }
    const imei = await resolveOneImeiOrThrow(q);

    let energyHex = (req.query.energy || defaultEnergyHex).toLowerCase();
    const typeHex   = (req.query.type   || '').toLowerCase() || null;
    const multiHexQ = (req.query.multi  || '').toLowerCase() || null;
    const selectedMulti =
      typeof multiHexQ === 'string' && /^[0-9a-f]{2}$/.test(multiHexQ) ? multiHexQ : null;

    const co2Factor = CO2_FOR(energyHex);

    const seriesPromise = fetchSeriesHourlyNonBlocking({
      imei, energyHex, typeHex, multiHex: selectedMulti
    });

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

    const [
      latestRows,
      todayFirstRows,
      prevMonthFirstRows,
      thisMonthFirstRows
    ] = await Promise.all([
      latestPerMulti(imei, { energyHex, typeHex }),
      firstAfterPerMulti(imei, start_utc,      { energyHex, typeHex }),
      firstAfterPerMulti(imei, prev_month_utc, { energyHex, typeHex }),
      firstAfterPerMulti(imei, this_month_utc, { energyHex, typeHex }),
    ]);

    const anyLatest = latestRows.some(r => r?.body);
    if (!anyLatest) {
      return res.status(422).json({
        error: 'no_frames_for_energy',
        message: `해당 IMEI에서 energy=0x${energyHex}${typeHex ? ` type=0x${typeHex}` : ''} 정상(0x00) 프레임이 없습니다.`
      });
    }

    let totalWhSum = 0n;
    let haveWh = false;
    let nowWSum = 0;
    const effList = [];
    const latestMetricsByMulti = new Map();

    for (const r of latestRows) {
      if (!r?.body) continue;

      const p = pickMetrics(r.body);
      if (!p) continue;

      if (p.wh != null) {
        totalWhSum += p.wh;
        haveWh = true;
      }
      if (p.w != null) {
        nowWSum += p.w;
      }
      if (typeof p.eff === 'number') {
        effList.push(p.eff);
      }

      const parts = (r.body || '').trim().split(/\s+/);
      const multiKey =
        (r.multi_hex || parts[3] || '00').toString().toLowerCase();

      latestMetricsByMulti.set(multiKey, {
        wh: p.wh,
        w: p.w,
        eff: p.eff
      });
    }

    const total_kwh = haveWh ? Number(totalWhSum) / 1000 : null;
    const total_mwh = haveWh ? Number(totalWhSum) / 1_000_000 : null;
    const now_kw    = nowWSum ? Math.round((nowWSum / 1000) * 100) / 100 : null;
    const inverter_efficiency_pct = effList.length
      ? Math.round((effList.reduce((a, b) => a + b, 0) / effList.length) * 10) / 10
      : null;

    const todayFirstMap = mapByMulti(todayFirstRows);
    let todayWhSum = 0n;
    let haveToday = false;

    for (const [multi, Lmetrics] of latestMetricsByMulti.entries()) {
      if (selectedMulti && multi !== selectedMulti) continue;

      const Frow = todayFirstMap.get(multi);
      if (!Frow?.body) continue;

      const Lwh = Lmetrics.wh;
      const FwhObj = pickWhOnly(Frow.body);
      const Fwh = FwhObj.wh;

      if (Lwh != null && Fwh != null && Lwh >= Fwh) {
        todayWhSum += (Lwh - Fwh);
        haveToday = true;
      }
    }
    const today_kwh = haveToday ? Math.max(0, Number(todayWhSum) / 1000) : null;

    const thisMonthMap = mapByMulti(thisMonthFirstRows);
    const prevMonthMap = mapByMulti(prevMonthFirstRows);
    let monthDiffWhSum = 0n;
    let haveMonth = false;

    for (const [multi, Arow] of thisMonthMap.entries()) {
      const Brow = prevMonthMap.get(multi);
      if (!Arow?.body || !Brow?.body) continue;

      const AwhObj = pickWhOnly(Arow.body);
      const BwhObj = pickWhOnly(Brow.body);
      const Awh = AwhObj.wh;
      const Bwh = BwhObj.wh;

      if (Awh != null && Bwh != null && Awh >= Bwh) {
        monthDiffWhSum += (Awh - Bwh);
        haveMonth = true;
      }
    }

    let last_month_avg_kw = null;
    if (haveMonth) {
      const hours =
        (new Date(this_month_utc) - new Date(prev_month_utc)) / 3600_000;
      if (hours > 0) {
        last_month_avg_kw = Math.round(
          ((Number(monthDiffWhSum) / 1000) / hours) * 100
        ) / 100;
      }
    }

    const co2_kg  = total_kwh != null ? Math.round(total_kwh * co2Factor * 100) / 100 : null;
    const co2_ton = co2_kg   != null ? Math.round((co2_kg / 1000) * 100) / 100 : null;
    const trees   = co2_kg   != null ? Math.floor(co2_kg / TREE_CO2_KG) : null;

    const latestAt = latestRows
      .map(r => (r?.time ? new Date(r.time).getTime() : 0))
      .reduce((a, b) => Math.max(a, b), 0);
    const latestAtIso = latestAt ? new Date(latestAt).toISOString() : null;

    const detail_hourly = await seriesPromise;

    res.json({
      deviceInfo: { rtuImei: imei, latestAt: latestAtIso },
      kpis: {
        now_kw,
        today_kwh,
        total_kwh: total_kwh != null ? Math.round(total_kwh * 100) / 100 : null,
        total_mwh: total_mwh != null ? Math.round(total_mwh * 1000) / 1000 : null,
        co2_kg,
        co2_ton,
        trees,
        last_month_avg_kw,
        inverter_efficiency_pct,
      },
      detail_hourly,
      meta: {
        table: 'public.log_rtureceivelog',
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

async function handleDebug(req, res, next, defaultEnergyHex = '') {
  try {
    const q = req.query.rtuImei || req.query.imei || req.query.name || req.query.q;
    const limit = Math.min(parseInt(req.query.limit || '5', 10), 50);
    if (!q) return res.status(400).json({ error: 'rtuImei/imei/name/q is required' });
    const imei = await resolveOneImeiOrThrow(q);

let energyHex = (req.query.energy || defaultEnergyHex).toLowerCase();

    const typeHex   = (req.query.type   || '').toLowerCase();
    const okParam   = String(req.query.ok || '');
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

async function handleInstant(req, res, next, defaultEnergyHex = '01') {
  let imei = null;

  try {
    const q = req.query.rtuImei || req.query.imei || req.query.name || req.query.q;
    if (!q) {
      return res.status(400).json({
        ok: false,
        code: "INVALID_REQUEST",
        message: "rtuImei / imei / name / q 중 하나가 필요합니다."
      });
    }

    imei = await resolveOneImeiOrThrow(q);

    let energyHex = (req.query.energy || defaultEnergyHex).toLowerCase();
    const typeHex = (req.query.type || '').toLowerCase() || null;
    const multiHex = (req.query.multi || '').toLowerCase() || null;

    const useMulti = (multiHex && MULTI_SUPPORTED(energyHex)) ? multiHex : null;

    const row = await (useMulti
      ? lastBeforeNowByMulti(imei, { energyHex, typeHex, multiHex: useMulti })
      : lastBeforeNow(imei, energyHex, typeHex));

    if (!row) {
      return res.status(422).json({
        ok: false,
        code: "NO_DATA",
        message: "최근 정상 프레임이 없습니다."
      });
    }
    const p = parseFrame(row.body);

    if (!p?.ok || !p?.metrics) {
      return res.status(422).json({
        ok: false,
        code: "PARSE_FAIL",
        message: p?.reason || "프레임 파싱 실패",
        raw: row.body,
      });
    }

    const m = p.metrics;
    const parts = (row.body || '').trim().split(/\s+/);
    const multiFromFrame = parts[3] || null;
    const isGeo = p.energy === 3;

    const producedByWh = 
      (m.cumulativeWh != null) ? Number(m.cumulativeWh) / 1000 : null;

    const payload = {
      ts: row.time,
      energy: p.energy,
      type: p.type,
      multi: useMulti ?? multiFromFrame,

      pv_voltage_v: m.pvVoltage ?? null,
      pv_current_a: m.pvCurrent ?? null,
      pv_power_w:
        m.pvPowerW ??
        m.pvOutputW ??
        ((m.pvVoltage != null && m.pvCurrent != null) ? m.pvVoltage * m.pvCurrent : null),

      system_voltage_v: m.systemVoltage ?? m.voltageV ?? null,
      system_current_a: m.systemCurrent ?? m.currentA ?? null,
      current_output_w:
        m.currentOutputW ??
        (isGeo ? m.outputW ?? m.inverterOutputW ?? null : null),

      power_factor: m.powerFactor ?? null,
      frequency_hz: m.frequencyHz ?? null,
      cumulative_wh: m.cumulativeWh ?? null,

      // 삼상
      system_r_voltage_v: m.systemR_V ?? null,
      system_s_voltage_v: m.systemS_V ?? null,
      system_t_voltage_v: m.systemT_V ?? null,
      system_r_current_a: m.systemR_I ?? null,
      system_s_current_a: m.systemS_I ?? null,
      system_t_current_a: m.systemT_I ?? null,

      // 열 / 지열
      inlet_temp_c: m.inletTempC ?? m.sourceInTempC ?? m.loadInTempC ?? null,
      outlet_temp_c: m.outletTempC ?? m.sourceOutTempC ?? m.loadOutTempC ?? null,
      load_in_temp_c: m.loadInTempC ?? null,
      load_out_temp_c: m.loadOutTempC ?? null,
      tank_top_temp_c: m.tankTopTempC ?? null,
      tank_bottom_temp_c: m.tankBottomTempC ?? null,
      cold_temp_c: m.coldTempC ?? m.tapFeedTempC ?? null,
      hot_temp_c: m.hotTempC ?? m.tapHotTempC ?? null,
      flow_lpm: m.flowLpm ?? m.loadFlowLpm ?? m.tapFlowLpm ?? null,
      consumed_flow_lpm: m.consumedFlowLpm ?? null,

      produced_kwh: m.producedKwh ?? producedByWh,
      used_kwh:
        m.usedKwh ??
        m.usedElectricKwh ??
        m.usedElecKwh ??
        m.loadUsedKwh ??
        m.tapUsedKwh ??
        null,

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

    // 풍력 필드 보조
    if (String(p.energy) === '4') {
      payload.pre_voltage_v = m.preVoltageV ?? null;
      payload.pre_current_a = m.preCurrentA ?? null;
      payload.pre_power_w =
        m.prePowerW ??
        ((m.preVoltageV != null && m.preCurrentA != null) 
          ? m.preVoltageV * m.preCurrentA
          : null);

      payload.post_voltage_v = m.postVoltageV ?? null;
      payload.post_current_a = m.postCurrentA ?? null;
      payload.post_output_w =
        m.postOutputW ??
        ((m.postVoltageV != null && m.postCurrentA != null && m.powerFactor != null)
          ? m.postVoltageV * m.postCurrentA * m.powerFactor
          : null);
    }

    return res.json(jsonSafe(payload));

  } catch (err) {
    try {
      if (imei) {
        const fb = await lastBeforeNow(imei, defaultEnergyHex, null);
        if (fb?.body) {
          const p = parseFrame(fb.body);
          if (p?.ok && p?.metrics) {
            return res.json(jsonSafe({
              ok: true,
              fallback: true,
              ts: fb.time,
              ...p,
            }));
          }
        }
      }
    } catch {}

    next(err);
  }
}

async function handleInstantMulti(req, res, next, defaultEnergyHex = '01') {
  try {
    const q = req.query.rtuImei || req.query.imei || req.query.name || req.query.q;
    if (!q) {
      const e = new Error('rtuImei/imei/name/q 중 하나가 필요합니다.');
      e.status = 400;
      throw e;
    }

    const imei = await resolveOneImeiOrThrow(q);

    let energyHex = (req.query.energy || defaultEnergyHex).toLowerCase();
    const typeHex = (req.query.type || '').toLowerCase() || null;

    const rows = await latestPerMulti(imei, { energyHex, typeHex });

    const units = rows.map(r => {
      const body = r.body;

      const parts = body.split(/\s+/);
      const multi = parts[3] || r.multi_hex;

      const p = parseFrame(body);
      const m = p?.metrics || {};

      let pvPowerW = null;
      if (m.pvPowerW != null) pvPowerW = m.pvPowerW;
      else if (m.pvOutputW != null) pvPowerW = m.pvOutputW;
      else if (m.pvVoltage != null && m.pvCurrent != null)
        pvPowerW = m.pvVoltage * m.pvCurrent;

      let producedKwh = null;
      if (m.cumulativeWh != null) producedKwh = Number(m.cumulativeWh) / 1000;
      if (m.producedKwh != null) producedKwh = m.producedKwh;

      const o = {
        multi,
        ts: r.time,

        pv_voltage_v: m.pvVoltage ?? null,
        pv_current_a: m.pvCurrent ?? null,
        pv_power_w: pvPowerW,

        system_voltage_v: m.systemVoltage ?? m.voltageV ?? null,
        system_current_a: m.systemCurrent ?? m.currentA ?? null,
        power_factor: m.powerFactor ?? null,
        frequency_hz: m.frequencyHz ?? null,

        current_output_w:
          m.currentOutputW ??
          m.outputW ??
          m.inverterOutputW ??
          null,

        cumulative_wh: m.cumulativeWh ?? null,

        system_r_voltage_v: m.systemR_V ?? null,
        system_s_voltage_v: m.systemS_V ?? null,
        system_t_voltage_v: m.systemT_V ?? null,
        system_r_current_a: m.systemR_I ?? null,
        system_s_current_a: m.systemS_I ?? null,
        system_t_current_a: m.systemT_I ?? null,

        inlet_temp_c: m.inletTempC ?? m.sourceInTempC ?? m.loadInTempC ?? null,
        outlet_temp_c: m.outletTempC ?? m.sourceOutTempC ?? m.loadOutTempC ?? null,
        load_in_temp_c: m.loadInTempC ?? null,
        load_out_temp_c: m.loadOutTempC ?? null,
        tank_top_temp_c: m.tankTopTempC ?? null,
        tank_bottom_temp_c: m.tankBottomTempC ?? null,
        cold_temp_c: m.coldTempC ?? m.tapFeedTempC ?? null,
        hot_temp_c: m.hotTempC ?? m.tapHotTempC ?? null,
        flow_lpm: m.flowLpm ?? m.loadFlowLpm ?? m.tapFlowLpm ?? null,
        consumed_flow_lpm: m.consumedFlowLpm ?? null,

        produced_kwh: producedKwh,
        used_kwh:
          m.usedKwh ??
          m.usedElectricKwh ??
          m.usedElecKwh ??
          m.loadUsedKwh ??
          m.tapUsedKwh ??
          null,

        status_flags: m.statusFlags ?? null,
        status_list: m.statusList ?? null,
        fault_code: m.faultCode ?? m.faultFlags ?? null,
        fault_flags: m.faultFlags ?? null,
        fault_list: m.faultList ?? null,
        state: m.state ?? null,
        state_raw: m.stateRaw ?? null,
        state_text: geoStateTextFrom(m),
        is_operating: inferIsOperating(m)
      };

      if (p.energy === 4) {
        o.pre_voltage_v = m.preVoltageV ?? null;
        o.pre_current_a = m.preCurrentA ?? null;
        o.pre_power_w =
          m.prePowerW ??
          ((m.preVoltageV != null && m.preCurrentA != null)
            ? m.preVoltageV * m.preCurrentA
            : null);

        o.post_voltage_v = m.postVoltageV ?? null;
        o.post_current_a = m.postCurrentA ?? null;
        o.post_output_w =
          m.postOutputW ??
          ((m.postVoltageV != null &&
            m.postCurrentA != null &&
            m.powerFactor != null)
            ? m.postVoltageV * m.postCurrentA * m.powerFactor
            : null);
      }

      return o;
    });

    const sum = (arr, k) =>
      arr.reduce((s, x) => s + (Number(x[k]) || 0), 0);
    const avg = (arr, k) => {
      const vals = arr
        .map(x => Number(x[k]))
        .filter(v => Number.isFinite(v));
      return vals.length
        ? Math.round((vals.reduce((a, b) => a + b, 0) / vals.length) * 100) /
          100
        : null;
    };

    const aggregate = {
      ts: units[0]?.ts || null,
      pv_power_w_sum: sum(units, 'pv_power_w') || null,
      current_output_w_sum: sum(units, 'current_output_w') || null,
      pv_voltage_v_avg: avg(units, 'pv_voltage_v'),
      pv_current_a_sum: sum(units, 'pv_current_a') || null,
      power_factor_avg: avg(units, 'power_factor'),
      frequency_hz_avg: avg(units, 'frequency_hz')
    };

    res.json(
      jsonSafe({
        deviceInfo: { rtuImei: imei, energy_hex: energyHex, type_hex: typeHex },
        units,
        aggregate
      })
    );
  } catch (e) {
    next(e);
  }
}

// src/energy/service.js - handleHourly 함수

async function handleHourly(req, res, next, defaultEnergyHex = '01') {
  try {
    const q = req.query.rtuImei || req.query.imei || req.query.name || req.query.q;
    if (!q) { const e = new Error('rtuImei/imei/name/q 중 하나가 필요합니다.'); e.status = 400; throw e; }
    const imei = await resolveOneImeiOrThrow(q);

    let energyHex = (req.query.energy || defaultEnergyHex).toLowerCase();

    const typeHexRaw = (req.query.type || '').toLowerCase();
    const typeHex    = typeHexRaw || null;
    const multiHex   = (req.query.multi || '').toLowerCase();

    const dateStr = req.query.date;
    const baseKST = dateStr
      ? DateTime.fromFormat(dateStr, 'yyyy-LL-dd', { zone: TZ })
      : DateTime.now().setZone(TZ);
    const startUtc = baseKST.startOf('day').toUTC().toJSDate();
    const endUtc   = baseKST.plus({ days: 1 }).startOf('day').toUTC().toJSDate();

    // 1. 파라미터 인덱스 수동 관리
    const params = [imei, startUtc, endUtc];
    const conds = [
      `"rtuImei" = $1`,
      `"time" >= $2`,
      `"time" <  $3`,
      CMD_IS_14,
      ERR_EQ_OK,
      LEN_WITH_WH_COND,
    ];
    
    let nextIndex = params.length + 1;

    // 2. energyHex 조건
    if (energyHex) {
      params.push(energyHex);
      conds.push(`split_part(body,' ',2) = $${nextIndex}`);
      nextIndex++;
    }
    
    // 3. typeHex 조건
    const e = energyHex.toLowerCase();
    const t = typeHexRaw.toLowerCase();
    
    if (e === '04' && (!t || t === 'auto')) {
        conds.push(`split_part(body,' ',3) IN ('00','01')`);
    } else if (t) {
        params.push(t);
        conds.push(`split_part(body,' ',3) = $${nextIndex}`);
        nextIndex++;
    }
    
    // 4. multiHex 조건
    const useMulti = (multiHex && MULTI_SUPPORTED(energyHex)) ? multiHex : null;
    if (useMulti) {
      params.push(useMulti);
      conds.push(`split_part(body,' ',4) = $${nextIndex}`);
    }

    const sql = `
      SELECT "time", body
      FROM public.log_rtureceivelog
      WHERE ${conds.join(' AND ')}
      ORDER BY "time" ASC
    `;
    const { rows } = await pool.query(sql, params);

    const hourKey = (jsDate) => DateTime.fromJSDate(jsDate).setZone(TZ).toFormat('HH');

    const perHourMulti = new Map();
    for (const r of rows) {
      const p = pickMetrics(r.body);
      const wh = p?.wh ?? null;
      
      // 0값 무시 (튀는 데이터 방지)
      if (wh == null || Number(wh) === 0) continue;

      // 🚨 [핵심 수정] Type 정보 가져오기 (지열은 Type 01, 02가 섞여 있음)
      // Type을 구분하지 않으면 서로 다른 계측기의 누적값 차이가 계산됨
      const type = p.type || '00';

      let m = '00';
      if (MULTI_SUPPORTED(energyHex)) {
        const parts = (r.body || '').trim().split(/\s+/);
        m = (parts[3] || '00').toLowerCase();
        if (useMulti && m !== useMulti) continue;
      }

      const hh = hourKey(new Date(r.time));
      
      // 🚨 [핵심 수정] Key에 Type을 포함시켜서 따로 집계함
      // 기존: `${hh}|${m}` -> 수정: `${hh}|${type}|${m}`
      const key = `${hh}|${type}|${m}`;
      
      const rec = perHourMulti.get(key) || { firstWh: null, lastWh: null };

      if (rec.firstWh == null) rec.firstWh = wh; 
      rec.lastWh = wh;                           
      perHourMulti.set(key, rec);
    }

    const hours = Array.from({ length: 24 }, (_, i) => {
      const hh = String(i).padStart(2, '0');
      let sumWh = 0n; let have = false;

      // Type별로 계산된 델타값을 모두 합산 (Type 01 사용량 + Type 02 사용량)
      for (const [key, rec] of perHourMulti.entries()) {
        if (!key.startsWith(hh + '|')) continue;
        
        if (!rec.firstWh || rec.firstWh === 0n) continue;
        if (!rec.lastWh  || rec.lastWh === 0n) continue;

        if (rec.lastWh >= rec.firstWh) {
          sumWh += (rec.lastWh - rec.firstWh);
          have = true;
        }
      }
      
      // 파서가 이미 Wh 단위로 변환했으므로 1000으로 나누면 kWh
      const kwh = have ? Number(sumWh) / 1000.0 : 0;
      
      return { hour: hh, kwh: Math.round(kwh * 100) / 100 };
    });

    return res.json({
      date: baseKST.toFormat('yyyy-LL-dd'),
      imei,
      energy: energyHex,
      type: typeHexRaw || null,
      multi: useMulti || (MULTI_SUPPORTED(energyHex) ? 'all' : null),
      mode: 'boundary-scan-type-separated',
      hours
    });
  } catch (e) {
    next(e);
  }
}

async function lastBeforeStartOfDay(imei, energyHex, typeHex, multiHex, tsDate = null) {
  const startUtc = tsDate || DateTime.now().toUTC().toJSDate();
const sql = `
    SELECT "time", body
    FROM public.log_rtureceivelog
    WHERE "rtuImei" = $1
      AND "time" < $2
      AND split_part(body, ' ', 5) = '00'        -- ◀ [필수 추가] 정상 데이터만
      AND left(body, 2) = '14'                   -- ◀ [필수 추가] 프로토콜 확인
      AND COALESCE("bodyLength", 9999) >= 12     -- ◀ [필수 추가] 길이 확인
    ORDER BY "time" DESC
    LIMIT 10
  `;

  const { rows } = await pool.query(sql, [imei, startUtc]);

  const targetEnergy = (energyHex || '').toLowerCase();
  const targetType   = (typeHex || '').toLowerCase();
  const targetMulti  = (multiHex || '').toLowerCase();
  
  for (const r of rows) {
    const p = parseFrame(r.body);
    if (!p?.ok || !p.metrics) continue;

    if (targetEnergy && p.energy !== parseInt(targetEnergy, 16)) continue;
    if (targetType && p.type !== parseInt(targetType, 16)) continue;

    if (MULTI_SUPPORTED(energyHex)) {
       const parts = (r.body || '').trim().split(/\s+/);
       const mHex = (parts[3] || '00').toLowerCase();
       if (!targetMulti && mHex !== '00') continue;
       if (targetMulti && mHex !== targetMulti) continue;
    }
    return r;
  }

  return null;
}

async function firstAfterTime(imei, tsDate, energyHex, typeHex, targetMulti) {
  const sql = `
    SELECT body FROM public.log_rtureceivelog
    WHERE "rtuImei" = $1 AND "time" >= $2
    ORDER BY "time" ASC
    LIMIT 500  -- [핵심 수정] 10 -> 500 (밤새 쌓인 에러 로그를 넘기기 위함)
  `;
  
  const { rows } = await pool.query(sql, [imei, tsDate]);
  const targetEnergy = parseInt(energyHex || '01', 16);
  const targetType = typeHex ? parseInt(typeHex, 16) : null;

  for (const r of rows) {
    const p = parseFrame(r.body);

    if (!p?.metrics?.cumulativeWh) continue;

    if (p.energy !== targetEnergy) continue;
    if (targetType && p.type !== targetType) continue;

    const parts = (r.body || '').split(/\s+/);
    const m = (parts[3] || '00').toLowerCase();
    if (targetMulti && m !== targetMulti) continue;

    return Number(p.metrics.cumulativeWh);
  }
  return null;
}

async function handleKPIOnly(req, res, next) {
  try {
    const q = req.query.imei || req.query.rtuImei || req.query.name || req.query.q;
    if (!q) return res.status(400).json({ error: "imei required" });

    const imei = await resolveOneImeiOrThrow(q);

    // 에너지원 및 타입 파라미터
    const energyHex = (req.query.energy || "01").toLowerCase();
    const typeHex = (req.query.type || "").toLowerCase() || null;
    const multiHex = (req.query.multi || "").toLowerCase() || null;
    
    // JS 필터링용 정수 변환
    const targetEnergyInt = parseInt(energyHex, 16);
    const targetTypeInt = typeHex ? parseInt(typeHex, 16) : null;

    // 1. [최적화됨] 최신 데이터 조회 (인덱스 조건 추가)
    // - 인덱스 조건(14, 길이, 00)을 모두 넣어 Partial Index를 강제로 타게 함
    // - 에너지원(energyHex)도 SQL에서 미리 걸러내어 불필요한 데이터 로딩 방지
    const sqlLatest = `
      SELECT "time", body
      FROM public.log_rtureceivelog
      WHERE "rtuImei" = $1
        AND split_part(body, ' ', 5) = '00'
        AND left(body, 2) = '14'
        AND COALESCE("bodyLength", 9999) >= 12
        AND split_part(body, ' ', 2) = $2
      ORDER BY "time" DESC
      LIMIT 300
    `;
    const { rows: rawRows } = await pool.query(sqlLatest, [imei, energyHex]);

    if (!rawRows.length) {
      return res.status(422).json({ error: "NO_DATA" });
    }

    // (기존 로직 유지: 멀티별 최신 데이터 추출)
    const latestMap = new Map();
    
    for (const r of rawRows) {
      const p = parseFrame(r.body);
      if (!p?.ok || !p.metrics) continue;

      // SQL에서 energyHex를 걸렀지만 안전을 위해 이중 체크
      if (p.energy !== targetEnergyInt) continue;
      if (targetTypeInt && p.type !== targetTypeInt) continue;

      const parts = (r.body || "").trim().split(/\s+/);
      const mHex = (parts[3] || "00").toLowerCase();

      if (multiHex && mHex !== multiHex) continue;

      if (!latestMap.has(mHex)) {
        latestMap.set(mHex, r);
      }
    }

    const latestRows = Array.from(latestMap.values());

    if (!latestRows.length) {
      return res.status(422).json({ error: "NO_DATA", message: "조건에 맞는 데이터가 없습니다." });
    }

    // 2. 현재 상태 합산
    let totalWhSum = 0;
    let nowWSum = 0;
    let effList = [];

    for (const r of latestRows) {
      const p = parseFrame(r.body);
      const m = p.metrics;

      if (m.cumulativeWh != null) totalWhSum += Number(m.cumulativeWh);
      if (m.currentOutputW != null) nowWSum += Number(m.currentOutputW);
      const eff = computeInverterEfficiency(m); 
      if (eff != null) effList.push(eff);
    }

    const total_kwh = totalWhSum > 0 ? Math.round((totalWhSum / 1000) * 100) / 100 : null;
    const now_kw = nowWSum > 0 ? Math.round((nowWSum / 1000) * 100) / 100 : null;
    const inverter_efficiency_pct = effList.length
      ? Math.round((effList.reduce((a, b) => a + b, 0) / effList.length) * 10) / 10
      : null;

    // 3. 오늘 발전량 계산
    let todayWhSum = 0;
    const promises = latestRows.map(async (r) => {
      const parts = (r.body || "").split(/\s+/);
      const multi = parts[3] || "00";
      // lastBeforeStartOfDay는 이미 최적화되어 있음
      const baseRow = await lastBeforeStartOfDay(imei, energyHex, typeHex, multi);
      const latestWh = parseFrame(r.body)?.metrics?.cumulativeWh;
      const baseWh = baseRow?.body 
        ? parseFrame(baseRow.body)?.metrics?.cumulativeWh 
        : null;

      if (latestWh != null && baseWh != null) {
        const diff = Number(latestWh) - Number(baseWh);
        if (diff > 0) return diff;
      }
      return 0;
    });

    const results = await Promise.all(promises);
    todayWhSum = results.reduce((a, b) => a + b, 0);
    const today_kwh = todayWhSum > 0 ? Math.round((todayWhSum / 1000) * 100) / 100 : 0;


    // =========================================================================
    // 4. 지난달 평균 출력 계산 (집계 뷰 사용)
    // =========================================================================
    let last_month_avg_kw = null;

    const monthQuery = `
      SELECT
        (date_trunc('month', ((now() AT TIME ZONE '${TZ}') - interval '1 month')) AT TIME ZONE '${TZ}') AS prev_month_utc,
        (date_trunc('month', (now() AT TIME ZONE '${TZ}')) AT TIME ZONE '${TZ}') AS this_month_utc
    `;
    const { rows: monthRows } = await pool.query(monthQuery);
    const { prev_month_utc, this_month_utc } = monthRows[0];

    const aggParams = [imei, prev_month_utc, this_month_utc, energyHex];
    let aggConds = [
        `"rtuImei" = $1`,
        `day >= $2`,
        `day < $3`,
        `energy_hex = $4`
    ];

    if (typeHex) {
        aggParams.push(typeHex);
        aggConds.push(`type_hex = $${aggParams.length}`);
    }
    if (multiHex) {
        aggParams.push(multiHex);
        aggConds.push(`multi_hex = $${aggParams.length}`);
    }

    const aggSql = `
        SELECT SUM(max_wh - min_wh) AS total_wh
        FROM log_rtureceivelog_daily
        WHERE ${aggConds.join(' AND ')}
    `;

    const { rows: aggResult } = await pool.query(aggSql, aggParams);
    const lastMonthTotalWh = Number(aggResult[0]?.total_wh || 0);

    if (lastMonthTotalWh > 0) {
        const hours = (new Date(this_month_utc) - new Date(prev_month_utc)) / 3600_000;
        if (hours > 0) {
            let monthKwh = 0;
            // 에너지원별 단위 변환
            if (energyHex === '01') monthKwh = lastMonthTotalWh / 1000;
            else if (energyHex === '02') monthKwh = (lastMonthTotalWh / 100) / 860.42065;
            else if (energyHex === '03') monthKwh = lastMonthTotalWh / 10;
            else monthKwh = lastMonthTotalWh / 1000;

            last_month_avg_kw = Math.round((monthKwh / hours) * 100) / 100;
        }
    }

    const co2Factor = CO2_FOR(energyHex);
    const co2_kg = total_kwh != null
      ? Math.round(total_kwh * co2Factor * 100) / 100
      : null;

    return res.json({
      fast: true,
      deviceInfo: {
        imei,
        energy: energyHex,
        latestAt: latestRows[0]?.time || null,
      },
      kpis: {
        now_kw,
        today_kwh,
        total_kwh,
        co2_kg,
        inverter_efficiency_pct,
        last_month_avg_kw,
      },
    });
  } catch (err) {
    next(err);
  }
}

router.get('/kpi-fast', limiterKPI, handleKPIOnly);

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
router.get('/wind/debug',              limiterDebug,    (req,res,n)=>handleDebug(req,res,n,'04'));
router.get('/wind/instant',            limiterInstant,  (req,res,n)=>handleInstant(req,res,n,'04'));
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

// 예외 처리
router.use((err, req, res, next) => {
  console.error('[EnergyService Error]', err);

  if (err.matches && Array.isArray(err.matches)) {
    return res.status(422).json({
      ok: false,
      code: "MULTIPLE_MATCHES",
      message: "여러 장비가 검색되었습니다. 하나를 선택해주세요.",
      matches: err.matches
    });
  }

  if (err.status === 422 || err.message?.includes('no_frame')) {
    return res.status(422).json({
      ok: false,
      code: "NO_DATA",
      message: "해당 IMEI에서 정상 데이터가 없습니다."
    });
  }

  if (err.message?.includes('parse_fail') || err.message?.includes('파싱')) {
    return res.status(422).json({
      ok: false,
      code: "PARSE_FAIL",
      message: "프레임 파싱 실패(지원되지 않는 장비)"
    });
  }

  if (err.code === 'ECONNABORTED' || err.message?.includes('timeout')) {
    return res.status(504).json({
      ok: false,
      code: "TIMEOUT",
      message: "장치 응답이 없습니다 (TIMEOUT)",
    });
  }

  // 5. 기타 서버 에러
  return res.status(err.status || 500).json({
    ok: false,
    code: "SERVER_ERROR",
    message: err.message || "서버 오류가 발생했습니다."
  });
});

module.exports = router;
