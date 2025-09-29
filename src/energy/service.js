// src/energy/service.js
const express = require('express');
const router = express.Router();
const rateLimit = require('express-rate-limit'); // ★ 추가
const { pool } = require('../db/db.pg');
const { parseFrame } = require('./parser');
const { TZ } = require('./timeutil');

const EMISSION_FACTOR = Number(process.env.EMISSION_FACTOR_KG_PER_KWH || 0.4747);
const TREE_CO2_KG = 6.6;

// --------------------------------------------------
// Rate limiters (라우트별 성격에 맞게 다른 한도)
// --------------------------------------------------
const makeLimiter = (maxPerMin) =>
  rateLimit({
    windowMs: 60 * 1000,
    max: maxPerMin,
    message: { error: 'Too many requests — try again later.' },
    standardHeaders: true,
    legacyHeaders: false,
  });

const limiterKPI       = makeLimiter(15); // /electric (여러 쿼리)
const limiterPreview   = makeLimiter(30); // /electric/preview
const limiterDebug     = makeLimiter(10); // /electric/debug (비용 큼)
const limiterInstant   = makeLimiter(30); // /electric/instant (단일 조회)
const limiterInstantM  = makeLimiter(30); // /electric/instant/multi
const limiterHourly    = makeLimiter(10); // /electric/hourly (시간대별 반복 조회)

// BigInt 안전 직렬화
const jsonSafe = (obj) =>
  JSON.parse(JSON.stringify(obj, (_, v) => (typeof v === 'bigint' ? v.toString() : v)));

const isImeiLike = (s) => typeof s === 'string' && s.length >= 8;
const ONLY_OK = 'AND split_part(body,\' \',5) = \'00\'';

/* ---------- 공통 유틸 ---------- */
const mapByMulti = (rows) => {
  const m = new Map();
  for (const r of rows || []) {
    if (r && r.multi_hex) m.set(r.multi_hex, r);
  }
  return m;
};


async function lastBeforeNow(imei, energyHex = null, typeHex = null) {
  let idx = 2;
  const sql = `
    SELECT "time", body
    FROM public.log_rtureceivelog
    WHERE "rtuImei" = $1
      ${energyHex ? `AND left(body,2)='14' AND split_part(body,' ',2) = $${idx++}` : ''}
      ${typeHex   ? `AND split_part(body,' ',3) = $${idx++}` : ''}
      ${ONLY_OK}
    ORDER BY "time" DESC
    LIMIT 1`;
  const params = [imei];
  if (energyHex) params.push(energyHex);
  if (typeHex) params.push(typeHex);
  const r = await pool.query(sql, params);
  return r.rows[0] || null;
}

async function lastBeforeNowByMulti(imei, { energyHex=null, typeHex=null, multiHex=null } = {}) {
  let idx = 2;
  const params = [imei];
  const parts = [];

  if (energyHex) { parts.push(`left(body,2)='14' AND split_part(body,' ',2) = $${idx++}`); params.push(energyHex); }
  if (typeHex)   { parts.push(`split_part(body,' ',3) = $${idx++}`);                       params.push(typeHex);   }
  if (multiHex)  { parts.push(`split_part(body,' ',4) = $${idx++}`);                       params.push(multiHex); }

  const sql = `
    SELECT "time", body
    FROM public.log_rtureceivelog
    WHERE "rtuImei" = $1
      ${parts.length ? 'AND ' + parts.join(' AND ') : ''}
      AND split_part(body,' ',5)='00'
    ORDER BY "time" DESC
    LIMIT 1`;
  const r = await pool.query(sql, params);
  return r.rows[0] || null;
}

/* ---------- SQL helpers (멀티별) ---------- */
async function latestPerMulti(imei, { energyHex=null, typeHex=null } = {}) {
  const params = [imei];
  let idx = 2;
  let condEnergy = '';
  let condType = '';

  if (energyHex) { condEnergy = `AND left(body,2)='14' AND split_part(body,' ',2) = $${idx++}`; params.push(energyHex); }
  if (typeHex)   { condType   = `AND split_part(body,' ',3) = $${idx++}`;                     params.push(typeHex);   }

  const sql = `
    (
      SELECT '00' AS multi_hex, "time", body
      FROM public.log_rtureceivelog
      WHERE "rtuImei"=$1 ${condEnergy} ${condType}
        AND split_part(body,' ',4)='00' AND split_part(body,' ',5)='00'
      ORDER BY "time" DESC LIMIT 1
    )
    UNION ALL
    (
      SELECT '01' AS multi_hex, "time", body
      FROM public.log_rtureceivelog
      WHERE "rtuImei"=$1 ${condEnergy} ${condType}
        AND split_part(body,' ',4)='01' AND split_part(body,' ',5)='00'
      ORDER BY "time" DESC LIMIT 1
    )
    UNION ALL
    (
      SELECT '02' AS multi_hex, "time", body
      FROM public.log_rtureceivelog
      WHERE "rtuImei"=$1 ${condEnergy} ${condType}
        AND split_part(body,' ',4)='02' AND split_part(body,' ',5)='00'
      ORDER BY "time" DESC LIMIT 1
    )
    UNION ALL
    (
      SELECT '03' AS multi_hex, "time", body
      FROM public.log_rtureceivelog
      WHERE "rtuImei"=$1 ${condEnergy} ${condType}
        AND split_part(body,' ',4)='03' AND split_part(body,' ',5)='00'
      ORDER BY "time" DESC LIMIT 1
    )
  `;
  const { rows } = await pool.query(sql, params);
  return rows.map(r => ({ multi_hex: r.multi_hex, time: r.time || null, body: r.body || null }));
}

async function firstAfterPerMulti(imei, tsUtc, { energyHex=null, typeHex=null } = {}) {
  const params = [imei, tsUtc];
  let idx = 3;
  let condEnergy = '';
  let condType = '';

  if (energyHex) { condEnergy = `AND left(body,2)='14' AND split_part(body,' ',2) = $${idx++}`; params.push(energyHex); }
  if (typeHex)   { condType   = `AND split_part(body,' ',3) = $${idx++}`;                     params.push(typeHex);   }

  const sql = `
    (
      SELECT '00' AS multi_hex, "time", body
      FROM public.log_rtureceivelog
      WHERE "rtuImei"=$1 AND "time" >= $2 ${condEnergy} ${condType}
        AND split_part(body,' ',4)='00' AND split_part(body,' ',5)='00'
      ORDER BY "time" ASC LIMIT 1
    )
    UNION ALL
    (
      SELECT '01' AS multi_hex, "time", body
      FROM public.log_rtureceivelog
      WHERE "rtuImei"=$1 AND "time" >= $2 ${condEnergy} ${condType}
        AND split_part(body,' ',4)='01' AND split_part(body,' ',5)='00'
      ORDER BY "time" ASC LIMIT 1
    )
    UNION ALL
    (
      SELECT '02' AS multi_hex, "time", body
      FROM public.log_rtureceivelog
      WHERE "rtuImei"=$1 AND "time" >= $2 ${condEnergy} ${condType}
        AND split_part(body,' ',4)='02' AND split_part(body,' ',5)='00'
      ORDER BY "time" ASC LIMIT 1
    )
    UNION ALL
    (
      SELECT '03' AS multi_hex, "time", body
      FROM public.log_rtureceivelog
      WHERE "rtuImei"=$1 AND "time" >= $2 ${condEnergy} ${condType}
        AND split_part(body,' ',4)='03' AND split_part(body,' ',5)='00'
      ORDER BY "time" ASC LIMIT 1
    )
  `;
  const { rows } = await pool.query(sql, params);
  return rows.map(r => ({ multi_hex: r.multi_hex, time: r.time || null, body: r.body || null }));
}

/* ---------- parsing helpers ---------- */
function headerFromHex(hex) {
  const parts = (hex || '').trim().split(/\s+/);
  return {
    energy: parts[1] ? parseInt(parts[1], 16) : null,
    type:   parts[2] ? parseInt(parts[2], 16) : null
  };
}

function pickMetrics(hex) {
  const p = parseFrame(hex);
  const head = headerFromHex(hex);
  if (!p || !p.ok || !p.metrics) {
    return { wh:null, w:null, eff:null, energy: p?.energy ?? head.energy, type: p?.type ?? head.type };
  }
  const m = p.metrics;
  const hasWh = Object.prototype.hasOwnProperty.call(m, 'cumulativeWh');
  const hasW  = Object.prototype.hasOwnProperty.call(m, 'currentOutputW');
  const wh = hasWh ? m.cumulativeWh : null;
  const w  = hasW  ? Number(m.currentOutputW) : null;

  let eff = null;
  if (m.pvVoltage!=null && m.pvCurrent!=null && m.powerFactor!=null) {
    if (m.systemVoltage!=null && m.systemCurrent!=null) {
      const inputW  = m.pvVoltage * m.pvCurrent;
      const outputW = m.systemVoltage * m.systemCurrent * m.powerFactor;
      if (outputW > 0) eff = Math.round(((inputW/outputW)*100)*100)/100;
    } else if (
      m.systemR_V!=null && m.systemS_V!=null && m.systemT_V!=null &&
      m.systemR_I!=null && m.systemS_I!=null && m.systemT_I!=null
    ) {
      const sumVI = (m.systemR_V*m.systemR_I) + (m.systemS_V*m.systemS_I) + (m.systemT_V*m.systemT_I);
      const inputW  = m.pvVoltage * m.pvCurrent;
      const outputW = sumVI * m.powerFactor;
      if (outputW > 0) eff = Math.round(((inputW/outputW)*100)*100)/100;
    }
  }
  return { wh, w: Number.isFinite(w) ? w : null, eff, energy: p.energy, type: p.type };
}

/* ---------- endpoints ---------- */

// KPI 요약 (멀티 합산)
router.get('/electric', limiterKPI, async (req, res, next) => { // ★ limiter 추가
  try {
    const imei = req.query.rtuImei || req.query.imei;
    if (!isImeiLike(imei)) { const e = new Error('rtuImei(또는 imei) 파라미터가 필요합니다.'); e.status = 400; throw e; }
    const energyHex = (req.query.energy || '01').toLowerCase();
    const typeHex   = (req.query.type   || '').toLowerCase() || null;

    // 기준 시각들 (KST 자정/월초를 UTC로 변환)
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

    // 멀티별 프레임 묶음
    const [latestRows, todayFirstRows, prevMonthFirstRows, thisMonthFirstRows] = await Promise.all([
      latestPerMulti(imei, { energyHex, typeHex }),
      firstAfterPerMulti(imei, start_utc,      { energyHex, typeHex }),
      firstAfterPerMulti(imei, prev_month_utc, { energyHex, typeHex }),
      firstAfterPerMulti(imei, this_month_utc, { energyHex, typeHex }),
    ]);

    // 최신 프레임이 하나도 없으면 종료
    const anyLatest = latestRows.some(r => r?.body);
    if (!anyLatest) return res.status(422).json({ error:'no_frames_for_energy',
      message:`해당 IMEI에서 energy=0x${energyHex}${typeHex?` type=0x${typeHex}`:''} 정상(0x00) 프레임이 없습니다.` });

    // 합산/평균 계산
    let totalWhSum = 0n;
    let haveWh = false;
    let nowWSum = 0;
    const effList = [];

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

    // ===== 금일 발전량(멀티키로 정확 매칭) =====
    const latestMap     = mapByMulti(latestRows);
    const todayFirstMap = mapByMulti(todayFirstRows);

    let todayWhSum = 0n; let haveToday = false;
    for (const [multi, Lrow] of latestMap.entries()) {
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

    // ===== 지난달 평균 kW(멀티키로 정확 매칭) =====
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

    // 파생 KPI
    const co2_kg  = total_kwh != null ? Math.round(total_kwh*EMISSION_FACTOR*100)/100 : null;
    const co2_ton = co2_kg   != null ? Math.round((co2_kg/1000)*100)/100 : null;
    const trees   = co2_kg   != null ? Math.floor(co2_kg / TREE_CO2_KG) : null;

    // 최신 시각(멀티 중 가장 최신)
    const latestAt = latestRows
      .map(r => r?.time ? new Date(r.time).getTime() : 0)
      .reduce((a,b)=>Math.max(a,b),0);
    const latestAtIso = latestAt ? new Date(latestAt).toISOString() : null;

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
      meta: {
        table:'public.log_rtureceivelog', tz:TZ,
        emission_factor_kg_per_kwh: EMISSION_FACTOR, energy_hex:energyHex, type_hex:typeHex
      }
    });
  } catch (e) { next(e); }
});

// 프리뷰 (최근 프레임 시계열)  ★ multi 필터 추가
router.get('/electric/preview', limiterPreview, async (req, res, next) => { // ★ limiter 추가
  try {
    const imei = req.query.rtuImei || req.query.imei;
    let limit = Math.min(parseInt(req.query.limit || '200', 10), 2000);
    if (!isImeiLike(imei)) { const e = new Error('rtuImei(또는 imei) 파라미터가 필요합니다.'); e.status = 400; throw e; }

    const energyHex = (req.query.energy || '').toLowerCase();
    const typeHex   = (req.query.type   || '').toLowerCase();
    const onlyOk    = String(req.query.ok || '') === '1';
    const multiHex  = (req.query.multi  || '').toLowerCase();  // ★ 추가

    const conds = ['"rtuImei" = $1'];
    const params = [imei];
    if (energyHex) { conds.push(`left(body,2)='14' AND split_part(body,' ',2) = $${params.length+1}`); params.push(energyHex); }
    if (typeHex)   { conds.push(`split_part(body,' ',3) = $${params.length+1}`);                       params.push(typeHex);   }
    if (multiHex)  { conds.push(`split_part(body,' ',4) = $${params.length+1}`);                       params.push(multiHex); } // ★ 추가
    if (onlyOk)    { conds.push('split_part(body,\' \',5)=\'00\''); }

    const q = `
      SELECT "time", body
      FROM public.log_rtureceivelog
      WHERE ${conds.join(' AND ')}
      ORDER BY "time" DESC
      LIMIT $${params.length+1}`;
    params.push(limit);

    const { rows } = await pool.query(q, params);

    const points = rows.map(r => {
      const p = pickMetrics(r.body);
      const head = headerFromHex(r.body);
      const parts = (r.body || '').trim().split(/\s+/);
      const whBig = p.wh;
      const whNum = (whBig!=null) ? Number(whBig) : null;
      return {
        ts: r.time,
        kw: p.w!=null ? Math.round((p.w/1000)*100)/100 : null,
        wh: whNum,
        wh_str: whBig!=null ? String(whBig) : null,
        energy: p.energy ?? head.energy,
        type:   p.type   ?? head.type,
        multi:  parts[3] || null,    // 디버깅/검증용
      };
    });
    res.json(points);
  } catch (e) { next(e); }
});

// 디버그 (원본+파싱상태)
router.get('/electric/debug', limiterDebug, async (req, res, next) => { // ★ limiter 추가
  try {
    const imei = req.query.rtuImei || req.query.imei;
    const limit = Math.min(parseInt(req.query.limit || '5', 10), 50);
    if (!imei) return res.status(400).json({ error: 'rtuImei is required' });

    const { rows } = await pool.query(
      `SELECT "time","bodyLength",body
       FROM public.log_rtureceivelog
       WHERE "rtuImei"=$1
       ORDER BY "time" DESC
       LIMIT $2`, [imei, limit]
    );

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
          metrics: p?.metrics ? {
            pvV: p.metrics.pvVoltage ?? null,
            pvA: p.metrics.pvCurrent ?? null,
            curW: p.metrics.currentOutputW ?? null,
            wh:  p.metrics.cumulativeWh ?? null,
            pf:  p.metrics.powerFactor ?? null,
            hz:  p.metrics.frequencyHz ?? null,
          } : null
        },
        raw: r.body
      };
    });
    res.json(jsonSafe(out));
  } catch (e) { next(e); }
});

// 최신 프레임 즉시 조회 (옵션: multi)
router.get('/electric/instant', limiterInstant, async (req, res, next) => { // ★ limiter 추가
  try {
    const imei = req.query.rtuImei || req.query.imei;
    if (!isImeiLike(imei)) { const e = new Error('rtuImei(또는 imei) 파라미터가 필요합니다.'); e.status = 400; throw e; }

    const energyHex = (req.query.energy || '01').toLowerCase();
    const typeHex   = (req.query.type   || '').toLowerCase() || null;
    const multiHex  = (req.query.multi  || '').toLowerCase() || null;

    const row = await (multiHex
      ? lastBeforeNowByMulti(imei, { energyHex, typeHex, multiHex })
      : lastBeforeNow(imei, energyHex, typeHex));
    if (!row) return res.status(422).json({ error: 'no_frame', message: '정상 프레임이 없습니다.' });

    const p = parseFrame(row.body);
    if (!p?.ok || !p?.metrics) {
      return res.status(422).json({ error: 'parse_fail', message: p?.reason || '파싱 실패', raw: row.body });
    }
    const m = p.metrics;

    // 프레임에서 읽은 멀티 값까지 표시
    const parts = (row.body || '').trim().split(/\s+/);
    const multiFromFrame = parts[3] || null;

    const payload = {
      ts: row.time,
      energy: p.energy,
      type: p.type,
      multi: (multiHex ?? multiFromFrame),
      pv_voltage_v: m.pvVoltage ?? null,
      pv_current_a: m.pvCurrent ?? null,
      pv_power_w: (() => {
        if (m.pvPowerW != null) return m.pvPowerW;
        if (m.pvOutputW != null) return m.pvOutputW;
        if (m.pvVoltage != null && m.pvCurrent != null) return m.pvVoltage * m.pvCurrent;
        return null;
      })(),
      system_voltage_v: m.systemVoltage ?? null,
      system_current_a: m.systemCurrent ?? null,
      power_factor: m.powerFactor ?? null,
      frequency_hz: m.frequencyHz ?? null,
      current_output_w: m.currentOutputW ?? null,
      cumulative_wh: m.cumulativeWh ?? null,
      // 삼상 보조
      system_r_voltage_v: m.systemR_V ?? null,
      system_s_voltage_v: m.systemS_V ?? null,
      system_t_voltage_v: m.systemT_V ?? null,
      system_r_current_a: m.systemR_I ?? null,
      system_s_current_a: m.systemS_I ?? null,
      system_t_current_a: m.systemT_I ?? null,
    };

    res.json(jsonSafe(payload));
  } catch (e) { next(e); }
});

// 멀티(설비 슬롯)별 최신값 + 합계/평균
router.get('/electric/instant/multi', limiterInstantM, async (req, res, next) => { // ★ limiter 추가
  try {
    const imei = req.query.rtuImei || req.query.imei;
    if (!isImeiLike(imei)) { const e = new Error('rtuImei(또는 imei) 파라미터가 필요합니다.'); e.status = 400; throw e; }
    const energyHex = (req.query.energy || '01').toLowerCase();
    const typeHex   = (req.query.type   || '').toLowerCase() || null;

    const rows = await latestPerMulti(imei, { energyHex, typeHex });

    const units = rows.map(r => {
      const multi = r.multi_hex; // '00'|'01'|'02'|'03'
      const p = parseFrame(r.body);
      const m = p?.metrics || {};
      const pvPowerW = (m.pvPowerW ?? m.pvOutputW ??
        ((m.pvVoltage!=null && m.pvCurrent!=null) ? m.pvVoltage*m.pvCurrent : null));

      return {
        multi,
        ts: r.time,
        pv_voltage_v: m.pvVoltage ?? null,
        pv_current_a: m.pvCurrent ?? null,
        pv_power_w:   pvPowerW,
        system_voltage_v: m.systemVoltage ?? null,
        system_current_a: m.systemCurrent ?? null,
        power_factor:    m.powerFactor ?? null,
        frequency_hz:    m.frequencyHz ?? null,
        current_output_w: m.currentOutputW ?? null,
        cumulative_wh:    m.cumulativeWh ?? null,
        // 삼상 보조
        system_r_voltage_v: m.systemR_V ?? null,
        system_s_voltage_v: m.systemS_V ?? null,
        system_t_voltage_v: m.systemT_V ?? null,
        system_r_current_a: m.systemR_I ?? null,
        system_s_current_a: m.systemS_I ?? null,
        system_t_current_a: m.systemT_I ?? null,
      };
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
});

const { DateTime } = require('luxon');

// 시간대별 발전량 (kWh)
router.get('/electric/hourly', limiterHourly, async (req, res, next) => { // ★ limiter 추가
  try {
    const imei = req.query.rtuImei || req.query.imei;
    if (!isImeiLike(imei)) {
      const e = new Error('rtuImei(또는 imei) 파라미터가 필요합니다.');
      e.status = 400; throw e;
    }

    const energyHex = (req.query.energy || '01').toLowerCase();
    const typeHex   = (req.query.type   || '').toLowerCase() || null;
    const dateStr   = req.query.date; // YYYY-MM-DD (KST 기준), 없으면 오늘

    // 기준 날짜 (KST)
    const baseKST = dateStr
      ? DateTime.fromFormat(dateStr, 'yyyy-LL-dd', { zone: TZ })
      : DateTime.now().setZone(TZ);

    // 0시~23시 KST 각 시간 구간의 UTC 경계
    const hours = [];
    for (let h=0; h<24; h++) {
      const startKST = baseKST.startOf('day').plus({ hours:h });
      const endKST   = startKST.plus({ hours:1 });
      hours.push({ h, startUtc:startKST.toUTC().toJSDate(), endUtc:endKST.toUTC().toJSDate() });
    }

    const results = [];
    for (const {h, startUtc, endUtc} of hours) {
      // 시간대 첫 프레임 / 마지막 프레임
      const firstRows = await firstAfterPerMulti(imei, startUtc, { energyHex, typeHex });
      const lastRows  = await firstAfterPerMulti(imei, endUtc,   { energyHex, typeHex });

      const firstMap = mapByMulti(firstRows);
      const lastMap  = mapByMulti(lastRows);

      let sumWh = 0n; let have = false;
      for (const [multi, Lrow] of lastMap.entries()) {
        const Frow = firstMap.get(multi);
        if (!Lrow?.body || !Frow?.body) continue;
        const L = pickMetrics(Lrow.body);
        const F = pickMetrics(Frow.body);
        if (L?.wh != null && F?.wh != null && L.wh >= F.wh) {
          sumWh += (L.wh - F.wh);
          have = true;
        }
      }
      const kwh = have ? Number(sumWh) / 1000 : null;
      results.push({ hour: String(h).padStart(2,'0'), kwh });
    }

    res.json({ date: baseKST.toFormat('yyyy-LL-dd'), imei, energy:energyHex, type:typeHex, hours:results });
  } catch (e) { next(e); }
});

module.exports = router;
