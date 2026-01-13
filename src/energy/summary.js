const { pool } = require('../db/db.pg');

const ELECTRIC_CO2_ADMIN = Number(process.env.ELECTRIC_CO2_PER_KWH || '0.466');
const THERMAL_CO2_ADMIN  = Number(process.env.THERMAL_CO2_PER_KWH  || '0.198');
const ELECTRIC_CO2_USER  = 0.4747;
const THERMAL_CO2_USER   = 0.198;
const KCAL_PER_KWH       = 860.42065;
const LOG_TBL            = process.env.LOG_TBL || 'log_rtureceivelog';
const HOURS_LOOKBACK     = Number(process.env.ENERGY_LOOKBACK_HOURS || '8');

const nz = (v) => (v == null ? 0 : Number(v));

function calculateCo2Ton(kwh, factor) {
  if (!kwh || kwh <= 0) return 0;
  const co2_kg = Math.round(kwh * factor * 100) / 100;
  return co2_kg / 1000;
}

async function queryRow(sql, params = []) {
  const { rows } = await pool.query(sql, params);
  return rows[0] || {};
}


async function getElectricFromMV() {
  const sql = `SELECT COALESCE(SUM(GREATEST(max_recent - min_recent,0)),0)::numeric / 1000 AS kwh_today, COALESCE(SUM(max_recent),0)::numeric / 1000 AS kwh_cumulative FROM mv_energy_recent WHERE kind='electric'`;
  return queryRow(sql);
}

async function getThermalFromMV() {
  const sql = `SELECT COALESCE(SUM(GREATEST(max_recent - min_recent,0)),0)::numeric AS kwh_today, COALESCE(SUM(max_recent),0)::numeric AS kwh_cumulative FROM mv_energy_recent WHERE kind='thermal'`;
  return queryRow(sql);
}

const U64_BE_ADMIN = (idxExpr) => `((get_byte(b, ${idxExpr})::numeric * 72057594037927936::numeric) + (get_byte(b, ${idxExpr}+1)::numeric * 281474976710656::numeric) + (get_byte(b, ${idxExpr}+2)::numeric * 1099511627776::numeric) + (get_byte(b, ${idxExpr}+3)::numeric * 4294967296::numeric) + (get_byte(b, ${idxExpr}+4)::numeric * 16777216::numeric) + (get_byte(b, ${idxExpr}+5)::numeric * 65536::numeric) + (get_byte(b, ${idxExpr}+6)::numeric * 256::numeric) + (get_byte(b, ${idxExpr}+7)::numeric))`;

const BASE_PARSE_ADMIN = `WITH raw0 AS (SELECT "time" AS ts, "rtuImei" AS imei, regexp_replace("body", '[^0-9A-Fa-f]', '', 'g') AS clean_hex0 FROM ${LOG_TBL} WHERE "body" IS NOT NULL AND "rtuImei" IS NOT NULL AND "time" >= now() - interval '${HOURS_LOOKBACK} hours'), raw1 AS (SELECT ts, imei, CASE WHEN length(clean_hex0) % 2 = 1 THEN '0' || clean_hex0 ELSE clean_hex0 END AS clean_hex FROM raw0), hdr AS (SELECT ts, imei, decode(clean_hex, 'hex') AS b, octet_length(decode(clean_hex, 'hex')) AS len, get_byte(decode(clean_hex, 'hex'),1) AS energy, get_byte(decode(clean_hex, 'hex'),2) AS type, 5 AS data_off FROM raw1 WHERE substring(clean_hex from 1 for 2) = '14')`;

const PG_ELECTRIC_AGG = `${BASE_PARSE_ADMIN}, pv AS (SELECT imei, ${U64_BE_ADMIN('data_off + 16')} AS cum_wh FROM hdr WHERE energy=1 AND type=1 AND len >= 31 UNION ALL SELECT imei, ${U64_BE_ADMIN('data_off + 28')} AS cum_wh FROM hdr WHERE energy=1 AND type=2 AND len >= 43), per_dev AS (SELECT imei, MIN(cum_wh) AS min_recent, MAX(cum_wh) AS max_recent FROM pv GROUP BY imei) SELECT COALESCE(SUM(max_recent - min_recent),0)::numeric / 1000 AS kwh_today, COALESCE(SUM(max_recent),0)::numeric / 1000 AS kwh_cumulative FROM per_dev`;

const PG_THERMAL_AGG = `${BASE_PARSE_ADMIN}, th AS (SELECT imei, (${U64_BE_ADMIN('data_off + 28')})::numeric / ${KCAL_PER_KWH} AS cum_kwh FROM hdr WHERE energy=2 AND len >= 43), per_dev AS (SELECT imei, MIN(cum_kwh) AS min_recent, MAX(cum_kwh) AS max_recent FROM th GROUP BY imei) SELECT COALESCE(SUM(max_recent - min_recent),0)::numeric AS kwh_today, COALESCE(SUM(max_recent),0)::numeric AS kwh_cumulative FROM per_dev`;


const U64_SQL_USER = (off) => `((get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off})::numeric * 72057594037927936::numeric) + (get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off}+1)::numeric * 281474976710656::numeric) + (get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off}+2)::numeric * 1099511627776::numeric) + (get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off}+3)::numeric * 4294967296::numeric) + (get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off}+4)::numeric * 16777216::numeric) + (get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off}+5)::numeric * 65536::numeric) + (get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off}+6)::numeric * 256::numeric) + (get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off}+7)::numeric))`;

async function getSummaryByUser(energyHex, imeiList) {
  const isElectric = energyHex === '01';
  const factor = isElectric ? ELECTRIC_CO2_USER : THERMAL_CO2_USER;
  const startKST = `date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') AT TIME ZONE 'Asia/Seoul'`;

  const sql = `
    WITH base_data AS (
      SELECT "rtuImei" as imei, split_part(body, ' ', 4) as multi, "time",
        CASE 
          WHEN energy_hex='01' AND type_hex='01' THEN ${U64_SQL_USER(21)}
          WHEN energy_hex='01' AND type_hex='02' THEN ${U64_SQL_USER(33)}
          WHEN energy_hex='02' AND type_hex='01' THEN ${U64_SQL_USER(5+28)} / ${KCAL_PER_KWH}
          WHEN energy_hex='02' AND type_hex='02' THEN ${U64_SQL_USER(5+8)} / ${KCAL_PER_KWH}
          WHEN energy_hex='03' THEN ${U64_SQL_USER(5+10)} / 10.0
          ELSE 0 
        END as val
      FROM public.log_rtureceivelog
      JOIN public.imei_meta m ON m.imei = "rtuImei"
      WHERE left(body, 2) = '14' AND split_part(body, ' ', 5) = '00'
        AND m.energy_hex = '${energyHex}' AND "rtuImei" = ANY($1)
        AND "time" >= ${startKST} - interval '1 day' -- 어제 데이터 포함
    ),
    baseline AS (
      -- 어제 마지막 데이터 (00:00 KST 이전의 가장 최근 값)
      SELECT DISTINCT ON (imei, multi) imei, multi, val 
      FROM base_data WHERE "time" < ${startKST} ORDER BY imei, multi, "time" DESC
    ),
    latest AS (
      -- 현재 가장 최신 데이터
      SELECT DISTINCT ON (imei, multi) imei, multi, val 
      FROM base_data ORDER BY imei, multi, "time" DESC
    )
    SELECT 
      COALESCE(SUM(l.val - COALESCE(b.val, l.val)), 0) as today_raw, 
      COALESCE(SUM(l.val), 0) as cumulative_raw
    FROM latest l LEFT JOIN baseline b ON l.imei = b.imei AND l.multi = b.multi
  `;

  const { rows } = await pool.query(sql, [imeiList]);
  const row = rows[0];
  const todayKwh = isElectric ? nz(row.today_raw) / 1000 : nz(row.today_raw);
  const totalKwh = isElectric ? nz(row.cumulative_raw) / 1000 : nz(row.cumulative_raw);

  return {
    today_kwh: Number(todayKwh.toFixed(3)),
    cumulative_kwh: Number(totalKwh.toFixed(3)),
    today_co2_ton: calculateCo2Ton(todayKwh, factor), 
    capacity_kw: 0
  };
}


async function getEnergySummary(imeiList = null) {
  if (imeiList === null) {
    const [e, t] = await Promise.all([getElectricNationwideSummary(), getThermalNationwideSummary()]);
    return { electric: e, thermal: t };
  } else {
    const [e, t] = await Promise.all([getSummaryByUser('01', imeiList), getSummaryByUser('02', imeiList)]);
    return { electric: e, thermal: t };
  }
}

async function getElectricNationwideSummary() {
  try {
    const mv = await getElectricFromMV();
    if (mv.kwh_today != null && nz(mv.kwh_cumulative) > 0) {
      const today = nz(mv.kwh_today);
      return { today_kwh: Number(today.toFixed(3)), today_co2_ton: calculateCo2Ton(today, ELECTRIC_CO2_ADMIN), capacity_kw: 0, cumulative_kwh: Number(nz(mv.kwh_cumulative).toFixed(3)) };
    }
  } catch (e) {}
  const row = await queryRow(PG_ELECTRIC_AGG);
  const today = nz(row.kwh_today);
  return { today_kwh: Number(today.toFixed(3)), today_co2_ton: calculateCo2Ton(today, ELECTRIC_CO2_ADMIN), capacity_kw: 0, cumulative_kwh: Number(nz(row.kwh_cumulative).toFixed(3)) };
}

async function getThermalNationwideSummary() {
  try {
    const mv = await getThermalFromMV();
    if (mv.kwh_today != null && nz(mv.kwh_cumulative) > 0) {
      const today = nz(mv.kwh_today);
      return { today_kwh: Number(today.toFixed(3)), today_co2_ton: calculateCo2Ton(today, THERMAL_CO2_ADMIN), cumulative_kwh: Number(nz(mv.kwh_cumulative).toFixed(3)), collector_area_m2: 0, output_kw: 0 };
    }
  } catch (e) {}
  const row = await queryRow(PG_THERMAL_AGG);
  const today = nz(row.kwh_today);
  return { today_kwh: Number(today.toFixed(3)), today_co2_ton: calculateCo2Ton(today, THERMAL_CO2_ADMIN), cumulative_kwh: Number(nz(row.kwh_cumulative).toFixed(3)), collector_area_m2: 0, output_kw: 0 };
}

module.exports = {
  getEnergySummary,
  getNationwideEnergySummary: () => getEnergySummary(null),
  getElectricNationwideSummary,
  getThermalNationwideSummary
};