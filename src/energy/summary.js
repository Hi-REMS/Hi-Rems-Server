const { pool } = require('../db/db.pg');

const ELECTRIC_CO2_ADMIN = Number(process.env.ELECTRIC_CO2_PER_KWH || '0.466');
const THERMAL_CO2_ADMIN  = Number(process.env.THERMAL_CO2_PER_KWH  || '0.198');
const ELECTRIC_CO2_USER   = 0.4747;
const THERMAL_CO2_USER    = 0.198;
const KCAL_PER_KWH       = 860.42065;
const LOG_TBL            = process.env.LOG_TBL || 'log_rtureceivelog';

const nz = (v) => (v == null ? 0 : Number(v));

function calculateCo2Ton(kwh, factor) {
  if (!kwh || kwh <= 0) return 0;
  const co2_kg = Math.round(kwh * factor * 100000) / 100000;
  return co2_kg / 1000;
}

async function queryRow(sql, params = []) {
  const { rows } = await pool.query(sql, params);
  return rows[0] || {};
}

const U64_SQL_USER = (off) => `((get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off})::numeric * 72057594037927936::numeric) + (get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off}+1)::numeric * 281474976710656::numeric) + (get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off}+2)::numeric * 1099511627776::numeric) + (get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off}+3)::numeric * 4294967296::numeric) + (get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off}+4)::numeric * 16777216::numeric) + (get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off}+5)::numeric * 65536::numeric) + (get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off}+6)::numeric * 256::numeric) + (get_byte(decode(regexp_replace(body, '[^0-9A-Fa-f]', '', 'g'), 'hex'), ${off}+7)::numeric))`;

async function getSummaryByUser(category, imeiList) {
  const isElectric = (category === 'electric');
  const factor = isElectric ? ELECTRIC_CO2_USER : THERMAL_CO2_USER;
  
  const startKST = `date_trunc('day', now() AT TIME ZONE 'Asia/Seoul') AT TIME ZONE 'Asia/Seoul'`;
  const targetCodes = isElectric ? "('01')" : "('02', '03', '04', '06', '07')";

  const sql = `
    WITH base_data AS (
      SELECT 
        "rtuImei" as imei, 
        split_part(body, ' ', 4) as multi, 
        "time",
        CASE 
          WHEN energy_hex='01' AND type_hex='01' THEN ${U64_SQL_USER(21)}
          WHEN energy_hex='01' AND type_hex='02' THEN ${U64_SQL_USER(33)}
          WHEN energy_hex='02' AND type_hex='01' THEN ${U64_SQL_USER(17)} / (${KCAL_PER_KWH} * 100.0)
          WHEN energy_hex='02' AND type_hex='02' THEN ${U64_SQL_USER(13)} / (${KCAL_PER_KWH} * 100.0)
          WHEN energy_hex='03' THEN ${U64_SQL_USER(15)} / 10.0
          ELSE ${U64_SQL_USER(21)} / 1000.0
        END as val
      FROM public.log_rtureceivelog
      JOIN public.imei_meta m ON m.imei = "rtuImei"
      WHERE left(body, 2) = '14' 
        AND split_part(body, ' ', 5) = '00' 
        AND m.energy_hex IN ${targetCodes} 
        AND "rtuImei" = ANY($1)
        AND "time" >= ${startKST}
    ),
    daily_stats AS (
      SELECT 
        imei, multi,
        MIN(val) as min_val,
        MAX(val) as max_val
      FROM base_data
      GROUP BY imei, multi
    )
    SELECT 
      COALESCE(SUM(max_val - min_val), 0) as today_raw, 
      COALESCE(SUM(max_val), 0) as cumulative_raw
    FROM daily_stats
  `;

  try {
    const { rows } = await pool.query(sql, [imeiList]);
    const row = rows[0] || { today_raw: 0, cumulative_raw: 0 };

    const todayKwh = isElectric ? nz(row.today_raw) / 1000 : nz(row.today_raw);
    const totalKwh = isElectric ? nz(row.cumulative_raw) / 1000 : nz(row.cumulative_raw);

    return {
      today_kwh: Number(todayKwh.toFixed(3)),
      cumulative_kwh: Number(totalKwh.toFixed(3)),
      today_co2_ton: calculateCo2Ton(todayKwh, factor),
      capacity_kw: 0
    };
  } catch (e) {
    console.error('getSummaryByUser SQL Error:', e);
    throw e;
  }
}

async function getEnergySummary(imeiList = null) {
  if (imeiList === null) {
    const [e, t] = await Promise.all([getElectricNationwideSummary(), getThermalNationwideSummary()]);
    return { electric: e, thermal: t };
  } else {
    const [e, t] = await Promise.all([
      getSummaryByUser('electric', imeiList), 
      getSummaryByUser('thermal', imeiList)
    ]);
    return { electric: e, thermal: t };
  }
}

async function getElectricFromMV() {
  const sql = `SELECT COALESCE(SUM(GREATEST(max_recent - min_recent,0)),0)::numeric / 1000 AS kwh_today, COALESCE(SUM(max_recent),0)::numeric / 1000 AS kwh_cumulative FROM mv_energy_recent WHERE kind='electric'`;
  return queryRow(sql);
}

async function getThermalFromMV() {
  const sql = `SELECT COALESCE(SUM(GREATEST(max_recent - min_recent,0)),0)::numeric AS kwh_today, COALESCE(SUM(max_recent),0)::numeric AS kwh_cumulative FROM mv_energy_recent WHERE kind='thermal'`;
  return queryRow(sql);
}

async function getElectricNationwideSummary() {
  try {
    const mv = await getElectricFromMV();
    if (mv.kwh_today != null && nz(mv.kwh_cumulative) > 0) {
      const today = nz(mv.kwh_today);
      return { today_kwh: Number(today.toFixed(3)), today_co2_ton: calculateCo2Ton(today, ELECTRIC_CO2_ADMIN), capacity_kw: 0, cumulative_kwh: Number(nz(mv.kwh_cumulative).toFixed(3)) };
    }
  } catch (e) {}
  return { today_kwh: 0, today_co2_ton: 0, capacity_kw: 0, cumulative_kwh: 0 };
}

async function getThermalNationwideSummary() {
  try {
    const mv = await getThermalFromMV();
    if (mv.kwh_today != null && nz(mv.kwh_cumulative) > 0) {
      const today = nz(mv.kwh_today);
      return { today_kwh: Number(today.toFixed(3)), today_co2_ton: calculateCo2Ton(today, THERMAL_CO2_ADMIN), cumulative_kwh: Number(nz(mv.kwh_cumulative).toFixed(3)), collector_area_m2: 0, output_kw: 0 };
    }
  } catch (e) {}
  return { today_kwh: 0, today_co2_ton: 0, cumulative_kwh: 0, collector_area_m2: 0, output_kw: 0 };
}

module.exports = {
  getEnergySummary,
  getNationwideEnergySummary: () => getEnergySummary(null),
  getElectricNationwideSummary,
  getThermalNationwideSummary
};