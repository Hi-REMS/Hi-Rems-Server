// src/energy/summary.js
// 전국 에너지 집계 (전기/열) — MV 우선, 원본 최근구간 fallback
// ✅ today/cumulative 모두 kWh로 통일

const { pool } = require('../db/db.pg');

// 환경 상수
const ELECTRIC_CO2_PER_KWH = Number(process.env.ELECTRIC_CO2_PER_KWH || '0.466'); // kgCO2/kWh
const THERMAL_CO2_PER_KWH  = Number(process.env.THERMAL_CO2_PER_KWH  || '0.198'); // kgCO2/kWh
const KCAL_PER_KWH         = 860.42065;
const LOG_TBL              = process.env.LOG_TBL || 'log_rtureceivelog';
const HOURS_LOOKBACK       = Number(process.env.ENERGY_LOOKBACK_HOURS || '8'); // 기본 8시간

const nz   = (v) => (v == null ? 0 : Number(v));
const kg2t = (kg) => kg / 1000;

async function queryRow(sql, params = []) {
  const { rows } = await pool.query(sql, params);
  return rows[0] || {};
}

/* -------------------- MV 기반 조회 -------------------- */
// mv_energy_recent: electric은 min/max가 Wh, thermal은 kWh로 저장했었음
// -> 여기서 모두 kWh로 변환해서 반환
async function getElectricFromMV() {
  const sql = `
    SELECT
      /* Wh → kWh */
      COALESCE(SUM(GREATEST(max_recent - min_recent,0)),0)::numeric / 1000 AS kwh_today,
      COALESCE(SUM(max_recent),0)::numeric                                / 1000 AS kwh_cumulative
    FROM mv_energy_recent
    WHERE kind='electric';
  `;
  return queryRow(sql);
}

async function getThermalFromMV() {
  const sql = `
    SELECT
      /* thermal은 이미 kWh */
      COALESCE(SUM(GREATEST(max_recent - min_recent,0)),0)::numeric AS kwh_today,
      COALESCE(SUM(max_recent),0)::numeric                          AS kwh_cumulative
    FROM mv_energy_recent
    WHERE kind='thermal';
  `;
  return queryRow(sql);
}

/* -------------------- fallback (최근 N시간만 원본) -------------------- */
const U64_BE = (idxExpr) => `
(
  (get_byte(b, ${idxExpr})     ::numeric * 72057594037927936::numeric) +
  (get_byte(b, ${idxExpr} + 1) ::numeric * 281474976710656::numeric)   +
  (get_byte(b, ${idxExpr} + 2) ::numeric * 1099511627776::numeric)     +
  (get_byte(b, ${idxExpr} + 3) ::numeric * 4294967296::numeric)        +
  (get_byte(b, ${idxExpr} + 4) ::numeric * 16777216::numeric)          +
  (get_byte(b, ${idxExpr} + 5) ::numeric * 65536::numeric)             +
  (get_byte(b, ${idxExpr} + 6) ::numeric * 256::numeric)               +
  (get_byte(b, ${idxExpr} + 7) ::numeric)
)
`;

const BASE_PARSE = `
WITH raw0 AS (
  SELECT
    "time" AS ts,
    "rtuImei" AS imei,
    regexp_replace("body", '[^0-9A-Fa-f]', '', 'g') AS clean_hex0
  FROM ${LOG_TBL}
  WHERE "body" IS NOT NULL
    AND "rtuImei" IS NOT NULL
    AND "time" >= now() - interval '${HOURS_LOOKBACK} hours'
),
raw1 AS (
  SELECT ts, imei,
         CASE WHEN length(clean_hex0) % 2 = 1 THEN '0' || clean_hex0 ELSE clean_hex0 END AS clean_hex
  FROM raw0
),
hdr AS (
  SELECT
    ts, imei,
    decode(clean_hex, 'hex') AS b,
    octet_length(decode(clean_hex, 'hex')) AS len,
    get_byte(decode(clean_hex, 'hex'),0) AS cmd,
    get_byte(decode(clean_hex, 'hex'),1) AS energy,
    get_byte(decode(clean_hex, 'hex'),2) AS type,
    get_byte(decode(clean_hex, 'hex'),3) AS multi,
    get_byte(decode(clean_hex, 'hex'),4) AS err,
    5 AS data_off
  FROM raw1
  WHERE substring(clean_hex from 1 for 2) = '14'
)
`;

// 전기: fallback도 kWh로 반환
const PG_ELECTRIC_AGG = `
${BASE_PARSE},
pv_single AS (
  SELECT ts, imei, multi,
         ${U64_BE('data_off + 18')} AS cum_wh
  FROM hdr
  WHERE err=0 AND energy=1 AND type=1 AND len >= data_off+26
),
pv_three AS (
  SELECT ts, imei, multi,
         ${U64_BE('data_off + 28')} AS cum_wh
  FROM hdr
  WHERE err=0 AND energy=1 AND type=2 AND len >= data_off+38
),
pv AS (SELECT * FROM pv_single UNION ALL SELECT * FROM pv_three),
per_dev AS (
  SELECT imei, multi,
         MIN(cum_wh) AS min_recent,
         MAX(cum_wh) AS max_recent
  FROM pv GROUP BY imei, multi
)
SELECT
  COALESCE(SUM(GREATEST(max_recent - min_recent,0)),0)::numeric / 1000 AS kwh_today,       -- Wh→kWh
  COALESCE(SUM(max_recent),0)::numeric                                / 1000 AS kwh_cumulative
FROM per_dev;
`;

// 열: fallback은 이미 kWh 산출
const PG_THERMAL_AGG = `
${BASE_PARSE},
th AS (
  SELECT ts, imei, multi,
         (${U64_BE('data_off + 28')})::numeric / ${KCAL_PER_KWH} AS cum_kwh
  FROM hdr
  WHERE err=0 AND energy=2 AND len >= data_off+38
),
per_dev AS (
  SELECT imei, multi,
         MIN(cum_kwh) AS min_recent,
         MAX(cum_kwh) AS max_recent
  FROM th GROUP BY imei, multi
)
SELECT
  COALESCE(SUM(GREATEST(max_recent - min_recent,0)),0)::numeric AS kwh_today,
  COALESCE(SUM(max_recent),0)::numeric                          AS kwh_cumulative
FROM per_dev;
`;

/* -------------------- 서비스 함수 -------------------- */
async function getElectricNationwideSummary() {
  try {
    const mv = await getElectricFromMV();
    if (mv.kwh_today != null) {
      const today = nz(mv.kwh_today);
      const cum   = nz(mv.kwh_cumulative);
      const co2_t = kg2t(today * ELECTRIC_CO2_PER_KWH);
      return {
        today_kwh:       Number(today.toFixed(3)),
        today_co2_ton:   Number(co2_t.toFixed(3)),
        capacity_kw:     0,
        cumulative_kwh:  Number(cum.toFixed(3))
      };
    }
  } catch (e) {
    console.error('MV electric failed, fallback:', e);
  }

  // fallback
  const row   = await queryRow(PG_ELECTRIC_AGG);
  const today = nz(row.kwh_today);
  const cum   = nz(row.kwh_cumulative);
  const co2_t = kg2t(today * ELECTRIC_CO2_PER_KWH);
  return {
    today_kwh:       Number(today.toFixed(3)),
    today_co2_ton:   Number(co2_t.toFixed(3)),
    capacity_kw:     0,
    cumulative_kwh:  Number(cum.toFixed(3))
  };
}

async function getThermalNationwideSummary() {
  try {
    const mv = await getThermalFromMV();
    if (mv.kwh_today != null) {
      const today = nz(mv.kwh_today);
      const cum   = nz(mv.kwh_cumulative);
      const co2_t = kg2t(today * THERMAL_CO2_PER_KWH);
      return {
        today_kwh:         Number(today.toFixed(3)),
        today_co2_ton:     Number(co2_t.toFixed(3)),
        cumulative_kwh:    Number(cum.toFixed(3)),
        collector_area_m2: 0,
        output_kw:         0
      };
    }
  } catch (e) {
    console.error('MV thermal failed, fallback:', e);
  }

  // fallback
  const row   = await queryRow(PG_THERMAL_AGG);
  const today = nz(row.kwh_today);
  const cum   = nz(row.kwh_cumulative);
  const co2_t = kg2t(today * THERMAL_CO2_PER_KWH);
  return {
    today_kwh:         Number(today.toFixed(3)),
    today_co2_ton:     Number(co2_t.toFixed(3)),
    cumulative_kwh:    Number(cum.toFixed(3)),
    collector_area_m2: 0,
    output_kw:         0
  };
}

async function getNationwideEnergySummary() {
  const [electric, thermal] = await Promise.all([
    getElectricNationwideSummary(),
    getThermalNationwideSummary()
  ]);
  return { electric, thermal };
}

module.exports = {
  getNationwideEnergySummary,
  getElectricNationwideSummary,
  getThermalNationwideSummary
};
