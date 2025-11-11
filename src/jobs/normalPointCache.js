// src/jobs/normalPointsCache.js
const { pool } = require('../db/db.pg');

let cache = { at: 0, data: [] };
const TTL = 5 * 60 * 1000;

async function fetchNormalPoints() {
  const sql = `
    WITH latest AS (
      SELECT DISTINCT ON (r."rtuImei")
        r."rtuImei" AS imei,
        r."opMode",
        r."time" AS last_time
      FROM public."log_rtureceivelog" r
      WHERE r."time" >= NOW() - make_interval(days => 3)
      ORDER BY r."rtuImei", r."time" DESC
    )
    SELECT l.imei, l."opMode" AS op_mode, l.last_time,
           m.sido, m.sigungu, m.address, m.lat, m.lon
    FROM latest l
    LEFT JOIN public.imei_meta m ON m.imei = l.imei
    WHERE l."opMode" = '0'
      AND m.lat IS NOT NULL AND m.lon IS NOT NULL
  `;
  const { rows } = await pool.query(sql);
  return rows.map(r => ({
    imei: r.imei,
    la: r.lat,
    lo: r.lon,
    s: r.sido,
    g: r.sigungu,
    a: r.address
  }));
}

async function getNormalPointsCached() {
  if (Date.now() - cache.at < TTL && cache.data.length) return cache.data;
  cache.data = await fetchNormalPoints();
  cache.at = Date.now();
  return cache.data;
}

module.exports = { getNormalPointsCached };
