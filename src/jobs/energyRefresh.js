// src/jobs/energyRefresh.js
const cron = require('node-cron');
const { pool } = require('../db/db.pg');
const {
  getElectricNationwideSummary,
  getThermalNationwideSummary,
} = require('../energy/summary');

let memoryCache = { electric: null, thermal: null, updatedAt: null };

async function refreshOnce() {
  const [electric, thermal] = await Promise.all([
    getElectricNationwideSummary(),
    getThermalNationwideSummary(),
  ]);
  memoryCache = { electric, thermal, updatedAt: new Date().toISOString() };

  // (옵션) 재시작 후에도 유지하려면 DB에 저장
  await pool.query(
    `CREATE TABLE IF NOT EXISTS energy_nationwide_cache (
       key text PRIMARY KEY,
       payload jsonb NOT NULL,
       updated_at timestamptz NOT NULL DEFAULT now()
     )`
  );
  await pool.query(
    `INSERT INTO energy_nationwide_cache (key, payload, updated_at)
     VALUES ($1,$2,now())
     ON CONFLICT (key) DO UPDATE SET payload=$2, updated_at=now()`,
    ['electric', electric]
  );
  await pool.query(
    `INSERT INTO energy_nationwide_cache (key, payload, updated_at)
     VALUES ($1,$2,now())
     ON CONFLICT (key) DO UPDATE SET payload=$2, updated_at=now()`,
    ['thermal', thermal]
  );

  return memoryCache;
}

function getCache() { return memoryCache; }

function setupEnergyCron() {
  // 매 1분 실행(원하면 */5로 5분마다 등)
  cron.schedule('*/1 * * * *', async () => {
    try { await refreshOnce(); console.log('[energyRefresh] refreshed'); }
    catch (e) { console.error('[energyRefresh] failed:', e); }
  });
  // 서버 기동 직후 1회
  refreshOnce().catch(console.error);
}

module.exports = { setupEnergyCron, getCache };
