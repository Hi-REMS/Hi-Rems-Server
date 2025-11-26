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
  cron.schedule('*/1 * * * *', async () => {
    try { await refreshOnce(); console.log('[energyRefresh] refreshed'); }
    catch (e) { console.error('[energyRefresh] failed:', e); }
  });
  refreshOnce().catch(console.error);
}

module.exports = { setupEnergyCron, getCache };
