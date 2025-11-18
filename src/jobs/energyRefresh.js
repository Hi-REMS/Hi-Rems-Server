/**
 * energyRefresh.js
 * ─────────────────────────────────────────────────────────────
 * 전국 에너지 요약 캐시 자동 갱신 작업
 * - node-cron을 이용해 1분마다 전국 전기/열 에너지 데이터를 재계산
 * - 최초 서버 부팅 시 1회 즉시 수행
 * - 계산된 결과는 memoryCache에 저장하며, 필요 시 DB에도 영속화
 *
 * 주요 기능
 * - getElectricNationwideSummary / getThermalNationwideSummary 호출로 집계 수행
 * - memoryCache: API 응답 시 빠른 반환용 메모리 캐시
 * - energy_nationwide_cache (DB): 재시작 후에도 유지 가능한 선택적 캐시 테이블
 *
 * 연동 관계
 * - routes/dashboard.js → /dashboard/energy API에서 getCache()로 조회
 * - app.js → setupEnergyCron()을 서버 시작 시 등록
 *
 * 스케줄
 * - 1분마다 자동 갱신 -> 66번 라인 cron.schedule
 */

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
  // 매 1분 실행
  cron.schedule('*/1 * * * *', async () => {
    try { await refreshOnce(); console.log('[energyRefresh] refreshed'); }
    catch (e) { console.error('[energyRefresh] failed:', e); }
  });
  refreshOnce().catch(console.error);
}

module.exports = { setupEnergyCron, getCache };
