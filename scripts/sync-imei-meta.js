// scripts/sync-imei-meta.js
require('dotenv').config();

const axios = require('axios').create({
  timeout: 12000,
  validateStatus: () => true,
});

const { pool } = require('../src/db/db.pg');
const { mysqlPool } = require('../src/db/db.mysql');

function normalizeSido(sido) {
  const map = {
    '강원특별자치도': '강원도',
    '강원': '강원도',
    '제주특별자치도': '제주도',
    '경남': '경상남도',
    '경북': '경상북도',
    '전남': '전라남도',
    '전북': '전라북도',
    '충남': '충청남도',
    '충북': '충청북도',
    '서울특별시': '서울',
    '부산광역시': '부산',
    '대구광역시': '대구',
    '인천광역시': '인천',
    '광주광역시': '광주',
    '대전광역시': '대전',
    '울산광역시': '울산',
    '세종특별자치시': '세종',
  };
  return map[sido] || sido || '미지정';
}

function parseKoreanAddress(addr = '') {
  const t = String(addr || '').replace(/\s*\(.*?\)\s*/g, '').trim();
  if (!t) return { sido: '미지정', sigungu: '' };
  const [sidoRaw = '미지정', sigungu = ''] = t.split(/\s+/);
  return { sido: normalizeSido(sidoRaw), sigungu };
}

async function geocodeViaLocalProxy(address) {
  const url = `http://localhost:3000/api/rems/geocode`;
  const resp = await axios.get(url, { params: { query: address } });
  const doc = resp.data?.results?.[0];
  if (!doc) return { lat: null, lon: null };
  return { lat: Number(doc.y) || null, lon: Number(doc.x) || null };
}

async function geocodeViaKakao(address) {
  const KAKAO_REST_KEY = process.env.KAKAO_REST_KEY || '';
  if (!KAKAO_REST_KEY) return { lat: null, lon: null };

  const resp = await axios.get('https://dapi.kakao.com/v2/local/search/address.json', {
    headers: { Authorization: `KakaoAK ${KAKAO_REST_KEY}` },
    params: { query: address },
  });
  const doc = resp.data?.documents?.[0];
  if (!doc) return { lat: null, lon: null };
  return { lat: Number(doc.y) || null, lon: Number(doc.x) || null };
}

async function upsertImeiMeta(pgPool, row) {
  const q = `
    INSERT INTO public.imei_meta(imei, address, sido, sigungu, lat, lon, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, now())
    ON CONFLICT (imei) DO UPDATE SET
      address = EXCLUDED.address,
      sido = EXCLUDED.sido,
      sigungu = EXCLUDED.sigungu,
      lat = COALESCE(EXCLUDED.lat, public.imei_meta.lat),
      lon = COALESCE(EXCLUDED.lon, public.imei_meta.lon),
      updated_at = now()
  `;
  const args = [row.imei, row.address, row.sido, row.sigungu, row.lat, row.lon];
  await pgPool.query(q, args);
}

(async () => {
  try {
    console.log('▶ IMEI 메타 동기화 시작');

    const sql = `
      SELECT
        rtu.rtuImei AS imei,
        COALESCE(rems.address, '') AS address
      FROM rtu_rtu AS rtu
      LEFT JOIN rems_rems AS rems
        ON rems.rtu_id = rtu.id
      WHERE COALESCE(rems.address, '') <> ''
    `;
    const [rows] = await mysqlPool.query(sql);
    console.log(`  - MySQL에서 주소 보유 IMEI ${rows.length}건`);

    const existRes = await pool.query(`SELECT imei, lat, lon FROM public.imei_meta`);
    const exists = new Map(existRes.rows.map(r => [r.imei, r]));

    const batchSize = 200;
    const concurrency = 5;
    const useKakaoDirect = !!process.env.KAKAO_REST_KEY;
    let processed = 0;
    let geocoded = 0;

    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);

      const queue = [];
      for (const src of batch) {
        queue.push((async () => {
          const imei = String(src.imei || '').trim();
          const address = String(src.address || '').trim();
          if (!imei || !address) return;

          const meta = exists.get(imei);
          let { sido, sigungu } = parseKoreanAddress(address);
          let lat = meta?.lat ?? null;
          let lon = meta?.lon ?? null;

          if (lat == null || lon == null) {
            try {
              const p = useKakaoDirect
                ? await geocodeViaKakao(address)
                : await geocodeViaLocalProxy(address);
              lat = p.lat;
              lon = p.lon;
              if (lat != null && lon != null) geocoded++;
              await new Promise(r => setTimeout(r, 120));
            } catch (err) {
            }
          }

          await upsertImeiMeta(pool, { imei, address, sido, sigungu, lat, lon });
          processed++;
        })());

        if (queue.length >= concurrency) {
          await Promise.race(queue);
          for (let k = queue.length - 1; k >= 0; k--) {
            if (queue[k].isFulfilled || queue[k].isRejected) queue.splice(k, 1);
          }
        }
      }
      await Promise.allSettled(queue);
      console.log(`  - 진행 ${Math.min(i + batch.length, rows.length)}/${rows.length} (누적 UPSERT ${processed}, 지오코딩 ${geocoded})`);
    }

    console.log('✅ 완료:', { processed, geocoded });
    process.exit(0);
  } catch (e) {
    console.error('❌ 실패:', e);
    process.exit(1);
  }
})();
