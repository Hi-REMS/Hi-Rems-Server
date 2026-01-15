const express = require('express');
const router = express.Router();
const { pool } = require('../db/db.pg');
const rateLimit = require('express-rate-limit');
const { parseFrame } = require('../energy/parser');
const { mysqlPool } = require('../db/db.mysql');
const TTL_MS = 5 * 60 * 1000;
const cache = new Map();
const { requireAuth } = require('../middlewares/requireAuth');

const snapshotCache = new Map();
const SNAPSHOT_TTL = 60 * 1000;

function getSnapshotCache(key) {
  const v = snapshotCache.get(key);
  if (v && v.exp > Date.now()) return v.data;
  if (v) snapshotCache.delete(key);
  return null;
}

function setSnapshotCache(key, data) {
  snapshotCache.set(key, { data, exp: Date.now() + SNAPSHOT_TTL });
}


const setCache = (key, data, ttl = TTL_MS) =>
  cache.set(key, { data, exp: Date.now() + ttl });

const getCache = (key) => {
  const v = cache.get(key);
  if (v && v.exp > Date.now()) return v.data;
  if (v) cache.delete(key);
  return null;
};

setInterval(() => {
  for (const [k, v] of cache.entries()) {
    if (!v || v.exp <= Date.now()) cache.delete(k);
  }
}, 15 * 60 * 1000).unref();

function normalizeSido(sido) {
  const map = {
    '강원': '강원도',
    '강원특별자치도': '강원도',
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
    '세종특별자치시': '세종'
  };
  return map[sido] || sido || '미지정';
}

async function getAuthorizedImeis(req) {
  if (req.user && req.user.is_admin) return null;

  const { sub: member_id } = req.user;
  
  const { rows: member } = await pool.query(
    'SELECT worker, "phoneNumber" FROM public.members WHERE member_id = $1',
    [member_id]
  );

  if (!member.length) return [];

  const { worker, phoneNumber } = member[0];

  const { rows: devices } = await pool.query(
    'SELECT imei FROM public.imei_meta WHERE worker = $1 AND "phoneNumber" = $2', 
    [worker, phoneNumber]
  );

  return devices.map(d => d.imei);
}

function parseKoreanAddress(addr = '') {
  const t = String(addr || '').replace(/\s*\(.*?\)\s*/g, '').trim();
  if (!t) return { sido: '미지정', sigungu: '' };
  const parts = t.split(/\s+/);
  const sidoRaw = parts[0] || '미지정';
  const sigungu = parts[1] || '';
  return { sido: normalizeSido(sidoRaw), sigungu };
}

async function fetchAddressMap(imeis) {
  const result = new Map();
  if (!imeis?.length) return result;

  try {
    const placeholders = imeis.map((_, i) => `$${i + 1}`).join(',');
    const { rows } = await pool.query(
      `SELECT imei, address, sido, sigungu
         FROM public.imei_meta
        WHERE imei IN (${placeholders})`,
      imeis
    );
    for (const r of rows) {
      result.set(r.imei, {
        address: r.address || '',
        sido: r.sido || '',
        sigungu: r.sigungu || '',
      });
    }
  } catch (_) {
  }
  return result;
}

async function computeHealthSnapshot(lookbackDays, offlineMin) {
  const { rows: latestRows } = await pool.query(
    `
    SELECT DISTINCT ON (r."rtuImei")
      r."rtuImei" AS imei,
      r."opMode"  AS op_mode,
      r."time"    AS last_time
    FROM public."log_rtureceivelog" r
    WHERE r."time" >= NOW() - make_interval(days => $1::int)
    ORDER BY r."rtuImei", r."time" DESC
    `,
    [lookbackDays]
  );

  if (!latestRows.length) {
    return { devices: [], byImei: new Map() };
  }

  const imeis = latestRows.map(r => r.imei);

  const { rows: metaRows } = await pool.query(
    `SELECT imei, energy_hex FROM public.imei_meta WHERE imei = ANY($1::text[])`,
    [imeis]
  );
  const metaMap = new Map(metaRows.map(m => [m.imei, m]));

const { rows: last1hRows } = await pool.query(
    `
    SELECT "rtuImei" AS imei, body, "time"
    FROM public."log_rtureceivelog"
    WHERE "time" >= NOW() - INTERVAL '1 hour'
      AND "rtuImei" = ANY($1::text[])
      AND body IS NOT NULL  -- [추가] 빈 껍데기 데이터(NULL)는 무시!
    ORDER BY "time" DESC
    `,
    [imeis]
  );

  const map1h = new Map();

  for (const r of last1hRows) {
    let entry = map1h.get(r.imei);
    if (!entry) {
      entry = { frames1h: 0, flagsHistory: [] };
      map1h.set(r.imei, entry);
    }

    entry.frames1h++;

    if (entry.flagsHistory.length >= 3) continue;

    try {
      const p = parseFrame(r.body);
      const m = p?.metrics || {};
      let flags = 0;

      if (typeof m.faultFlags === 'number') flags = m.faultFlags;
      else if (typeof m.statusFlags === 'number') flags = m.statusFlags;
      else if (typeof m.faultCode === 'number') flags = m.faultCode;

      entry.flagsHistory.push(flags);
    } catch (_) {}
  }

  const hourKST = new Date().getHours();
  const isNightTime = (hourKST < 8 || hourKST >= 17);

  const devices = latestRows.map(r => {
    const h = map1h.get(r.imei) || { frames1h: 0, flagsHistory: [] };
    const minutesSince = (Date.now() - new Date(r.last_time).getTime()) / 60000;
    
    const energy = metaMap.get(r.imei)?.energy_hex || null;
    const isPV = energy === '01';
    const isThermal = energy === '02';
    const isGeo = energy === '03';

    let reason = 'NORMAL';

    if (isPV && isNightTime) {
      reason = 'NORMAL';
    }

    else if ((isThermal || isGeo) && minutesSince >= 240) {
      reason = 'OFFLINE';
    }
    else {

      const recentFlags = h.flagsHistory;
      const isConsecutiveFault = 
          recentFlags.length >= 3 &&
          recentFlags[0] > 0 &&
          recentFlags[1] > 0 &&
          recentFlags[2] > 0;

      if (isConsecutiveFault) {
        reason = 'FAULT_BIT';
      } 

      else if (minutesSince >= offlineMin) {
        reason = 'OFFLINE';
      } 

      else if (r.op_mode !== '0') {
        reason = 'OPMODE_ABNORMAL';
      }
    }

    const kstDate = new Date(r.last_time);
    kstDate.setHours(kstDate.getHours() + 9); 
    const lastTimeKST = kstDate.toISOString().replace('T', ' ').substring(0, 19); 

    return {
      imei: r.imei,
      op_mode: r.op_mode,
      last_time: lastTimeKST,
      
      minutes_since: Number(minutesSince.toFixed(1)),
      frames_1h: h.frames1h,
      has_fault_1h: (h.flagsHistory[0] > 0) ? 1 : 0,
      flags_1h: h.flagsHistory,
      energy,
      reason,
    };
});

  const byImei = new Map(devices.map(d => [d.imei, d]));
  return { devices, byImei };
}

router.get('/basic', requireAuth, async (req, res, next) => {
  try {
    const lookbackDays = Math.max(parseInt(req.query.lookbackDays || '30', 10), 1);
    const offlineMin   = Math.max(parseInt(req.query.offlineMin   || '90', 10), 10);
    const noCache      = String(req.query.nocache || '') === '1';

    const memberSuffix = req.user.is_admin ? 'admin' : `user:${req.user.sub}`;
    const cacheKey = `basic:${lookbackDays}:${offlineMin}:${memberSuffix}`;
    
    if (!noCache) {
      const c = getCache(cacheKey);
      if (c) return res.json(c);
    }

    const snapKey = `${lookbackDays}:${offlineMin}`;
    let snapshot = getSnapshotCache(snapKey);

    if (!snapshot) {
      snapshot = await computeHealthSnapshot(lookbackDays, offlineMin);
      setSnapshotCache(snapKey, snapshot);
    }

    let { devices } = snapshot;

    const allowedImeis = await getAuthorizedImeis(req);
    if (allowedImeis) {
      devices = devices.filter(d => allowedImeis.includes(d.imei));
    }

    const total_plants = devices.length;
    const faultCnt   = devices.filter(d => d.reason === 'FAULT_BIT').length;
    const offlineCnt = devices.filter(d => d.reason === 'OFFLINE').length;
    const abnormal_plants = faultCnt + offlineCnt;
    const normal_plants   = total_plants - abnormal_plants;

    let todayQuery = `
      WITH bounds AS (
        SELECT
          (date_trunc('day', (now() AT TIME ZONE 'Asia/Seoul')) AT TIME ZONE 'Asia/Seoul') AS kst_start_utc,
          ((date_trunc('day', (now() AT TIME ZONE 'Asia/Seoul')) + interval '1 day') AT TIME ZONE 'Asia/Seoul') AS kst_end_utc
      )
      SELECT
        (SELECT COUNT(*)::int
           FROM public."log_rtureceivelog", bounds b
           WHERE "time" >= b.kst_start_utc AND "time" < b.kst_end_utc
           ${allowedImeis ? 'AND "rtuImei" = ANY($1)' : ''}) AS total_messages,
        (SELECT COUNT(DISTINCT "rtuImei")::int
           FROM public."log_rtureceivelog", bounds b
           WHERE "time" >= b.kst_start_utc AND "time" < b.kst_end_utc
           ${allowedImeis ? 'AND "rtuImei" = ANY($1)' : ''}) AS devices;
    `;

    const { rows: todayRows } = await pool.query(todayQuery, allowedImeis ? [allowedImeis] : []);

    const payload = {
      totals: {
        total_plants,
        normal_plants,
        abnormal_plants,
      },
      today: {
        total_messages: todayRows?.[0]?.total_messages ?? 0,
        devices:        todayRows?.[0]?.devices        ?? 0,
      },
      lookbackDays,
      offlineMin,
      isAdmin: req.user.is_admin,
      cached: false,
    };

    setCache(cacheKey, { ...payload, cached: true });
    res.json(payload);
  } catch (e) {
    next(e);
  }
});

router.get('/abnormal/list', requireAuth, async (req, res, next) => {
  try {
    const lookbackDays = Math.max(parseInt(req.query.lookbackDays || '3', 10), 1);
    const offlineMin   = Math.max(parseInt(req.query.offlineMin   || '90', 10), 10);
    const limit        = Math.min(parseInt(req.query.limit        || '50', 10), 200);
    const offset       = Math.max(parseInt(req.query.offset       || '0',  10), 0);

    const snapKey = `${lookbackDays}:${offlineMin}`;
    let snapshot = getSnapshotCache(snapKey);

    if (!snapshot) {
      snapshot = await computeHealthSnapshot(lookbackDays, offlineMin);
      setSnapshotCache(snapKey, snapshot);
    }

    let { devices } = snapshot;

    const allowedImeis = await getAuthorizedImeis(req);
    if (allowedImeis) {
      devices = devices.filter(d => allowedImeis.includes(d.imei));
    }

    const abnormal = devices
      .filter(d => d.reason !== 'NORMAL')
      .map(d => ({
        ...d,
        fault_flags: d.flags_1h || [],
        severity:
          d.reason === 'FAULT_BIT' ? 3 :
          d.reason === 'OFFLINE' ? 2 :
          d.reason === 'OPMODE_ABNORMAL' ? 1 : 0,
      }))
      .sort((a, b) => {
        if (b.severity !== a.severity) return b.severity - a.severity;
        return b.minutes_since - a.minutes_since;
      });
    
    const sliced = abnormal.slice(offset, offset + limit);
    const imeis = sliced.map(s => s.imei);

    let metaMap = new Map();
    try {
      if (imeis.length > 0) {
        const { rows: metas } = await pool.query(
          `SELECT imei, address, sido, sigungu, lat, lon,
                  energy_hex, type_hex, multi_count, worker
           FROM public.imei_meta
           WHERE imei = ANY($1::text[])`,
          [imeis]
        );
        metaMap = new Map(metas.map(m => [m.imei, m]));
      }
    } catch (_) {}

    if (mysqlPool && imeis.length > 0) {
      const lacks = imeis.filter(i => {
        const m = metaMap.get(i);
        return !m || !m.address;
      });

      if (lacks.length) {
        const sql = `
          SELECT
            rtu.rtuImei AS imei,
            COALESCE(rems.address, '') AS address,
            rems.worker AS worker
          FROM rtu_rtu AS rtu
          LEFT JOIN rems_rems AS rems
            ON rems.rtu_id = rtu.id
          WHERE rtu.rtuImei IN (${lacks.map(()=>'?').join(',')})
        `;
        const [rows] = await mysqlPool.query(sql, lacks);

        for (const r of rows) {
          const { sido, sigungu } = parseKoreanAddress(r.address || '');
          const old = metaMap.get(r.imei) || {};
          metaMap.set(r.imei, {
            ...old,
            imei: r.imei,
            address: r.address || '',
            worker: r.worker || old.worker || null,
            sido,
            sigungu,
          });
        }
      }
    }

    const UNMAPPED_MSG = '현장 설치나 매핑 작업이 아직 안 된 상태입니다. 상세 모니터링에서 검색 후 조회해주세요.';

    const enriched = sliced.map(s => {
      const m = metaMap.get(s.imei) || {};
      const hasAddress = !!(m.address && m.address.trim());

      return {
        ...s,
        address: m.address || '',
        sido: m.sido || '',
        sigungu: m.sigungu || '',
        lat: m.lat ?? null,
        lon: m.lon ?? null,
        worker: m.worker || null,
        energy: m.energy_hex ?? null,
        type: m.type_hex ?? null,
        multi: m.multi_count ?? null,
        display_message: hasAddress ? null : UNMAPPED_MSG
      };
    });

    res.json({
      items: enriched,
      limit,
      offset,
      lookbackDays,
      offlineMin,
    });

  } catch (e) {
    next(e);
  }
});

router.get('/abnormal/summary', requireAuth, async (req, res, next) => {
  try {
    const lookbackDays = Math.max(parseInt(req.query.lookbackDays || '3', 10), 1);
    const offlineMin   = Math.max(parseInt(req.query.offlineMin   || '90', 10), 10);
    const noCache      = String(req.query.nocache || '') === '1';

    const userSuffix = req.user.is_admin ? 'admin' : `user:${req.user.sub}`;
    const cacheKey = `abn-summary:${lookbackDays}:${offlineMin}:${userSuffix}`;

    if (!noCache) {
      const c = getCache(cacheKey);
      if (c) return res.json(c);
    }

    const snapKey = `${lookbackDays}:${offlineMin}`;
    let snapshot = getSnapshotCache(snapKey);

    if (!snapshot) {
      snapshot = await computeHealthSnapshot(lookbackDays, offlineMin);
      setSnapshotCache(snapKey, snapshot);
    }

    let { devices } = snapshot;

    const allowedImeis = await getAuthorizedImeis(req);
    if (allowedImeis) {
      devices = devices.filter(d => allowedImeis.includes(d.imei));
    }

    const summary = {
      FAULT_BIT:        devices.filter(d => d.reason === 'FAULT_BIT').length,
      OFFLINE:          devices.filter(d => d.reason === 'OFFLINE').length,
      OPMODE_ABNORMAL:  devices.filter(d => d.reason === 'OPMODE_ABNORMAL').length,
    };

    const payload = { summary, lookbackDays, offlineMin, cached: false };
    setCache(cacheKey, { ...payload, cached: true });
    res.json(payload);
  } catch (e) {
    next(e);
  }
});

router.get('/abnormal/by-region', requireAuth, async (req, res) => {
  try {
    const lookbackDays = Math.max(parseInt(req.query.lookbackDays || '3', 10), 1);
    const offlineMin   = Math.max(parseInt(req.query.offlineMin   || '120', 10), 10);
    const level        = (req.query.level || 'sido').toLowerCase();
    const filterSido   = req.query.sido ? req.query.sido.trim() : null;

    if (!mysqlPool) {
      return res.status(503).json({ ok: false, error: 'MySQL unavailable for region join' });
    }

    const snapKey = `${lookbackDays}:${offlineMin}`;
    let snapshot = getSnapshotCache(snapKey);

    if (!snapshot) {
      snapshot = await computeHealthSnapshot(lookbackDays, offlineMin);
      setSnapshotCache(snapKey, snapshot);
    }

    let { devices } = snapshot;

    const allowedImeis = await getAuthorizedImeis(req);
    if (allowedImeis) {
      devices = devices.filter(d => allowedImeis.includes(d.imei));
    }

    const abnormal = devices.filter(d => d.reason !== 'NORMAL');

    if (!abnormal.length) {
      return res.json({ ok: true, items: [], count: 0, level, filterSido, lookbackDays, offlineMin });
    }

    const imeis = abnormal.map(d => d.imei);
    const chunkSize = 1000;
    const addrMap = new Map();

    for (let i = 0; i < imeis.length; i += chunkSize) {
      const batch = imeis.slice(i, i + chunkSize);
      const sql = `
        SELECT rtu.rtuImei AS imei,
               rems.address AS address
          FROM rtu_rtu AS rtu
          LEFT JOIN rems_rems AS rems
                 ON rems.rtu_id = rtu.id
         WHERE rtu.rtuImei IN (${batch.map(() => '?').join(',')})
      `;
      const [metaRows] = await mysqlPool.query(sql, batch);
      for (const row of metaRows) {
        const { sido, sigungu } = parseKoreanAddress(row.address);
        addrMap.set(row.imei, { sido: normalizeSido(sido), sigungu });
      }
    }

    const regionAgg = new Map();
    const norm = (s) => (s || '').replace(/\s+/g, '').replace(/도|시|군|구|특별자치시|광역시/g, '');

    for (const d of abnormal) {
      const meta = addrMap.get(d.imei) || {};
      const sido = normalizeSido(meta.sido || '미지정');
      const sigungu = meta.sigungu || '';

      if (filterSido && norm(sido) !== norm(normalizeSido(filterSido))) continue;

      const key = level === 'sido' ? `${sido}|` : `${sido}|${sigungu}`;
      const cur = regionAgg.get(key) || {
        sido,
        sigungu,
        OFFLINE: 0,
        OPMODE_ABNORMAL: 0,
        FAULT_BIT: 0,
        total: 0,
      };

      if (d.reason === 'FAULT_BIT') cur.FAULT_BIT++;
      else if (d.reason === 'OFFLINE') cur.OFFLINE++;
      else if (d.reason === 'OPMODE_ABNORMAL') cur.OPMODE_ABNORMAL++;

      cur.total++;
      regionAgg.set(key, cur);
    }

    const items = [...regionAgg.values()];
    res.json({ ok: true, level, filterSido, lookbackDays, offlineMin, count: items.length, items });
  } catch (e) {
    console.error('/abnormal/by-region error:', e);
    res.status(500).json({ ok: false, error: e.message });
  }
});


router.get('/energy', requireAuth, async (req, res, next) => {
  try {
    const { getEnergySummary } = require('../energy/summary');
    const imeiList = await getAuthorizedImeis(req);

    console.log(`\n=== [Energy API Debug Start] ===`);
    console.log(`- User ID: ${req.user.sub}`);
    console.log(`- Device Count: ${imeiList ? imeiList.length : 'ALL (Admin)'}`);
    if (imeiList) console.log(`- IMEIs: ${JSON.stringify(imeiList)}`);

    const summary = await getEnergySummary(imeiList);

    console.log(`- [FINAL RESULT] Electric:`, summary.electric);
    console.log(`- [FINAL RESULT] Thermal:`, summary.thermal);
    console.log(`=== [Energy API Debug End] ===\n`);

    res.json({
      ok: true,
      data: summary
    });

  } catch (e) {
    console.error(' /energy API 에러:', e);
    next(e);
  }
});

router.get('/abnormal/points', requireAuth, async (req, res, next) => {
  try {
    const lookbackDays = Math.max(parseInt(req.query.lookbackDays || '30', 10), 1);
    const offlineMin   = Math.max(parseInt(req.query.offlineMin   || '90',  10), 10);
    const reasonFilter = String(req.query.reason || 'ALL').toUpperCase();
    const filterSido   = (req.query.sido    || '').trim();
    const filterSigungu= (req.query.sigungu || '').trim();

    const snapKey = `${lookbackDays}:${offlineMin}`;
    let snapshot = getSnapshotCache(snapKey);

    if (!snapshot) {
      snapshot = await computeHealthSnapshot(lookbackDays, offlineMin);
      setSnapshotCache(snapKey, snapshot);
    }

    let { devices } = snapshot;

    const allowedImeis = await getAuthorizedImeis(req);
    if (allowedImeis) {
      devices = devices.filter(d => allowedImeis.includes(d.imei));
    }

    let rows = devices.filter(d => d.reason !== 'NORMAL');
    if (reasonFilter !== 'ALL') {
      rows = rows.filter(d => d.reason === reasonFilter);
    }

    if (!rows.length) return res.json({ ok: true, items: [] });

    const imeis = rows.map(r => r.imei);

    let metaMap = new Map();
    try {
      const { rows: metas } = await pool.query(
        `SELECT imei, address, sido, sigungu, lat, lon,
                energy_hex, type_hex, multi_count, worker
         FROM public.imei_meta
         WHERE imei = ANY($1::text[])`,
        [imeis]
      );
      metaMap = new Map(metas.map(m => [m.imei, m]));
    } catch (_) {}

    if (mysqlPool) {
      const lacks = rows.filter(r => {
        const meta = metaMap.get(r.imei);
        return !meta || !meta.address;
      });

      const CHUNK = 500;
      for (let i = 0; i < lacks.length; i += CHUNK) {
        const batchImeis = lacks.slice(i, i + CHUNK).map(r => r.imei);
        if (!batchImeis.length) break;

        const sql = `
          SELECT
            COALESCE(rtu.rtuImei, rems.rtu_id) AS imei,
            COALESCE(rems.address, '') AS address,
            rems.worker AS worker
          FROM rems_rems AS rems
          LEFT JOIN rtu_rtu AS rtu ON rtu.id = rems.rtu_id
          WHERE rtu.rtuImei IN (${batchImeis.map(()=>'?').join(',')})
             OR rems.rtu_id  IN (${batchImeis.map(()=>'?').join(',')})
        `;
        const [mrows] = await mysqlPool.query(sql, [...batchImeis, ...batchImeis]);

        for (const m of mrows) {
          const { sido, sigungu } = parseKoreanAddress(m.address || '');
          const existing = metaMap.get(m.imei) || {};
          metaMap.set(m.imei, {
            ...existing,
            imei: m.imei,
            address: m.address || '',
            worker: m.worker || existing.worker || null,
            sido, sigungu,
            lat: existing.lat ?? null,
            lon: existing.lon ?? null,
            energy_hex: existing.energy_hex ?? null,
            type_hex: existing.type_hex ?? null,
            multi_count: existing.multi_count ?? null,
          });
        }
      }
    }

    const norm = s => (s || '').replace(/\s+/g, '').replace(/도|시|군|구|특별자치시|광역시/g, '');
    const wantSido = filterSido ? norm(normalizeSido(filterSido)) : null;
    const wantSigun = filterSigungu ? norm(filterSigungu) : null;

    const items = [];
    for (const r of rows) {
      const meta = metaMap.get(r.imei) || {};
      const sido = normalizeSido(meta.sido || '');
      const sigungu = meta.sigungu || '';

      if (wantSido && norm(sido) !== wantSido) continue;
      if (wantSigun && norm(sigungu) !== wantSigun) continue;

      items.push({
        imei: r.imei,
        reason: r.reason,
        op_mode: r.op_mode,
        last_time: r.last_time,
        minutes_since: r.minutes_since,
        sido,
        sigungu,
        address: meta.address || '',
        lat: meta.lat ?? null,
        lon: meta.lon ?? null,
        worker: meta.worker || null,
        energy: meta.energy_hex ?? null,
        type: meta.type_hex ?? null,
        multi: meta.multi_count ?? null
      });
    }

    res.json({ ok: true, items });
  } catch (e) {
    next(e);
  }
});


router.get('/normal/points', requireAuth, async (req, res, next) => {
  try {
    const lookbackDays = Math.max(parseInt(req.query.lookbackDays || '3', 10), 1);
    const offlineMin   = Math.max(parseInt(req.query.offlineMin   || '90', 10), 10);

    const snapshot = await computeHealthSnapshot(lookbackDays, offlineMin);
    let { devices } = snapshot;

    const allowedImeis = await getAuthorizedImeis(req);
    if (allowedImeis) {
      devices = devices.filter(d => allowedImeis.includes(d.imei));
    }

    const normalDevices = devices.filter(d => d.reason === 'NORMAL');

    if (!normalDevices.length) {
      return res.json({ ok: true, items: [] });
    }

    const imeis = normalDevices.map(d => d.imei);

    const { rows: metas } = await pool.query(
      `SELECT imei, address, sido, sigungu, lat, lon,
              energy_hex, type_hex, multi_count, worker
       FROM public.imei_meta
       WHERE imei = ANY($1::text[])`,
      [imeis]
    );

    const metaMap = new Map(metas.map(m => [m.imei, m]));

    const items = normalDevices.map(d => {
      const m = metaMap.get(d.imei) || {};
      return {
        ...d,
        sido: m.sido || '',
        sigungu: m.sigungu || '',
        address: m.address || '',
        lat: m.lat || null,
        lon: m.lon || null,
        worker: m.worker || null,
        energy: m.energy_hex || null,
        type: m.type_hex || null,
        multi: m.multi_count || null
      };
    });

    const filteredItems = items.filter(it => it.lat && it.lon);

    res.json({ 
      ok: true, 
      items: filteredItems,
      count: filteredItems.length
    });

  } catch (e) {
    console.error('/normal/points error:', e);
    next(e);
  }
});

router.get('/debug/db-time', async (req, res) => {
  const result = {
    server_time: new Date().toString(),
    postgres: null,
    mysql: null,
  };

  try {
    const { rows } = await pool.query(`
      SELECT NOW() as raw_time, 
             TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') as fmt_time,
             current_setting('TIMEZONE') as timezone
    `);
    result.postgres = rows[0];
  } catch (e) {
    result.postgres = { error: e.message };
  }

  if (mysqlPool) {
    try {
      const [rows] = await mysqlPool.query(`
        SELECT NOW() as raw_time, 
               @@global.time_zone as global_tz, 
               @@session.time_zone as session_tz
      `);
      result.mysql = rows[0];
    } catch (e) {
      result.mysql = { error: e.message };
    }
  } else {
    result.mysql = { status: 'MySQL Pool is not configured' };
  }

  res.json(result);
});

module.exports = router;