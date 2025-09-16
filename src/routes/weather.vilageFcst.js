// src/routes/weather.vilageFcst.js
const express = require('express');
const router = express.Router();

const http = require('http');
const https = require('https');
const axiosBase = require('axios');
const LRU = require('lru-cache');
const qs = require('qs'); // KMA serviceKey 재인코딩 방지용

const { dfs_xy_conv } = require('../utils/kmaGrid');

const { pool } = require('../db/db.pg');
const { mysqlPool } = require('../db/db.mysql');

/* ================================================================
 * Axios (keep-alive)
 * ================================================================ */
const axios = axiosBase.create({
  timeout: 15000,
  validateStatus: () => true,
  httpAgent: new http.Agent({ keepAlive: true }),
  httpsAgent: new https.Agent({ keepAlive: true }),
});

/* ================================================================
 * 캐시 & in-flight 결합
 * ================================================================ */
// IMEI → CID (5분)
const imeiCidCache = new LRU({ max: 2000, ttl: 5 * 60 * 1000 });
// CID → Address (10분)
const cidAddrCache = new LRU({ max: 2000, ttl: 10 * 60 * 1000 });
// 주소 → 좌표 (1시간)
const geocache = new LRU({ max: 1000, ttl: 60 * 60 * 1000 });
// 주소/IMEI 해석 체인 전체를 묶은 메타 캐시 (하루): imei -> {address, lat, lon, nx, ny}
const imeiMetaCache = new LRU({ max: 5000, ttl: 24 * 60 * 60 * 1000 });

// KMA 캐시 (base 기준) : (base_date,base_time,nx,ny)
const kmaCache = new LRU({ max: 4000, ttl: 15 * 60 * 1000 });
// KMA 최신 스냅샷 (그리드 기준) : nx,ny → 가장 최근 성공 결과(슬롯 상관없이 6시간)
const kmaLatestByGrid = new LRU({ max: 5000, ttl: 6 * 60 * 60 * 1000 });

// 최종 응답 캐시(≈90초)
const responseCache = new LRU({ max: 2000, ttl: 90 * 1000 });

// in-flight 결합
const inflight = new Map(); // key -> Promise
function withInflight(key, producer) {
  const ex = inflight.get(key);
  if (ex) return ex;
  const p = (async () => {
    try { return await producer(); }
    finally { inflight.delete(key); }
  })();
  inflight.set(key, p);
  return p;
}

/* ================================================================
 * DB helpers (+ cached)
 * ================================================================ */
async function getLatestCidByImeiRaw(pgPool, imei) {
  const { rows } = await pgPool.query(
    `SELECT "cid"
       FROM public.log_remssendlog
      WHERE "rtuImei" = $1
      ORDER BY "time" DESC
      LIMIT 1`,
    [imei]
  );
  return rows.length ? rows[0].cid : null;
}
async function getLatestCidByImeiCached(pgPool, imei) {
  const key = `imeiCid:${imei}`;
  const cached = imeiCidCache.get(key);
  if (cached) return cached;
  const cid = await withInflight(key, () => getLatestCidByImeiRaw(pgPool, imei));
  if (cid) imeiCidCache.set(key, cid);
  return cid;
}

async function getLatestAddressByCidRaw(mysqlPool, cid) {
  const [rows] = await mysqlPool.query(
    `SELECT address, createdDate
       FROM alliothub.rems_rems
      WHERE cid = ?
      ORDER BY createdDate DESC
      LIMIT 1`,
    [cid]
  );
  return rows.length ? rows[0] : null;
}
async function getLatestAddressByCidCached(mysqlPool, cid) {
  const key = `cidAddr:${cid}`;
  const cached = cidAddrCache.get(key);
  if (cached) return cached;
  const row = await withInflight(key, () => getLatestAddressByCidRaw(mysqlPool, cid));
  if (row) cidAddrCache.set(key, row);
  return row;
}

/* ================================================================
 * 시간/발표 시각 유틸
 * ================================================================ */
function fmtHour(hhmm = '') {
  const s = String(hhmm || '').trim();
  if (!/^\d{4}$/.test(s)) return null;
  return `${s.slice(0, 2)}:00`;
}
const SLOTS = [2, 5, 8, 11, 14, 17, 20, 23];

function pickBase() {
  const now = new Date();
  const kst = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
  const yyyy = kst.getFullYear();
  const mm = String(kst.getMonth() + 1).padStart(2, '0');
  const dd = String(kst.getDate()).padStart(2, '0');
  const hh = kst.getHours();

  let baseDate = `${yyyy}${mm}${dd}`;
  let slot = SLOTS[0];
  for (const s of SLOTS) if (hh >= s) slot = s;

  if (hh < 2) {
    const prev = new Date(kst.getTime() - 24 * 60 * 60 * 1000);
    const y = prev.getFullYear();
    const m = String(prev.getMonth() + 1).padStart(2, '0');
    const d = String(prev.getDate()).padStart(2, '0');
    baseDate = `${y}${m}${d}`;
    slot = 23;
  }
  return { base_date: baseDate, base_time: String(slot).padStart(2, '0') + '00' };
}
function prevBase(base_date, base_time) {
  const hh = parseInt(String(base_time).slice(0, 2), 10);
  const idx = SLOTS.indexOf(hh);
  if (idx > 0) {
    return { base_date, base_time: String(SLOTS[idx - 1]).padStart(2, '0') + '00' };
  }
  const y = parseInt(base_date.slice(0, 4), 10);
  const m = parseInt(base_date.slice(4, 6), 10) - 1;
  const d = parseInt(base_date.slice(6, 8), 10);
  const dt = new Date(y, m, d);
  dt.setDate(dt.getDate() - 1);
  const ny = dt.getFullYear();
  const nm = String(dt.getMonth() + 1).padStart(2, '0');
  const nd = String(dt.getDate()).padStart(2, '0');
  return { base_date: `${ny}${nm}${nd}`, base_time: '2300' };
}
function ttlUntilNextSlotKst(base_date, base_time) {
  const y = parseInt(base_date.slice(0, 4), 10);
  const m = parseInt(base_date.slice(4, 6), 10) - 1;
  const d = parseInt(base_date.slice(6, 8), 10);
  const hh = parseInt(base_time.slice(0, 2), 10);
  const idx = SLOTS.indexOf(hh);
  const nextH = (idx >= 0 && idx < SLOTS.length - 1) ? SLOTS[idx + 1] : null;

  const now = new Date();
  const nowKst = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
  let next = null;
  if (nextH !== null) {
    next = new Date(new Date(Date.UTC(y, m, d)).toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
    next.setFullYear(y); next.setMonth(m); next.setDate(d); next.setHours(nextH, 0, 0, 0);
  } else {
    const dt = new Date(new Date(Date.UTC(y, m, d)).toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
    dt.setFullYear(y); dt.setMonth(m); dt.setDate(d); dt.setHours(24 + SLOTS[0], 0, 0, 0);
    next = dt;
  }
  const diff = Math.max(30 * 1000, next - nowKst);
  return Math.min(diff, 3 * 60 * 60 * 1000);
}

/* ================================================================
 * 키 유틸
 * ================================================================ */
function normalizeMaybeEncoded(raw) {
  if (!raw) return raw;
  try {
    return raw.includes('%') ? decodeURIComponent(raw) : raw;
  } catch { return raw; }
}

/* ================================================================
 * KMA 호출 (+ 캐시)
 * ================================================================ */
async function callKmaOnceRaw({ base_date, base_time, nx, ny }) {
  const url = 'https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst';
  const resp = await axios.get(url, {
    params: {
      serviceKey: normalizeMaybeEncoded(process.env.KMA_VILAGE_KEY),
      pageNo: 1,
      numOfRows: 1000,
      dataType: 'JSON',
      base_date,
      base_time,
      nx, ny,
    },
    paramsSerializer: (params) => qs.stringify(params, { encode: false }),
    timeout: 12000,
    validateStatus: () => true,
  });

  const data = resp.data;
  const header = data?.response?.header || null;
  const body = data?.response?.body || null;
  const items = body?.items?.item || [];

  return {
    status: resp.status,
    header,
    bodyInfo: body ? { totalCount: body.totalCount, pageNo: body.pageNo, numOfRows: body.numOfRows } : null,
    items: Array.isArray(items) ? items : [],
  };
}
async function callKmaOnceCached({ base_date, base_time, nx, ny }) {
  const key = `kma:${base_date}:${base_time}:${nx}:${ny}`;
  const cached = kmaCache.get(key);
  if (cached) return cached;

  const data = await withInflight(key, () => callKmaOnceRaw({ base_date, base_time, nx, ny }));
  const ttl = ttlUntilNextSlotKst(base_date, base_time);
  kmaCache.set(key, data, { ttl });
  if (data?.items?.length) {
    // 최신 스냅샷 갱신
    kmaLatestByGrid.set(`grid:${nx}:${ny}`, {
      base_date, base_time, nx, ny, items: data.items, status: data.status, header: data.header
    });
  }
  return data;
}

/* ================================================================
 * Geocoding (Kakao 1순위 → (선택) Naver → (선택) vWorld)
 * ================================================================ */
async function geocodeByKakao(address) {
  if (!address) return { ok: false, reason: 'NO_ADDRESS' };
  try {
    const resp = await axios.get('https://dapi.kakao.com/v2/local/search/address.json', {
      params: { query: address },
      headers: { Authorization: `KakaoAK ${process.env.KAKAO_REST_KEY}` },
      timeout: 10000, validateStatus: () => true,
    });
    const doc = resp.data?.documents?.[0];
    const lat = doc ? Number(doc.y) : null;
    const lon = doc ? Number(doc.x) : null;
    if (Number.isFinite(lat) && Number.isFinite(lon)) {
      return { ok: true, lat, lon, source: 'kakao:address', http: resp.status };
    }
    return { ok: false, http: resp.status, raw: resp.data };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}
async function geocodeByNaver(address) {
  if (!address) return { ok: false, reason: 'NO_ADDRESS' };
  const cid = process.env.NAVER_CLIENT_ID;
  const secret = process.env.NAVER_CLIENT_SECRET;
  if (!cid || !secret) return { ok: false, reason: 'NAVER_NOT_CONFIGURED' };
  try {
    const resp = await axios.get('https://naveropenapi.apigw.ntruss.com/map-geocode/v2/geocode', {
      params: { query: address },
      headers: { 'X-NCP-APIGW-API-KEY-ID': cid, 'X-NCP-APIGW-API-KEY': secret },
      timeout: 10000, validateStatus: () => true,
    });
    const a = resp.data?.addresses?.[0];
    const lat = a ? Number(a.y) : null;
    const lon = a ? Number(a.x) : null;
    if (Number.isFinite(lat) && Number.isFinite(lon)) {
      return { ok: true, lat, lon, source: 'naver:geocode', http: resp.status };
    }
    return { ok: false, http: resp.status, raw: resp.data };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}
async function geocodeByVWorldSearch(address) {
  if (!address) return { ok: false, reason: 'NO_ADDRESS' };
  const key = normalizeMaybeEncoded(process.env.VWORLD_API_KEY);
  if (!key) return { ok: false, reason: 'VWORLD_NOT_CONFIGURED' };
  try {
    const resp = await axios.get('http://api.vworld.kr/req/search', {
      params: {
        service:'search', request:'search', version:'2.0', crs:'epsg:4326',
        format:'json', key, query:address, category:'address', size:1
      },
      timeout: 10000, validateStatus: () => true,
    });
    const item = resp.data?.response?.result?.items?.[0];
    const lon = item?.point?.x, lat = item?.point?.y;
    if (Number.isFinite(Number(lat)) && Number.isFinite(Number(lon))) {
      return { ok: true, lat: Number(lat), lon: Number(lon), source: 'vworld:search', http: resp.status };
    }
    return { ok: false, http: resp.status, raw: resp.data };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}
async function geocodeAddress(address) {
  if (!address) return { ok: false, reason: 'NO_ADDRESS' };
  const ck = `geo:${address}`;
  const cached = geocache.get(ck);
  if (cached) return { ok: true, ...cached, from: 'cache' };

  let g = await geocodeByKakao(address);
  if (g?.ok) { const out = { lat:g.lat, lon:g.lon, source:g.source }; geocache.set(ck, out); return { ok:true, ...out, providerDebug:g }; }
  g = await geocodeByNaver(address);
  if (g?.ok) { const out = { lat:g.lat, lon:g.lon, source:g.source }; geocache.set(ck, out); return { ok:true, ...out, providerDebug:g }; }
  g = await geocodeByVWorldSearch(address);
  if (g?.ok) { const out = { lat:g.lat, lon:g.lon, source:g.source }; geocache.set(ck, out); return { ok:true, ...out, providerDebug:g }; }

  return { ok:false, reason:'ALL_PROVIDERS_FAILED', providerDebug:g };
}

/* ================================================================
 * IMEI → 주소/좌표/그리드 “체인 메타” (긴 TTL 캐시)
 * ================================================================ */
async function resolveImeiMeta({ imei, forcedAddress, forceGeo }) {
  const metaKey = `imeiMeta:${imei}${forcedAddress ? `:${forcedAddress}` : ''}${forceGeo ? `:${forceGeo.lat},${forceGeo.lon}` : ''}`;
  const cached = imeiMetaCache.get(metaKey);
  if (cached) return { ...cached, _from: 'metaCache' };

  // 1) IMEI → cid
  const cid = await getLatestCidByImeiCached(pool, imei);
  if (!cid) return { imei, found:false, reason:'no cid for imei' };

  // 2) cid → address (또는 강제 주소)
  const addrRow = forcedAddress ? null : await getLatestAddressByCidCached(mysqlPool, cid);
  const address = forcedAddress || addrRow?.address?.trim?.() || null;

  // 3) address → 좌표
  const FALLBACK = { lat: 35.335, lon: 129.037, source: 'fallback' };
  let geo = FALLBACK, geocodeDebug = null;
  if (forceGeo) {
    geocodeDebug = { ok: true, source: 'query', note: 'lat/lon from query' };
    geo = forceGeo;
  } else {
    const g = await geocodeAddress(address);
    geocodeDebug = g;
    if (g?.ok && g.lat && g.lon) geo = { lat:g.lat, lon:g.lon, source:g.source };
  }

  // 4) 좌표 → 그리드
  const { nx, ny } = dfs_xy_conv('toXY', geo.lat, geo.lon);

  const meta = { imei, cid, address, geo, nx, ny, geocodeDebug };
  // 하루 TTL: 주소/설치 위치가 자주 바뀌지 않는다는 가정
  imeiMetaCache.set(metaKey, meta);
  return meta;
}

/* ================================================================
 * 라우트
 * ================================================================ */
router.get('/by-imei', async (req, res, next) => {
  try {
    const imei = req.query.imei;
    if (!imei) return res.status(400).json({ error: 'imei 파라미터 필요' });

    const forcedAddress = (req.query.address || '').trim();
    const qLat = parseFloat(req.query.lat);
    const qLon = parseFloat(req.query.lon);
    const forceGeo = (Number.isFinite(qLat) && Number.isFinite(qLon)) ? { lat:qLat, lon:qLon, source:'query' } : null;

    // 최종 응답 캐시
    const respKey = `resp:${imei}:${forcedAddress || ''}:${forceGeo ? `${qLat},${qLon}` : ''}`;
    const cachedResp = responseCache.get(respKey);
    if (cachedResp) {
      res.set('Cache-Control', 'public, max-age=60, stale-while-revalidate=60');
      return res.json(cachedResp);
    }

    // ===== IMEI 체인 메타 (긴 TTL) : 주소/좌표/그리드 빠르게 확보
    const meta = await resolveImeiMeta({ imei, forcedAddress, forceGeo });
    if (meta.found === false) {
      const out = { imei, found:false, reason: meta.reason };
      responseCache.set(respKey, out);
      res.set('Cache-Control', 'public, max-age=60, stale-while-revalidate=60');
      return res.json(out);
    }
    const { cid, address, geo, nx, ny, geocodeDebug } = meta;

    // ===== KMA: 신선 데이터 vs 오래된 스냅샷 즉시 응답 =====
    let { base_date, base_time } = pickBase();

    const gridKey = `grid:${nx}:${ny}`;
    const staleSnap = kmaLatestByGrid.get(gridKey); // 과거 성공 스냅샷(있을 수도, 없을 수도)

    const FAST_BUDGET_MS = 450; // 새 데이터가 이 시간 내에 안오면 바로 stale 응답

    // 새 데이터 (slot 3회 폴백 포함)
    const fetchFresh = (async () => {
      let tried = [];
      let fresh = null;
      let bd = base_date, bt = base_time;

      for (let i = 0; i < 3; i++) {
        const kma = await callKmaOnceCached({ base_date: bd, base_time: bt, nx, ny });
        tried.push({
          base_date: bd, base_time: bt,
          status: kma.status, resultCode: kma.header?.resultCode,
          total: kma.bodyInfo?.totalCount,
        });
        if (kma.status !== 200) break;
        if (Array.isArray(kma.items) && kma.items.length > 0) { fresh = { bd, bt, kma, tried }; break; }
        const prev = prevBase(bd, bt); bd = prev.base_date; bt = prev.base_time;
      }
      return fresh || { tried };
    })();

    // 새 데이터 vs 타임박스
    const timer = new Promise(resolve => setTimeout(() => resolve({ timeout:true }), FAST_BUDGET_MS));
    const raced = await Promise.race([ fetchFresh, timer ]);

    let hourlyRows = [];
    let tried = [];
    let usedBase = { base_date, base_time };
    let isStale = false;

    if (raced && raced.timeout && staleSnap) {
      // ⏱ 타임박스 초과 → stale 스냅샷 즉시 응답
      const items = staleSnap.items || [];
      const hourlyMap = {};
      for (const it of items) {
        const key = it.fcstTime; if (!key) continue;
        const hourLabel = fmtHour(key); if (!hourLabel) continue;
        if (!hourlyMap[key]) hourlyMap[key] = { hour: hourLabel, TA: null, SKY: null, PTY: null };
        if (it.category === 'TMP') hourlyMap[key].TA = Number(it.fcstValue);
        if (it.category === 'SKY') hourlyMap[key].SKY = String(it.fcstValue);
        if (it.category === 'PTY') hourlyMap[key].PTY = String(it.fcstValue);
      }
      hourlyRows = Object.values(hourlyMap).sort((a,b)=>a.hour.localeCompare(b.hour));
      usedBase = { base_date: staleSnap.base_date, base_time: staleSnap.base_time };
      tried = [{ base_date: usedBase.base_date, base_time: usedBase.base_time, status: 200, resultCode: 'STALE', total: items.length }];
      isStale = true;
    } else if (raced && raced.kma) {
      // ✅ 신선 데이터 도착
      const items = raced.kma.items || [];
      const hourlyMap = {};
      for (const it of items) {
        const key = it.fcstTime; if (!key) continue;
        const hourLabel = fmtHour(key); if (!hourLabel) continue;
        if (!hourlyMap[key]) hourlyMap[key] = { hour: hourLabel, TA: null, SKY: null, PTY: null };
        if (it.category === 'TMP') hourlyMap[key].TA = Number(it.fcstValue);
        if (it.category === 'SKY') hourlyMap[key].SKY = String(it.fcstValue);
        if (it.category === 'PTY') hourlyMap[key].PTY = String(it.fcstValue);
      }
      hourlyRows = Object.values(hourlyMap).sort((a,b)=>a.hour.localeCompare(b.hour));
      usedBase = { base_date: raced.bd, base_time: raced.bt };
      tried = raced.tried || [];
      isStale = false;
    } else {
      // 타임박스 초과 + 스테일 없음 → 신선 데이터 끝까지 기다림
      const finalFresh = await fetchFresh;
      const items = finalFresh?.kma?.items || [];
      const hourlyMap = {};
      for (const it of items) {
        const key = it.fcstTime; if (!key) continue;
        const hourLabel = fmtHour(key); if (!hourLabel) continue;
        if (!hourlyMap[key]) hourlyMap[key] = { hour: hourLabel, TA: null, SKY: null, PTY: null };
        if (it.category === 'TMP') hourlyMap[key].TA = Number(it.fcstValue);
        if (it.category === 'SKY') hourlyMap[key].SKY = String(it.fcstValue);
        if (it.category === 'PTY') hourlyMap[key].PTY = String(it.fcstValue);
      }
      hourlyRows = Object.values(hourlyMap).sort((a,b)=>a.hour.localeCompare(b.hour));
      usedBase = { base_date: finalFresh?.bd || base_date, base_time: finalFresh?.bt || base_time };
      tried = finalFresh?.tried || [];
      isStale = false;
    }

    // 최종 응답
    const out = {
      imei, cid, address, nx, ny,
      base_date: usedBase.base_date,
      base_time: usedBase.base_time,
      hourly: hourlyRows,
      stale: isStale || undefined,
      debug: {
        geo, geocode: geocodeDebug, tried,
      },
    };

    responseCache.set(respKey, out);
    res.set('Cache-Control', 'public, max-age=60, stale-while-revalidate=60');
    return res.json(out);
  } catch (err) {
    next(err);
  }
});

module.exports = router;
