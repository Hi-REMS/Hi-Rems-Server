// src/routes/rems.js
// REMS 데이터 조회 및 집계 API (MySQL)
// + 카카오 JS 키 전달 / 지오코딩 프록시 포함

const express = require('express');
const router = express.Router();
const axios = require('axios');
const rateLimit = require('express-rate-limit');
const { mysqlPool } = require('../db/db.mysql');

// ---------------------
// Rate limiters
// ---------------------
const makeLimiter = (maxPerMin) =>
  rateLimit({
    windowMs: 60 * 1000,
    max: maxPerMin,
    message: { error: 'Too many requests — try again later.' },
    standardHeaders: true,
    legacyHeaders: false,
  });

// 외부 API 프록시는 더 엄격하게
const limiterKey      = makeLimiter(20); // /kakao-jskey
const limiterGeocode  = makeLimiter(15); // /geocode (외부 API 호출)
const limiterList     = makeLimiter(30); // / (목록 조회)
const limiterAggSido  = makeLimiter(20); // /agg/sido
const limiterAggSigu  = makeLimiter(30); // /agg/sigungu

// =====================
// 카카오 JS키 전달
// =====================
router.get('/kakao-jskey', limiterKey, (_req, res) => {
  const key = process.env.KAKAO_JS_KEY || '';
  if (!key) return res.status(500).json({ error: 'KAKAO_JS_KEY is not configured' });
  res.json({ key });
});

// =====================
// 카카오 로컬 지오코딩 프록시(REST 키 사용)
// =====================
router.get('/geocode', limiterGeocode, async (req, res, next) => {
  try {
    const query = (req.query.query || '').trim();
    if (!query) return res.status(400).json({ error: 'query is required' });
    if (query.length > 100) return res.status(400).json({ error: 'query too long' }); // 간단한 남용 방지

    const REST_KEY = process.env.KAKAO_REST_KEY || '';
    if (!REST_KEY) return res.status(500).json({ error: 'KAKAO_REST_KEY is not configured' });

    const url = 'https://dapi.kakao.com/v2/local/search/address.json';
    const resp = await axios.get(url, {
      params: { query },
      headers: { Authorization: `KakaoAK ${REST_KEY}` },
      timeout: 7000,
    });

    const docs = Array.isArray(resp.data?.documents) ? resp.data.documents : [];
    const results = docs.map(d => ({
      address_name: d.address_name,
      x: parseFloat(d.x), // lng
      y: parseFloat(d.y), // lat
    }));

    res.json({ results });
  } catch (e) { next(e); }
});

/**
 * GET /api/rems
 * - REMS 장비 목록 조회
 */
router.get('/', limiterList, async (req, res, next) => {
  try {
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = Math.max(parseInt(req.query.offset || '0',  10), 0);
    const search = (req.query.search || '').trim();

    const conds = ['address IS NOT NULL', "address <> ''"];
    const args  = [];
    if (search) { conds.push('address LIKE ?'); args.push(`%${search}%`); }
    const where = `WHERE ${conds.join(' AND ')}`;

    const sql = `
      SELECT id, cid, authKey, worker, phoneNumber, address,
             facCompany, monitorCompany,
             NULL AS multId,
             token, createdDate, rtu_id
      FROM \`rems_rems\`
      ${where}
      ORDER BY id DESC
      LIMIT ? OFFSET ?
    `;
    args.push(limit, offset);

    const [rows] = await mysqlPool.query(sql, args);
    res.json({ items: rows, limit, offset });
  } catch (e) { next(e); }
});

/**
 * GET /api/rems/agg/sido
 */
router.get('/agg/sido', limiterAggSido, async (_req, res, next) => {
  try {
    const sql = `
      SELECT name, COUNT(*) AS count
      FROM (
        SELECT
          CASE
            WHEN raw IN ('서울','서울특별시') THEN '서울특별시'
            WHEN raw IN ('부산','부산광역시') THEN '부산광역시'
            WHEN raw IN ('대구','대구광역시') THEN '대구광역시'
            WHEN raw IN ('인천','인천광역시') THEN '인천광역시'
            WHEN raw IN ('광주','광주광역시') THEN '광주광역시'
            WHEN raw IN ('대전','대전광역시') THEN '대전광역시'
            WHEN raw IN ('울산','울산광역시') THEN '울산광역시'
            WHEN raw IN ('세종','세종특별자치시','세종시') THEN '세종특별자치시'
            WHEN raw IN ('제주','제주특별자치도','제주도') THEN '제주특별자치도'
            WHEN raw IN ('경기도') THEN '경기도'
            WHEN raw IN ('강원','강원도','강원특별자치도') THEN '강원특별자치도'
            WHEN raw IN ('충북','충청북도') THEN '충청북도'
            WHEN raw IN ('충남','충청남도') THEN '충청남도'
            WHEN raw IN ('전북','전라북도','전북특별자치도') THEN '전북특별자치도'
            WHEN raw IN ('전남','전라남도') THEN '전라남도'
            WHEN raw IN ('경북','경상북도') THEN '경상북도'
            WHEN raw IN ('경남','경상남도') THEN '경상남도'
            ELSE raw
          END AS name
        FROM (
          SELECT TRIM(SUBSTRING_INDEX(address, ' ', 1)) AS raw
          FROM \`rems_rems\`
          WHERE address IS NOT NULL AND address <> ''
        ) r
        WHERE raw IS NOT NULL
          AND raw <> ''
          AND raw NOT REGEXP '^[0-9]'
          AND raw NOT REGEXP '^[0-9-]+$'
          AND raw NOT LIKE 'TEL%'
          AND raw NOT LIKE '연락처%'
          AND (
            raw REGEXP '(도|광역시|특별시|특별자치시|특별자치도)$'
            OR raw IN ('서울','부산','대구','इन천','광주','대전','울산','세종','제주',
                       '경남','경북','전남','전북','충남','충북','강원')
          )
      ) x
      GROUP BY name
      ORDER BY count DESC
    `;
    const [rows] = await mysqlPool.query(sql);
    res.json(rows.map(r => ({ name: r.name || '기타/미상', count: Number(r.count) })));
  } catch (e) { next(e); };
});

/**
 * GET /api/rems/agg/sigungu?sido=경기도
 */
router.get('/agg/sigungu', limiterAggSigu, async (req, res, next) => {
  try {
    const sido = (req.query.sido || '').trim();
    if (!sido) return res.status(400).json({ error: 'sido is required' });

    const sql = `
      SELECT name, COUNT(*) AS count
      FROM (
        SELECT
          TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(address, ' ', 2), ' ', -1)) AS name
        FROM \`rems_rems\`
        WHERE address IS NOT NULL AND address <> ''
          AND TRIM(SUBSTRING_INDEX(address, ' ', 1)) = ?
      ) AS x
      WHERE name IS NOT NULL
        AND name <> ''
        AND name NOT REGEXP '^[0-9]'
        AND name NOT REGEXP '^[0-9-]+$'
        AND name NOT LIKE 'TEL%'
        AND name NOT LIKE '연락처%'
      GROUP BY name
      ORDER BY count DESC
    `;
    const [rows] = await mysqlPool.query(sql, [sido]);
    res.json(rows.map(r => ({ name: r.name || '기타/미상', count: Number(r.count) })));
  } catch (e) { next(e); }
});

module.exports = router;
