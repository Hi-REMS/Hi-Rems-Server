const express = require('express');
const router = express.Router();
const axios = require('axios');
const rateLimit = require('express-rate-limit');
const { mysqlPool } = require('../db/db.mysql');
const { pool: pg } = require('../db/db.pg'); 

router.get('/kakao-jskey', (_req, res) => {
  const key = process.env.KAKAO_JS_KEY || '';
  if (!key) return res.status(500).json({ error: 'KAKAO_JS_KEY is not configured' });
  res.json({ key });
});

router.get('/geocode', async (req, res, next) => {
  try {
    const query = (req.query.query || '').trim();
    console.log('[GEOCODE] query =', query);
    if (!query) return res.status(400).json({ error: 'query is required' });

    const REST_KEY = process.env.KAKAO_REST_KEY || '';
    if (!REST_KEY) return res.status(500).json({ error: 'KAKAO_REST_KEY is not configured' });

    const addrUrl = 'https://dapi.kakao.com/v2/local/search/address.json';
    const coordUrl = 'https://dapi.kakao.com/v2/local/geo/coord2address.json';

    const addrResp = await axios.get(addrUrl, {
      params: { query },
      headers: { Authorization: `KakaoAK ${REST_KEY}` },
      timeout: 7000,
      validateStatus: () => true,
    });

    const doc = addrResp.data?.documents?.[0];
    if (!doc) return res.status(404).json({ error: 'no address found' });

    const { x, y } = doc;
    let detailAddr = null;

    try {
      const coordResp = await axios.get(coordUrl, {
        params: { x, y },
        headers: { Authorization: `KakaoAK ${REST_KEY}` },
        timeout: 7000,
        validateStatus: () => true,
      });

      detailAddr = coordResp.data?.documents?.[0]?.address ||
                   coordResp.data?.documents?.[0]?.road_address ||
                   null;
    } catch (err) {
      console.warn('[coord2address] fallback failed:', err.message);
    }

    const result = {
      query,
      address_name: detailAddr?.address_name || doc.address_name,
      region_1depth_name: detailAddr?.region_1depth_name || doc.address?.region_1depth_name,
      region_2depth_name: detailAddr?.region_2depth_name || doc.address?.region_2depth_name,
      region_3depth_name: detailAddr?.region_3depth_name || doc.address?.region_3depth_name,
      road_name: detailAddr?.road_name || doc.road_address?.road_name,
      building_name: detailAddr?.building_name || doc.road_address?.building_name,
      zone_no: detailAddr?.zone_no || doc.road_address?.zone_no,
      x: parseFloat(x),
      y: parseFloat(y),
    };

    res.json({ results: [result] });
  } catch (e) {
    next(e);
  }
});

router.get('/', async (req, res, next) => {
  try {
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = Math.max(parseInt(req.query.offset || '0',  10), 0);
    const search = (req.query.search || '').trim();

    const conds = ['address IS NOT NULL', "address <> ''"];
    const args  = [];

    if (search) {
      conds.push('worker LIKE ?'); 
      args.push(`%${search}%`); 
    }

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

router.get('/agg/sido', async (_req, res, next) => {
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

router.get('/agg/sigungu', async (req, res, next) => {
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
