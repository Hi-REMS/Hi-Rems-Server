// src/routes/weather.asosDaily.js

/*
 기상청 ASOS(지상관측) 일자료 조회 API 라우트
 - 외부 공공데이터포털 기상청 API (AsosDalyInfoService/getWthrDataList) 호출
 - .env의 KMA_ASOS_KEY, KMA_ASOS_ENDPOINT, KMA_ASOS_FALLBACK_STNID 사용
 - 내부 엔드포인트: GET /api/weather/asos/daily
   ▸ 파라미터: stnId(관측소ID, 기본=서울 108), start=YYYYMMDD, end=YYYYMMDD
   ▸ 선택: pageNo(기본 1), numOfRows(기본 365), debug=1
 - 반환: { stnId, startDt, endDt, count, items[] }
 - 기능:
   ▸ 환경변수 serviceKey(기상청 API 키)를 안전히 처리 (인코딩/디코딩 자동 감지)
   ▸ debug=1 옵션으로 fullUrl, header, 응답 일부 스니펫 확인 가능
   ▸ 에러 처리: 잘못된 날짜, HTTP 오류, JSON 아님(XML), resultCode≠00 등
*/

const express = require('express');
const router = express.Router();
const axios = require('axios');
const qs = require('qs');

const ENDPOINT = (process.env.KMA_ASOS_ENDPOINT || 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList').trim();
const FALLBACK_STNID = String(process.env.KMA_ASOS_FALLBACK_STNID || '108');

function normalizeMaybeEncoded(raw) {
  if (!raw) return raw;
  const s = String(raw).trim().replace(/^"+|"+$/g, '');
  try {
    return s.includes('%') ? decodeURIComponent(s) : s;
  } catch {
    return s;
  }
}

/**
 * 일자료 조회
 * GET /api/weather/asos/daily?stnId=108&start=YYYYMMDD&end=YYYYMMDD
 * 옵션: pageNo, numOfRows, debug=1
 */
router.get('/daily', async (req, res, next) => {
  try {
    const stnId     = req.query.stnId ? String(req.query.stnId) : FALLBACK_STNID;
    const startDt   = String(req.query.start || '').replace(/-/g, '');
    const endDt     = String(req.query.end   || '').replace(/-/g, '');
    const pageNo    = parseInt(req.query.pageNo    || '1', 10);
    const numOfRows = parseInt(req.query.numOfRows || '365', 10);

    if (!/^\d{8}$/.test(startDt) || !/^\d{8}$/.test(endDt)) {
      return res.status(400).json({ error: 'INVALID_DATE', hint: 'YYYYMMDD 형식의 start / end 필요' });
    }

    const serviceKey = normalizeMaybeEncoded(process.env.KMA_ASOS_KEY || '');

    const params = {
      serviceKey,
      dataType: 'JSON',
      dataCd: 'ASOS',
      dateCd: 'DAY',
      startDt, endDt,
      stnIds: stnId,
      pageNo, numOfRows,
    };

    const query = qs.stringify(params, { encode: false });
    const fullUrl = `${ENDPOINT}?${query}`;

    const kmaResp = await axios.get(ENDPOINT, {
      params,
      paramsSerializer: (p) => qs.stringify(p, { encode: false }),
      timeout: 15000,
      validateStatus: () => true,
    });

    const httpStatus = kmaResp.status;
    const body = kmaResp.data;

    if (req.query.debug === '1') {
      return res.json({
        http: httpStatus,
        endpoint: ENDPOINT,
        env: {
          ENDPOINT,
          KEY_PREFIX: serviceKey ? serviceKey.slice(0, 6) : '',
        },
        requestUrl: fullUrl,
        rawType: typeof body,
        bodySnippet: typeof body === 'string' ? String(body).slice(0, 800) : undefined,
        header: body?.response?.header,
      });
    }

    if (httpStatus !== 200) {
      return res.status(502).json({
        error: 'KMA_ASOS_BAD_STATUS',
        http: httpStatus,
        requestUrl: fullUrl,
        bodySnippet: typeof body === 'string' ? String(body).slice(0, 800) : body,
      });
    }

    if (typeof body === 'string') {
      return res.status(502).json({
        error: 'KMA_ASOS_NON_JSON',
        requestUrl: fullUrl,
        bodySnippet: String(body).slice(0, 800),
      });
    }

    const header = body?.response?.header;
    if (header?.resultCode !== '00') {
      return res.status(502).json({
        error: 'KMA_ASOS_API_ERROR',
        requestUrl: fullUrl,
        header,
      });
    }

    const items = body?.response?.body?.items?.item || [];
    return res.json({ stnId, startDt, endDt, count: items.length, items });
  } catch (e) {
    next(e);
  }
});

module.exports = router;