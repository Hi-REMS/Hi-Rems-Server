// src/routes/weather.vilageFcst.byPoint.js
const express = require('express');
const router = express.Router();

/**
 * KMA DFS(격자) 변환 (위경도 -> nx, ny)
 * 기상청 샘플 공식을 그대로 사용
 */
function latLonToGrid(lat, lon) {
  const RE = 6371.00877; // 지구 반경(km)
  const GRID = 5.0;      // 격자 간격(km)
  const SLAT1 = 30.0;    // 투영 위도1(degree)
  const SLAT2 = 60.0;    // 투영 위도2(degree)
  const OLON = 126.0;    // 기준 경도(degree)
  const OLAT = 38.0;     // 기준 위도(degree)
  const XO = 43;         // 기준 X좌표(GRID)
  const YO = 136;        // 기준 Y좌표(GRID)

  const DEGRAD = Math.PI / 180.0;

  const re = RE / GRID;
  const slat1 = SLAT1 * DEGRAD;
  const slat2 = SLAT2 * DEGRAD;
  const olon = OLON * DEGRAD;
  const olat = OLAT * DEGRAD;

  let sn = Math.tan(Math.PI * 0.25 + slat2 * 0.5) / Math.tan(Math.PI * 0.25 + slat1 * 0.5);
  sn = Math.log(Math.cos(slat1) / Math.cos(slat2)) / Math.log(sn);
  let sf = Math.tan(Math.PI * 0.25 + slat1 * 0.5);
  sf = Math.pow(sf, sn) * Math.cos(slat1) / sn;
  let ro = Math.tan(Math.PI * 0.25 + olat * 0.5);
  ro = re * sf / Math.pow(ro, sn);

  const ra = re * sf / Math.pow(Math.tan(Math.PI * 0.25 + lat * DEGRAD * 0.5), sn);
  let theta = lon * DEGRAD - olon;
  if (theta > Math.PI) theta -= 2.0 * Math.PI;
  if (theta < -Math.PI) theta += 2.0 * Math.PI;
  theta *= sn;

  const nx = Math.floor(ra * Math.sin(theta) + XO + 0.5);
  const ny = Math.floor(ro - ra * Math.cos(theta) + YO + 0.5);

  return { nx, ny };
}

/**
 * GET /api/weather/vilageFcst/by-point?lat=..&lon=..
 * - 위경도 -> DFS 격자(nx, ny)만 반환 (JSON)
 * - 후속으로 동네예보 API 연동 시 이 값을 사용
 */
router.get('/by-point', async (req, res) => {
  try {
    const lat = parseFloat(req.query.lat);
    const lon = parseFloat(req.query.lon);

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return res
        .status(400)
        .json({ ok: false, error: 'INVALID_PARAMS', hint: 'lat, lon이 필요합니다.' });
    }
    // 느슨한 한반도 범위 체크
    if (lat < 31 || lat > 39 || lon < 123 || lon > 134) {
      return res
        .status(400)
        .json({ ok: false, error: 'OUT_OF_RANGE', hint: '위경도 범위를 확인하세요.' });
    }

    const grid = latLonToGrid(lat, lon);

    return res.status(200).json({
      ok: true,
      method: 'by-point',
      query: { lat, lon },
      grid,          // { nx, ny }
      hourly: [],    // 추후 API 연동용 자리
      note: 'JSON OK'
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

module.exports = router;
