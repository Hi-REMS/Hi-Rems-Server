// src/utils/kmaGrid.js


/*
 기상청 단기예보(VilageFcst) API에서 사용하는 DFS 격자 좌표 변환 유틸
 - 위경도(lat, lon) → DFS 격자(nx, ny) 로 변환 (toXY 모드)
 - 기상청 공식 변환 수식(5km 격자망, 기준점: 위도 38°, 경도 126°)
 - 사용 예시:
     const { dfs_xy_conv } = require('./kmaGrid');
     const grid = dfs_xy_conv('toXY', 37.5665, 126.9780); // 서울
     → { nx: 60, ny: 127 }  (기상청 API 호출 시 nx,ny 파라미터에 사용)

현재는 'toXY' 변환만 지원 (위경도 → 격자)
*/


function dfs_xy_conv(code, v1, v2) {
  const RE = 6371.00877; // 지구 반경(km)
  const GRID = 5.0;      // 격자 간격(km)
  const SLAT1 = 30.0;
  const SLAT2 = 60.0;
  const OLON = 126.0;
  const OLAT = 38.0;
  const XO = 43;
  const YO = 136;

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

  if (code === 'toXY') {
    const ra = Math.tan(Math.PI * 0.25 + v1 * DEGRAD * 0.5);
    const ra2 = re * sf / Math.pow(ra, sn);
    let theta = v2 * DEGRAD - olon;
    if (theta > Math.PI) theta -= 2.0 * Math.PI;
    if (theta < -Math.PI) theta += 2.0 * Math.PI;
    theta *= sn;
    return {
      nx: Math.floor(ra2 * Math.sin(theta) + XO + 0.5),
      ny: Math.floor(ro - ra2 * Math.cos(theta) + YO + 0.5),
    };
  }
}
module.exports = { dfs_xy_conv };
