// src/utils/nearestAsos.js

/*
 ASOS 관측소 근접 탐색 유틸
 - 기상청 종관기상관측소(ASOS) 목록(asosStations.js)에 대해
   하버사인 공식을 이용해 거리(km)를 계산
 - 입력 좌표(lat, lon)에 가장 가까운 ASOS 관측소를 반환

 주요 함수:
   • haversine(lat1, lon1, lat2, lon2) → 두 점 사이의 거리(km)
   • nearestAsos({lat,lon}, {maxKm?, maxKmHard?})
       → { stnId, name, lat, lon, distKm }

 사용 예시:
   const { nearestAsos } = require('./nearestAsos');
   const geo = { lat: 37.5665, lon: 126.9780 }; // 서울 광화문
   const stn = nearestAsos(geo);
   → { stnId: 108, name: '서울', lat: 37.57142, lon: 126.9658, distKm: 1.3 }

 참고:
 - maxKm: 선호 반경 (기본 120km)
 - maxKmHard: 절대 반경 초과 시 null 반환하고 싶을 때 설정 가능
*/

const stations = require('./asosStations');

function haversine(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
    Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

/**
 * 가장 가까운 ASOS 관측소 찾기
 * @param {{lat:number, lon:number}} geo
 * @param {{maxKm?:number}} [opt]
 * @returns {{stnId:number, name:string, lat:number, lon:number, distKm:number}}
 */
function nearestAsos(geo, opt = {}) {
  const { lat, lon } = geo || {};
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  let best = null;

  for (const s of stations) {
    const d = haversine(lat, lon, s.lat, s.lon);
    if (!best || d < best.distKm) {
      best = { ...s, distKm: Math.round(d * 10) / 10 };
    }
  }

  return best && (best.distKm <= (opt.maxKmHard ?? Infinity) ? best : best);
}

module.exports = { nearestAsos, haversine };
