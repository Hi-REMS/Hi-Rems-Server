// src/utils/nearestAsos.js
const stations = require('./asosStations');

// 하버사인 거리(km)
function haversine(lat1, lon1, lat2, lon2) {
  const R = 6371; // km
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

  const maxKm = opt.maxKm ?? 120; // 선호 반경
  let best = null;

  for (const s of stations) {
    const d = haversine(lat, lon, s.lat, s.lon);
    if (!best || d < best.distKm) {
      best = { ...s, distKm: Math.round(d * 10) / 10 };
    }
  }

  // 선호 반경 밖이면 그대로 가장 가까운 곳 반환(혹은 null을 원하면 여기서 필터)
  return best && (best.distKm <= (opt.maxKmHard ?? Infinity) ? best : best);
}

module.exports = { nearestAsos, haversine };
