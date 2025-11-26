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
