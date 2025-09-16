// src/energy/timeutil.js
// 시간/날짜 관련 유틸리티 함수 모듈
// - KST(Asia/Seoul) 기준 기간 범위를 UTC로 변환
// - 일/월 단위 버킷 키 생성
// - 누적 Wh 차이 → kWh 변환
// - Luxon(DateTime) 라이브러리 사용

const { DateTime } = require('luxon');
const TZ = 'Asia/Seoul';

/**
 * KST 기준 특정 기간(weekly|monthly|yearly)의 UTC 경계값 반환
 * @param {string} range - 'weekly' | 'monthly' | 'yearly'
 * @returns {Object} { startUtc: Date, endUtc: Date, bucket: 'day'|'month' }
 */
function getRangeUtc(range) {
  const nowKST = DateTime.now().setZone(TZ);

  if (range === 'weekly') {
    // 오늘 포함 최근 7일 (일 단위 버킷)
    const startKST = nowKST.startOf('day').minus({ days: 6 });
    const endKST   = nowKST.endOf('day').plus({ seconds: 1 });
    return { startUtc: startKST.toUTC().toJSDate(), endUtc: endKST.toUTC().toJSDate(), bucket: 'day' };
  }

  if (range === 'monthly') {
    // 이번 달 시작일 ~ 오늘까지 (일 단위 버킷)
    const startKST = nowKST.startOf('month');
    const endKST   = nowKST.endOf('day').plus({ seconds: 1 });
    return { startUtc: startKST.toUTC().toJSDate(), endUtc: endKST.toUTC().toJSDate(), bucket: 'day' };
  }

  if (range === 'yearly') {
    // 최근 12개월 (월 단위 버킷)
    const startKST = nowKST.startOf('month').minus({ months: 11 });
    const endKST   = nowKST.startOf('month').plus({ months: 1 });
    return { startUtc: startKST.toUTC().toJSDate(), endUtc: endKST.toUTC().toJSDate(), bucket: 'month' };
  }

  throw new Error('range must be weekly|monthly|yearly');
}

/**
 * KST 기준 버킷 키 생성
 * @param {Date} jsDate - JavaScript Date 객체
 * @param {string} bucket - 'day' | 'month'
 * @returns {string} YYYY-MM-DD 또는 YYYY-MM
 */
function bucketKeyKST(jsDate, bucket) {
  const dt = DateTime.fromJSDate(jsDate, { zone: TZ });
  return bucket === 'day' ? dt.toFormat('yyyy-LL-dd') : dt.toFormat('yyyy-LL');
}

/**
 * 누적 Wh 차이를 kWh로 변환
 * @param {bigint|number} firstWh - 시작 누적 Wh
 * @param {bigint|number} lastWh - 끝 누적 Wh
 * @returns {number|null} kWh (소수 2자리 반올림), 음수 또는 잘못된 값일 경우 null
 */
function whDeltaToKwh(firstWh, lastWh) {
  if (firstWh == null || lastWh == null) return null;
  const dWh = Number(lastWh - firstWh);
  if (!Number.isFinite(dWh) || dWh < 0) return null;
  return Math.round((dWh / 1000) * 100) / 100;
}

module.exports = { TZ, getRangeUtc, bucketKeyKST, whDeltaToKwh };
