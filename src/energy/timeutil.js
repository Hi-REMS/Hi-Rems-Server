const { DateTime } = require('luxon');
const TZ = 'Asia/Seoul';

function getRangeUtc(range) {
  const nowKST = DateTime.now().setZone(TZ);

  if (range === 'weekly') {
    const startKST = nowKST.startOf('day').minus({ days: 6 });
    const endKST   = nowKST.endOf('day').plus({ seconds: 1 });
    return { startUtc: startKST.toUTC().toJSDate(), endUtc: endKST.toUTC().toJSDate(), bucket: 'day' };
  }

  if (range === 'monthly') {
    const startKST = nowKST.startOf('month');
    const endKST   = nowKST.endOf('day').plus({ seconds: 1 });
    return { startUtc: startKST.toUTC().toJSDate(), endUtc: endKST.toUTC().toJSDate(), bucket: 'day' };
  }

if (range === 'yearly') {
  const nowKST = DateTime.now().setZone(TZ);
  const startKST = DateTime.fromObject(
    { year: nowKST.year, month: 1, day: 1 },
    { zone: TZ }
  ).startOf('day');

  const endKST = nowKST.endOf('day').plus({ seconds: 1 });

  return {
    startUtc: startKST.toUTC().toJSDate(),
    endUtc: endKST.toUTC().toJSDate(),
    bucket: 'day'
  };
}

  throw new Error('range must be weekly|monthly|yearly');
}

function bucketKeyKST(jsDate, bucket) {
  const dt = DateTime.fromJSDate(jsDate, { zone: TZ });
  return bucket === 'day' ? dt.toFormat('yyyy-LL-dd') : dt.toFormat('yyyy-LL');
}

function whDeltaToKwh(firstWh, lastWh) {
  if (firstWh == null || lastWh == null) return null;
  const dWh = Number(lastWh - firstWh);
  if (!Number.isFinite(dWh) || dWh < 0) return null;
  return Math.round((dWh / 1000) * 100) / 100;
}

module.exports = { TZ, getRangeUtc, bucketKeyKST, whDeltaToKwh };
