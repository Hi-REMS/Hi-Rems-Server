// src/api.js
const express = require('express');
const router = express.Router();

// -------------------- 기본/공용 --------------------
const healthRoutes     = require('./routes/health');
const ordersRoutes     = require('./routes/orders');
const logsRoutes       = require('./routes/logs');
const dashboardRoutes  = require('./routes/dashboard');
const remsRoutes       = require('./routes/rems');

// -------------------- 테스트 --------------------
const dbTestRoutes     = require('./db/db.routes.test');

// -------------------- 에너지 --------------------
const energyRoutes        = require('./energy/service');
const energySeriesRoutes  = require('./energy/series');

// ✅ 날씨/기상
const vilageFcstRoutes        = require('./routes/weather.vilageFcst');         // 단기예보(격자, 기존)
const vilageFcstByPointRoutes = require('./routes/weather.vilageFcst.byPoint'); // ✅ 새로 추가된 by-point
const asosDailyRoutes         = require('./routes/weather.asosDaily');          // ASOS 일자료

// ✅ 익스포트
const exportMonthCsvRoutes    = require('./routes/export.monthCsv');            // 월별 CSV 다운로드

// -------------------- 라우터 마운트 --------------------
// 최상위
router.use('/', healthRoutes);
router.use('/', dbTestRoutes);

// 공용 엔드포인트
router.use('/orders',      ordersRoutes);
router.use('/logs',        logsRoutes);
router.use('/dashboard',   dashboardRoutes);
router.use('/rems',        remsRoutes);

// 날씨
router.use('/weather/vilageFcst', vilageFcstRoutes);
router.use('/weather/vilageFcst', vilageFcstByPointRoutes); // ✅ /by-point
router.use('/weather/asos',       asosDailyRoutes);

// 에너지
router.use('/energy/electric', energySeriesRoutes);
router.use('/energy',          energyRoutes);

// 익스포트
router.use('/export', exportMonthCsvRoutes);

module.exports = router;
