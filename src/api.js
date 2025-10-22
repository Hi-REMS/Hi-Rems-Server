// src/api.js
const express = require('express');
const router = express.Router();

// -------------------- 기본/공용 --------------------
const healthRoutes     = require('./routes/health');
const ordersRoutes     = require('./routes/orders');
const logsRoutes       = require('./routes/logs');
const dashboardRoutes  = require('./routes/dashboard');
const remsRoutes       = require('./routes/rems');
const dbTestRoutes     = require('./db/db.routes.test');
const energyRoutes        = require('./energy/service');
const energySeriesRoutes  = require('./energy/series');
const vilageFcstRoutes        = require('./routes/weather.vilageFcst');
const vilageFcstByPointRoutes = require('./routes/weather.vilageFcst.byPoint');
const asosDailyRoutes         = require('./routes/weather.asosDaily');
const exportMonthCsvRoutes    = require('./routes/export.monthCsv');
const authRoutes = require('./routes/auth');
const userRoutes = require('./routes/user');   // ← 이 줄 추가

// -------------------- 라우터 마운트 --------------------
// 최상위
router.use('/health', healthRoutes);
router.use('/', dbTestRoutes);

// 공용 엔드포인트
router.use('/orders',      ordersRoutes);
router.use('/logs',        logsRoutes);
router.use('/dashboard',   dashboardRoutes);
router.use('/rems',        remsRoutes);

// 날씨
router.use('/weather/vilageFcst', vilageFcstRoutes);
router.use('/weather/vilageFcst', vilageFcstByPointRoutes);
router.use('/weather/asos',       asosDailyRoutes);

// 에너지
router.use('/energy/electric', energySeriesRoutes);
router.use('/energy',          energyRoutes);

// 익스포트
router.use('/export', exportMonthCsvRoutes);

router.use('/auth', authRoutes);

router.use('/user', userRoutes);

module.exports = router;
