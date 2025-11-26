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

// -------------------- 날씨 --------------------
const omByPointRoutes = require('./routes/weather.openMeteo.byPoint');
const omByImeiRoutes  = require('./routes/weather.openMeteo.byImei');
const omByImeiDailyRoutes = require('./routes/weather.openMeteo.byImei.daily');
const asosDailyRoutes = require('./routes/weather.asosDaily');

// -------------------- 익스포트 --------------------
const exportMonthCsvRoutes = require('./routes/export.monthCsv');

// -------------------- 인증/사용자/설비/유지보수 --------------------
const authRoutes        = require('./routes/auth');
const userRoutes        = require('./routes/user');
const facilityRoutes    = require('./routes/facility');
const maintenanceRoutes = require('./routes/maintenance');

const membersRoutes     = require('./routes/members');
const facilityUploadRoutes = require('./routes/facility.upload');

// 최상위
router.use('/health', healthRoutes);
router.use('/', dbTestRoutes);

// 공용 엔드포인트
router.use('/orders',      ordersRoutes);
router.use('/logs',        logsRoutes);
router.use('/dashboard',   dashboardRoutes);
router.use('/rems',        remsRoutes);

// === 날씨 ===
router.use('/weather/openmeteo', omByPointRoutes);
router.use('/weather/openmeteo', omByImeiRoutes);
router.use('/weather/openmeteo', omByImeiDailyRoutes);
router.use('/weather/asos', asosDailyRoutes);

// === 에너지 ===
router.use('/energy', energySeriesRoutes);
router.use('/energy', energyRoutes);

router.use('/export', exportMonthCsvRoutes);

// === 인증/사용자/설비/유지보수
router.use('/auth',        authRoutes);
router.use('/user',        userRoutes);
router.use('/facility',    facilityRoutes);
router.use('/facility', facilityUploadRoutes);
router.use('/maintenance', maintenanceRoutes);

router.use('/members',     membersRoutes);
module.exports = router;
