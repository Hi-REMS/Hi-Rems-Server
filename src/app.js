require('dotenv').config();
const express = require('express');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const cookieParser = require('cookie-parser');
const api = require('./api');
const path = require('path');
const fs = require('fs');
const { setupEnergyCron } = require('./jobs/energyRefresh');
const { getNormalPointsCached } = require('./jobs/normalPointCache');

const app = express();
app.use(express.json());
app.use(cookieParser());
app.set('trust proxy', 1);

// ──────────────────────────────────────────────
// ✅ CORS 설정 (부분매칭 허용)
const whitelist = (process.env.CORS_ORIGIN || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

app.use(cors({
  origin(origin, cb) {
    if (!origin) return cb(null, true);
    const allowed = whitelist.some(w => origin.startsWith(w));
    if (allowed) return cb(null, true);
    cb(new Error(`CORS blocked: ${origin}`));
  },
  credentials: true,
}));

// ──────────────────────────────────────────────
app.get('/health', (_req, res) => {
  res.status(200).json({ ok: true, uptime: process.uptime() });
});

// 글로벌 요청 제한
const globalLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 200,
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => (req.path || '') === '/api/health-direct',
});
app.use(globalLimiter);

// ──────────────────────────────────────────────
// API
app.use('/api', api);

// DB health
app.get('/api/health-direct', async (_req, res) => {
  try {
    const { pool } = require('./db/db.pg');
    const { rows } = await pool.query('SELECT NOW() as now');
    res.json({ ok: true, db_now: rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ──────────────────────────────────────────────
// 크론잡
setupEnergyCron();

// ✅ 정상 발전소 데이터 프리로드
const dist = path.join(__dirname, '../frontend/dist');
app.get(/^\/(?!api\/).*/, async (req, res, next) => {
  try {
    let normalPoints = [];
    try {
      normalPoints = await getNormalPointsCached();
    } catch (err) {
      console.warn('[WARN] Failed to load normalPoints cache:', err.message);
    }

    const htmlPath = path.join(dist, 'index.html');
    let html = fs.readFileSync(htmlPath, 'utf8');
    const preloadScript = `<script>window.__NORMAL_POINTS__=${JSON.stringify(normalPoints)};</script>`;
    html = html.replace('<!--__PRELOAD_NORMAL_POINTS__-->', preloadScript);

    res.setHeader('Cache-Control', 'no-cache');
    res.type('html').send(html);
  } catch (err) {
    next(err);
  }
});

// ──────────────────────────────────────────────
// 오류 핸들러
app.use((err, _req, res, _next) => {
  console.error(err);
  const status = err.status || 500;
  const body = { error: err.message || 'Server Error' };
  if (status === 422 && Array.isArray(err.matches)) body.matches = err.matches;
  res.status(status).json(body);
});

// ──────────────────────────────────────────────
// 서버 시작
const port = Number(process.env.PORT || 3000);
app.listen(port, () => {
  console.log(`🚀 API listening on port ${port} (env: ${process.env.NODE_ENV})`);
});
