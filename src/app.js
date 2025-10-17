// src/app.js
// Express 서버 진입점
// - 환경변수 로딩(dotenv)
// - JSON 파서, CORS 설정
// - /api 라우트 마운트
// - 헬스체크 및 오류 핸들러
// 실행: node src/app.js (또는 npm start)

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const cookieParser = require('cookie-parser');
const api = require('./api');
const path = require('path');
const { setupEnergyCron } = require(path.join(__dirname, './jobs/energyRefresh'));


const app = express();
app.use(express.json());
app.use(cookieParser());

// 프록시(Nginx/Cloudflare 등) 뒤에서 실제 클라이언트 IP 인식
app.set('trust proxy', 1);

// -------------------- CORS 설정 --------------------
const whitelist = (process.env.CORS_ORIGIN || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

app.use(cors({
  origin(origin, cb) {
    if (!origin) return cb(null, true); // 서버-서버/CLI 요청 허용
    if (!whitelist.length || whitelist.includes(origin)) return cb(null, true);
    cb(new Error('Not allowed by CORS'));
  },
  credentials: true,
}));

// -------------------- 헬스 체크 --------------------
app.get('/health', (_req, res) => {
  res.status(200).json({ ok: true, uptime: process.uptime() });
});

// 전역 기본 제한 (모든 엔드포인트에 적용)
const globalLimiter = rateLimit({
  windowMs: 60 * 1000, // 1분
  max: 200,            // 1분당 최대 200요청
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => (req.path || '') === '/api/health-direct',
});
app.use(globalLimiter);

// -------------------- 라우트 마운트 --------------------
app.use('/api', api); // 모든 /api/* 요청을 src/api.js로 위임

// -------------------- 임시 헬스체크 (직접 DB쿼리) --------------------
app.get('/api/health-direct', async (_req, res) => {
  try {
    const { pool } = require('./db/db.pg');
    const { rows } = await pool.query('SELECT NOW() as now');
    res.json({ ok: true, db_now: rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

setupEnergyCron();

// -------------------- 오류 핸들러 --------------------
app.use((err, _req, res, _next) => {
  console.error(err);
  res.status(err.status || 500).json({ error: err.message || 'Server Error' });
});

// -------------------- 서버 시작 --------------------
const port = Number(process.env.PORT || 3000);
app.listen(port, () => {
  console.log(`API listening on http://127.0.0.1:${port}`);
});
