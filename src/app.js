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
const api = require('./api');

const app = express();
app.use(express.json());

// 프록시(Nginx/Cloudflare 등) 뒤에서 실제 클라이언트 IP 인식
app.set('trust proxy', 1);

// -------------------- CORS 설정 --------------------
// 개발 시: 특정 프론트엔드(dev 서버)만 허용
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
}));

// -------------------- 헬스 체크 --------------------
// 전역 리미터보다 "먼저" 선언하여 완전히 제외 (모니터링 안정성)
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

// 전역 미들웨어로 적용 (CORS 이후, 라우트 등록 이전)
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

// -------------------- 오류 핸들러 --------------------
app.use((err, _req, res) => {
  console.error(err);
  res.status(err.status || 500).json({ error: err.message || 'Server Error' });
});

// -------------------- 서버 시작 --------------------
const port = Number(process.env.PORT || 3000);
app.listen(port, () => {
  console.log(`API listening on http://127.0.0.1:${port}`);
});
