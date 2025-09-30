// src/routes/auth.js
const express = require('express');
const argon2 = require('argon2');
const jwt = require('jsonwebtoken');
const { pool } = require('../db/db.pg');         // ✅ 경로 주의: src/db/db.pg.js
const { requireAuth } = require('../middlewares/requireAuth');

const router = express.Router();

/* 쿠키 옵션 */
function cookieOpts(days = 7) {
  const prod = process.env.NODE_ENV === 'production';
  const domain = process.env.COOKIE_DOMAIN || undefined;
  return {
    httpOnly: true,
    secure: prod,
    sameSite: prod ? 'none' : 'lax',
    domain,
    maxAge: days * 24 * 60 * 60 * 1000,
  };
}

/* JWT 생성기 */
function signAccessToken(user) {
  return jwt.sign(
    { username: user.username },
    process.env.JWT_ACCESS_SECRET,
    { subject: String(user.member_id), expiresIn: process.env.ACCESS_TOKEN_EXPIRES || '15m' }
  );
}
function signRefreshToken(user) {
  return jwt.sign(
    { username: user.username },
    process.env.JWT_REFRESH_SECRET,
    { subject: String(user.member_id), expiresIn: process.env.REFRESH_TOKEN_EXPIRES || '7d' }
  );
}

/* 회원가입 */
router.post('/register', async (req, res) => {
  try {
    const { username, password } = req.body || {};
    if (!username || !password) return res.status(400).json({ message: 'username/password required' });

    const { rows: dup } = await pool.query('SELECT 1 FROM public.members WHERE username=$1', [username]);
    if (dup.length) return res.status(409).json({ message: 'username already exists' });

    const hash = await argon2.hash(password);
    const { rows } = await pool.query(
      `INSERT INTO public.members (username, password)
       VALUES ($1, $2)
       RETURNING member_id, username`,
      [username, hash]
    );
    const user = rows[0];

    // 가입 직후 자동 로그인(원치 않으면 이 블록 제거)
    const access = signAccessToken(user);
    const refresh = signRefreshToken(user);
    res
      .cookie('access_token', access, cookieOpts(1))
      .cookie('refresh_token', refresh, cookieOpts(7))
      .status(201)
      .json({ user: { id: user.member_id, username: user.username } });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'register failed' });
  }
});

/* 로그인 */
router.post('/login', async (req, res) => {
  try {
    const { username, password } = req.body || {};
    if (!username || !password) return res.status(400).json({ message: 'username/password required' });

    const { rows } = await pool.query(
      'SELECT member_id, username, password FROM public.members WHERE username=$1',
      [username]
    );
    const user = rows[0];
    if (!user) return res.status(401).json({ message: 'invalid credentials' });

    const ok = await argon2.verify(user.password, password);
    if (!ok) return res.status(401).json({ message: 'invalid credentials' });

    const access = signAccessToken(user);
    const refresh = signRefreshToken(user);

    res
      .cookie('access_token', access, cookieOpts(1))
      .cookie('refresh_token', refresh, cookieOpts(7))
      .json({ user: { id: user.member_id, username: user.username } });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'login failed' });
  }
});

/* 토큰 갱신 */
router.post('/refresh', async (req, res) => {
  try {
    const token = req.cookies && req.cookies.refresh_token;
    if (!token) return res.status(401).json({ message: 'no refresh token' });

    const payload = jwt.verify(token, process.env.JWT_REFRESH_SECRET);
    const userId = payload.sub;

    const { rows } = await pool.query(
      'SELECT member_id, username FROM public.members WHERE member_id=$1',
      [userId]
    );
    const user = rows[0];
    if (!user) return res.status(401).json({ message: 'user not found' });

    const access = signAccessToken(user);
    res.cookie('access_token', access, cookieOpts(1)).json({ ok: true });
  } catch (e) {
    console.error(e);
    res.status(401).json({ message: 'invalid refresh token' });
  }
});

/* 로그아웃 */
router.post('/logout', (req, res) => {
  res
    .clearCookie('access_token', cookieOpts())
    .clearCookie('refresh_token', cookieOpts())
    .json({ ok: true });
});

/* 내 정보 (보호됨) */
router.get('/me', requireAuth, (req, res) => {
  res.json({ user: req.user });
});

module.exports = router;
