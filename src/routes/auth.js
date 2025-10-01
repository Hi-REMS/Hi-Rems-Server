const express = require('express');
const argon2 = require('argon2');
const jwt = require('jsonwebtoken');
const rateLimit = require('express-rate-limit');
const { pool } = require('../db/db.pg');
const { requireAuth, cookieOpts, signAccessToken } = require('../middlewares/requireAuth');

const router = express.Router();

/* 로그인 시도 횟수 제한 (브루트포스 방지) */
const loginLimiter = rateLimit({
  windowMs: 10 * 60 * 1000, // 10분
  max: 50,                  // 10분 동안 50회 이상 로그인 차단
  standardHeaders: true,
  legacyHeaders: false,
});

/* 회원가입 */
router.post('/register', async (req, res) => {
  try {
    const { username, password } = req.body || {};
    if (!username || !password) {
      return res.status(400).json({ message: 'username/password required' });
    }

    const { rows: dup } = await pool.query(
      'SELECT 1 FROM public.members WHERE username=$1',
      [username]
    );
    if (dup.length) {
      return res.status(409).json({ message: 'username already exists' });
    }

    const hash = await argon2.hash(password);
    const { rows } = await pool.query(
      `INSERT INTO public.members (username, password)
       VALUES ($1, $2)
       RETURNING member_id, username`,
      [username, hash]
    );
    const user = rows[0];

    const access = signAccessToken(user);
    res
      .cookie('access_token', access, cookieOpts())
      .status(201)
      .json({ user: { id: user.member_id, username: user.username } });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'register failed' });
  }
});

/* 로그인 */
router.post('/login', loginLimiter, async (req, res) => {
  try {
    const { username, password } = req.body || {};
    if (!username || !password) {
      return res.status(400).json({ message: 'username/password required' });
    }

    const { rows } = await pool.query(
      'SELECT member_id, username, password FROM public.members WHERE username=$1',
      [username]
    );
    const user = rows[0];
    if (!user) {
      return res.status(401).json({ message: 'invalid credentials' });
    }

    const ok = await argon2.verify(user.password, password);
    if (!ok) {
      return res.status(401).json({ message: 'invalid credentials' });
    }

    const access = signAccessToken(user);
    res
      .cookie('access_token', access, cookieOpts())
      .json({ user: { id: user.member_id, username: user.username } });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'login failed' });
  }
});

/* 로그아웃 */
router.post('/logout', (req, res) => {
  res.clearCookie('access_token', cookieOpts()).json({ ok: true });
});

/* 내 정보 (보호됨) */
router.get('/me', requireAuth, (req, res) => {
  res.json({ user: req.user });
});

module.exports = router;
