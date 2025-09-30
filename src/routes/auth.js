// src/routes/auth.js
const express = require('express');
const argon2 = require('argon2');
const jwt = require('jsonwebtoken');
const { pool } = require('../db/db.pg'); // src/db/db.pg.js
const { requireAuth } = require('../middlewares/requireAuth');

const router = express.Router();

/* 쿠키 옵션: 세션 쿠키 (브라우저 닫으면 삭제) */
function cookieOpts() {
  const prod = process.env.NODE_ENV === 'production';
  const domain = process.env.COOKIE_DOMAIN || undefined;
  return {
    httpOnly: true,
    secure: prod,
    sameSite: prod ? 'none' : 'lax',
    domain,
    // maxAge / expires 없음 → 세션 쿠키
  };
}

/* JWT 생성기
   - sess(세션 시작 시각) 넣어서 1시간 절대만료 체크에 활용
*/
function signAccessToken(user, sess) {
  const sessionStart = sess ?? Date.now();
  return jwt.sign(
    { username: user.username, sess: sessionStart },
    process.env.JWT_ACCESS_SECRET,
    {
      subject: String(user.member_id),
      expiresIn: process.env.ACCESS_TOKEN_EXPIRES || '15m', // 슬라이딩 만료(기본 15분)
    }
  );
}

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

    // 가입 직후 자동 로그인
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
router.post('/login', async (req, res) => {
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
