// routes/auth.js
const express = require('express');
const argon2 = require('argon2');
const rateLimit = require('express-rate-limit');
const crypto = require('crypto');
const nodemailer = require('nodemailer');

const { pool } = require('../db/db.pg');
const { requireAuth, cookieOpts, signAccessToken } = require('../middlewares/requireAuth');

const router = express.Router();

/* ─────────────────────────────────────────────────────
 * 공통 유틸
 * ───────────────────────────────────────────────────── */
function clientInfo(req) {
  return {
    ip: req.ip || req.headers['x-forwarded-for'] || req.connection?.remoteAddress || null,
    ua: req.headers['user-agent'] || '',
  };
}

async function logLoginAttempt({ member_id = null, username = null, success, ip, user_agent, reason = null }) {
  try {
    await pool.query(
      `INSERT INTO public.auth_login_attempts
       (member_id, username, success, ip, user_agent, reason)
       VALUES ($1,$2,$3,$4,$5,$6)`,
      [member_id, username, success, ip, user_agent, reason]
    );
  } catch (e) {
    console.error('[auth] logLoginAttempt failed:', e.message);
  }
}

/* 비밀번호 정책(프론트와 동일 규칙) */
function validatePassword(pw, username) {
  const errors = [];
  if (!pw || pw.length < 8) errors.push('8자 이상이어야 합니다.');
  if (!/[A-Z]/.test(pw)) errors.push('대문자(A-Z)를 포함하세요.');
  if (!/[a-z]/.test(pw)) errors.push('소문자(a-z)를 포함하세요.');
  if (!/[0-9]/.test(pw)) errors.push('숫자(0-9)를 포함하세요.');
  if (!/[^A-Za-z0-9]/.test(pw)) errors.push('특수문자를 포함하세요.');
  if (/\s/.test(pw)) errors.push('공백 문자는 사용할 수 없습니다.');
  if (username && pw.toLowerCase().includes(String(username).toLowerCase()))
    errors.push('비밀번호에 아이디(이메일)를 포함할 수 없습니다.');
  return errors;
}

/* SMTP: 설정 없으면 콘솔 폴백 */
async function sendMail({ to, subject, text, html }) {
  const { SMTP_HOST, SMTP_PORT, SMTP_SECURE, SMTP_USER, SMTP_PASS } = process.env;
  if (!SMTP_HOST || !SMTP_PORT || !SMTP_USER || !SMTP_PASS) {
    console.warn('[mail] SMTP env not set → console fallback');
    console.log('To:', to, '\nSubject:', subject, '\nText:', text, '\nHTML:', html);
    return;
  }
  const transporter = nodemailer.createTransport({
    host: SMTP_HOST,
    port: Number(SMTP_PORT),
    secure: String(SMTP_SECURE) === 'true',
    auth: { user: SMTP_USER, pass: SMTP_PASS },
  });
  await transporter.sendMail({ from: `"Hi-REMS" <${SMTP_USER}>`, to, subject, text, html });
}

/* 재설정 토큰(평문/해시 분리) */
function createResetToken() {
  const token = crypto.randomBytes(32).toString('base64url');
  const hash = crypto.createHash('sha256').update(token).digest('base64url');
  return { token, hash };
}

/* ─────────────────────────────────────────────────────
 * Rate Limit
 * ───────────────────────────────────────────────────── */
const loginLimiter = rateLimit({
  windowMs: 10 * 60 * 1000, // 10분
  max: 50,
  standardHeaders: true,
  legacyHeaders: false,
});

const forgotLimiter = rateLimit({
  windowMs: 10 * 60 * 1000, // 10분
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
});

/* ─────────────────────────────────────────────────────
 * 회원가입
 * ───────────────────────────────────────────────────── */
router.post('/register', async (req, res) => {
  const client = await pool.connect();
  try {
    const { username, password } = req.body || {};
    if (!username || !password) {
      return res.status(400).json({ message: 'username/password required' });
    }

    const { rows: dup } = await client.query(
      'SELECT 1 FROM public.members WHERE username=$1',
      [username]
    );
    if (dup.length) {
      return res.status(409).json({ message: 'username already exists' });
    }

    await client.query('BEGIN');

    const hash = await argon2.hash(password, {
      type: argon2.argon2id,
      memoryCost: 19456,
      timeCost: 2,
      parallelism: 1,
    });

    const { rows } = await client.query(
      `INSERT INTO public.members (username, password)
       VALUES ($1, $2)
       RETURNING member_id, username, password`,
      [username, hash]
    );
    const user = rows[0];

    await client.query(
      `INSERT INTO public.auth_password_history (member_id, password_hash)
       VALUES ($1, $2)`,
      [user.member_id, user.password]
    );

    await client.query('COMMIT');

    const access = signAccessToken({ sub: user.member_id, username: user.username });
    res
      .cookie('access_token', access, cookieOpts())
      .status(201)
      .json({ user: { id: user.member_id, username: user.username } });
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    console.error(e);
    res.status(500).json({ message: 'register failed' });
  } finally {
    client.release();
  }
});

/* ─────────────────────────────────────────────────────
 * 로그인
 * ───────────────────────────────────────────────────── */
router.post('/login', loginLimiter, async (req, res) => {
  const { ip, ua } = clientInfo(req);

  try {
    const { username, password } = req.body || {};
    if (!username || !password) {
      await logLoginAttempt({ username, success: false, ip, user_agent: ua, reason: 'missing_fields' });
      return res.status(400).json({ message: 'username/password required' });
    }

    const { rows } = await pool.query(
      'SELECT member_id, username, password FROM public.members WHERE username=$1',
      [username]
    );
    const user = rows[0];

    if (!user) {
      await logLoginAttempt({ username, success: false, ip, user_agent: ua, reason: 'user_not_found' });
      return res.status(401).json({ message: 'invalid credentials' });
    }

    const ok = await argon2.verify(user.password, password);
    if (!ok) {
      await logLoginAttempt({ member_id: user.member_id, username, success: false, ip, user_agent: ua, reason: 'invalid_password' });
      return res.status(401).json({ message: 'invalid credentials' });
    }

    await logLoginAttempt({ member_id: user.member_id, username, success: true, ip, user_agent: ua, reason: null });

    const access = signAccessToken({ sub: user.member_id, username: user.username });
    res
      .cookie('access_token', access, cookieOpts())
      .json({ user: { id: user.member_id, username: user.username } });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'login failed' });
  }
});

/* ─────────────────────────────────────────────────────
 * 로그아웃 / 내 정보
 * ───────────────────────────────────────────────────── */
router.post('/logout', (req, res) => {
  res.clearCookie('access_token', cookieOpts()).json({ ok: true });
});

router.get('/me', requireAuth, (req, res) => {
  res.json({ user: req.user });
});

/* ─────────────────────────────────────────────────────
 * 비밀번호 찾기: 재설정 링크 발송
 * ───────────────────────────────────────────────────── */
router.post('/forgot', forgotLimiter, async (req, res) => {
  const client = await pool.connect();
  try {
    const raw = (req.body && req.body.username) || '';
    const username = String(raw).trim().toLowerCase();   // 🔹 정규화(권장)
    if (!username) return res.status(400).json({ ok:false, message: 'username required' });

    const { rows } = await client.query(
      // 🔹 대소문자 무시 매칭
      'SELECT member_id, username FROM public.members WHERE LOWER(username) = $1',
      [username]
    );
    const user = rows[0];

    // 계정 없으면 404 반환
    if (!user) {
      return res.status(404).json({ ok:false, message: '등록된 이메일이 없습니다.' });
    }

    // 🔹 계정 있음 → 토큰 생성 + 메일 발송
    const { token, hash } = createResetToken();
    const ttlMin = Number(process.env.RESET_TOKEN_TTL_MIN || 30);

    await client.query(
      `INSERT INTO public.auth_password_reset (member_id, token_hash, expires_at)
       VALUES ($1, $2, now() + ($3 || ' minutes')::interval)`,
      [user.member_id, hash, ttlMin]
    );

    const appBase = process.env.APP_BASE_URL || 'http://localhost:5173';
    const link = `${appBase}/#/reset?token=${encodeURIComponent(token)}`;

    const subject = 'Hi-REMS 비밀번호 재설정 안내';
    const text = `아래 링크에서 비밀번호를 재설정하세요 (유효기간 ${ttlMin}분)\n${link}`;
    const html = `
      <p>아래 버튼을 눌러 비밀번호를 재설정하세요. (유효기간 ${ttlMin}분)</p>
      <p><a href="${link}" style="display:inline-block;padding:10px 16px;background:#00b3a4;color:#fff;text-decoration:none;border-radius:6px;">비밀번호 재설정</a></p>
      <p>또는 다음 링크를 복사해 브라우저에 붙여넣기:<br/>${link}</p>
    `;

    await sendMail({ to: user.username, subject, text, html });

    // 계정 있을 때만 ok:true
    return res.json({ ok:true, message: '재설정 안내를 이메일로 발송했습니다.' });
  } catch (e) {
    console.error('[forgot] error:', e);
    return res.status(500).json({ ok:false, message: 'forgot failed' });
  } finally {
    client.release();
  }
});


/* ─────────────────────────────────────────────────────
 * 비밀번호 재설정: 토큰 검증 후 저장
 * ───────────────────────────────────────────────────── */
router.post('/reset', async (req, res) => {
  const client = await pool.connect();
  try {
    const { token, new_password } = req.body || {};
    if (!token || !new_password) {
      return res.status(400).json({ message: 'token/new_password required' });
    }

    const tokenHash = crypto.createHash('sha256').update(token).digest('base64url');

    await client.query('BEGIN');

    const { rows } = await client.query(
      `SELECT r.id, r.member_id, r.expires_at, r.used_at, m.username
         FROM public.auth_password_reset r
         JOIN public.members m ON m.member_id = r.member_id
        WHERE r.token_hash=$1
          AND r.used_at IS NULL
          AND r.expires_at > now()
        FOR UPDATE`,
      [tokenHash]
    );
    const tk = rows[0];
    if (!tk) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'invalid or expired token' });
    }

    const policyErrors = validatePassword(new_password, tk.username);
    if (policyErrors.length) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: policyErrors.join(' ') });
    }

    const hash = await argon2.hash(new_password, {
      type: argon2.argon2id,
      memoryCost: 19456,
      timeCost: 2,
      parallelism: 1,
    });

    await client.query(
      `UPDATE public.members SET password=$1 WHERE member_id=$2`,
      [hash, tk.member_id]
    );

    await client.query(
      `INSERT INTO public.auth_password_history (member_id, password_hash)
       VALUES ($1, $2)`,
      [tk.member_id, hash]
    );

    await client.query(
      `UPDATE public.auth_password_reset SET used_at = now() WHERE id = $1`,
      [tk.id]
    );

    await client.query('COMMIT');

    // (옵션) JWT 발급하여 자동 로그인하려면 아래 주석 해제
    // const access = signAccessToken({ sub: tk.member_id, username: tk.username });
    // return res.cookie('access_token', access, cookieOpts()).json({ ok: true });

    return res.json({ ok: true, message: '비밀번호가 재설정되었습니다. 다시 로그인해 주세요.' });
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    console.error(e);
    return res.status(500).json({ message: 'reset failed' });
  } finally {
    client.release();
  }
});

module.exports = router;
