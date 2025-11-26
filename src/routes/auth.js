const express = require('express');
const argon2 = require('argon2');
const rateLimit = require('express-rate-limit');
const crypto = require('crypto');
const nodemailer = require('nodemailer');

const { pool } = require('../db/db.pg');
const { requireAuth, cookieOpts, signAccessToken } = require('../middlewares/requireAuth');

const router = express.Router();

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

function createResetToken() {
  const token = crypto.randomBytes(32).toString('base64url');
  const hash = crypto.createHash('sha256').update(token).digest('base64url');
  return { token, hash };
}

const loginLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 50,
  standardHeaders: true,
  legacyHeaders: false,
});
const forgotLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
});

router.post('/register', async (req, res) => {
  const client = await pool.connect();
  try {
    const raw = req.body || {};
    const username = String(raw.username || '').trim().toLowerCase();
    const password = String(raw.password || '');
    const worker = String(raw.worker || '').trim();
    const phoneNumber = String(raw.phoneNumber || '').trim();

    if (!username || !password || !worker || !phoneNumber) {
      return res.status(400).json({ message: 'username/password/worker/phoneNumber required' });
    }
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(username)) {
      return res.status(400).json({ message: '이메일 형식이 올바르지 않습니다.' });
    }

    const { rows: dup } = await client.query(
      'SELECT 1 FROM public.members WHERE LOWER(username)=$1',
      [username]
    );
    if (dup.length) {
      return res.status(409).json({ message: '이메일이 존재합니다' });
    }

    await client.query('BEGIN');

    const hash = await argon2.hash(password, {
      type: argon2.argon2id,
      memoryCost: 19456,
      timeCost: 2,
      parallelism: 1,
    });

    const { rows } = await client.query(
      `INSERT INTO public.members (username, password, worker, "phoneNumber")
       VALUES ($1, $2, $3, $4)
       RETURNING member_id, username, password, worker, "phoneNumber"`,
      [username, hash, worker, phoneNumber]
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
      .json({
        user: {
          id: user.member_id,
          username: user.username,
          worker: user.worker,
          phoneNumber: user.phoneNumber
        }
      });
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    console.error(e);
    res.status(500).json({ message: '회원가입에 실패하였습니다' });
  } finally {
    client.release();
  }
});

router.post('/login', loginLimiter, async (req, res) => {
  const { ip, ua } = clientInfo(req);
  try {
    const { username, password } = req.body || {};
    if (!username || !password) {
      await logLoginAttempt({ username, success: false, ip, user_agent: ua, reason: 'missing_fields' });
      return res.status(400).json({ message: 'username/password required' });
    }
    const { rows } = await pool.query(
      'SELECT member_id, username, password, worker, "phoneNumber" FROM public.members WHERE LOWER(username)=$1',
      [String(username).trim().toLowerCase()]
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
      .json({
        user: {
          id: user.member_id,
          username: user.username,
          worker: user.worker,
          phoneNumber: user.phoneNumber
        }
      });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'login failed' });
  }
});

router.post('/change-password', requireAuth, async (req, res) => {
  const client = await pool.connect();
  try {
    const { current_password, new_password } = req.body || {};
    const userId = req.user?.sub;

    if (!current_password || !new_password) {
      return res.status(400).json({ message: 'current_password/new_password required' });
    }

    const { rows } = await client.query(
      'SELECT member_id, username, password FROM public.members WHERE member_id = $1',
      [userId]
    );
    const me = rows[0];
    if (!me) return res.status(401).json({ message: 'unauthorized' });

    const ok = await argon2.verify(me.password, current_password);
    if (!ok) return res.status(401).json({ message: '현재 비밀번호가 올바르지 않습니다.' });

    const policyErrors = validatePassword(new_password, me.username);
    if (policyErrors.length) {
      return res.status(400).json({ message: policyErrors.join(' ') });
    }

    const { rows: hist } = await client.query(
      `SELECT password_hash FROM public.auth_password_history
        WHERE member_id = $1
        ORDER BY created_at DESC NULLS LAST
        LIMIT 5`,
      [me.member_id]
    );
    for (const h of hist) {
      if (await argon2.verify(h.password_hash, new_password)) {
        return res.status(400).json({ message: '최근에 사용한 비밀번호는 다시 사용할 수 없습니다.' });
      }
    }

    const hash = await argon2.hash(new_password, {
      type: argon2.argon2id,
      memoryCost: 19456,
      timeCost: 2,
      parallelism: 1,
    });

    await client.query('BEGIN');

    await client.query(
      `UPDATE public.members SET password = $1 WHERE member_id = $2`,
      [hash, me.member_id]
    );

    await client.query(
      `INSERT INTO public.auth_password_history (member_id, password_hash)
       VALUES ($1, $2)`,
      [me.member_id, hash]
    );

    await client.query(
      `UPDATE public.auth_password_reset
         SET used_at = COALESCE(used_at, now()), expires_at = now()
       WHERE member_id = $1 AND used_at IS NULL AND expires_at > now()`,
      [me.member_id]
    );

    await client.query('COMMIT');
    return res.json({ ok: true, message: '비밀번호가 변경되었습니다. 다시 로그인해 주세요.' });
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('[change-password] error:', e);
    return res.status(500).json({ message: 'change password failed' });
  } finally {
    client.release();
  }
});

router.post('/logout', (req, res) => {
  res.clearCookie('access_token', cookieOpts()).json({ ok: true });
});

router.get('/me', requireAuth, (req, res) => {
  res.json({ user: req.user });
});

router.post('/forgot', forgotLimiter, async (req, res) => {
  const client = await pool.connect();
  try {
    const raw = (req.body && req.body.username) || '';
    const username = String(raw).trim().toLowerCase();
    if (!username) return res.status(400).json({ ok:false, message: 'username required' });

    const { rows } = await client.query(
      'SELECT member_id, username FROM public.members WHERE LOWER(username) = $1',
      [username]
    );
    const user = rows[0];
    if (!user) return res.status(404).json({ ok:false, message: '등록된 이메일이 없습니다.' });

    const { token, hash } = createResetToken();
    const ttlMin = Number(process.env.RESET_TOKEN_TTL_MIN || 30);

    await client.query(
      `INSERT INTO public.auth_password_reset (member_id, token_hash, expires_at)
       VALUES ($1, $2, now() + ($3 || ' minutes')::interval)`,
      [user.member_id, hash, ttlMin]
    );

    const APP_NAME = process.env.APP_NAME || 'Hi-REMS';
    const SUPPORT = process.env.SUPPORT_EMAIL || process.env.SMTP_USER || 'noreply@example.com';

    const appBase = process.env.APP_BASE_URL || 'http://localhost:5173';
    const link = `${appBase}/#/reset?token=${encodeURIComponent(token)}`;

    const subject = `[${APP_NAME}] 비밀번호 재설정 안내`;
    const text =
`안녕하세요, ${APP_NAME} 입니다.

아래 링크에서 새 비밀번호를 설정해 주세요. (유효기간 ${ttlMin}분)
${link}

본인이 요청하지 않았다면 이 메일은 무시하셔도 됩니다.
문의: ${SUPPORT}`;
    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,'Apple SD Gothic Neo','Noto Sans KR',sans-serif; line-height:1.6; color:#222;">
        <h2 style="margin:0 0 12px;">${APP_NAME} 비밀번호 재설정</h2>
        <p>아래 버튼을 눌러 <strong>새 비밀번호</strong>를 설정해 주세요.<br>
           이 링크의 유효기간은 <strong>${ttlMin}분</strong>입니다.</p>
        <p style="margin:16px 0;">
          <a href="${link}" style="display:inline-block;padding:12px 18px;border-radius:8px;background:#00b3a4;color:#fff;text-decoration:none;font-weight:700;">
            비밀번호 재설정
          </a>
        </p>
        <p style="margin:14px 0 6px; font-size:14px; color:#444;">버튼이 클릭되지 않으면 아래 주소를 브라우저 주소창에 복사해 붙여넣기 하세요:</p>
        <p style="word-break:break-all; font-size:13px; color:#555;">${link}</p>
        <hr style="border:none;border-top:1px solid #eee;margin:18px 0;">
        <p style="font-size:13px; color:#666;">
          본인이 요청하지 않았다면 이 메일은 무시하셔도 됩니다.<br>
          문의: <a href="mailto:${SUPPORT}" style="color:#0b7">${SUPPORT}</a>
        </p>
      </div>
    `;

    await sendMail({ to: user.username, subject, text, html });
    return res.json({ ok:true, message: '재설정 안내를 이메일로 발송했습니다.', ttlMin });
  } catch (e) {
    console.error('[forgot] error:', e);
    return res.status(500).json({ ok:false, message: 'forgot failed' });
  } finally {
    client.release();
  }
});

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
