// src/middlewares/requireAuth.js
const jwt = require('jsonwebtoken');

/* 세션 쿠키 옵션 (auth.js와 동일해야 함) */
function cookieOpts() {
  const prod = process.env.NODE_ENV === 'production';
  const domain = process.env.COOKIE_DOMAIN || undefined;
  return {
    httpOnly: true,
    secure: prod,
    sameSite: prod ? 'none' : 'lax',
    domain,
    // 세션 쿠키 → maxAge 없음
  };
}

/* AccessToken 재발급 함수 */
function signAccessToken(payload, sess) {
  return jwt.sign(
    { username: payload.username, sess },
    process.env.JWT_ACCESS_SECRET,
    {
      subject: String(payload.sub),
      expiresIn: process.env.ACCESS_TOKEN_EXPIRES || '15m',
    }
  );
}

function requireAuth(req, res, next) {
  const bearer = req.headers.authorization || '';
  const token = bearer.startsWith('Bearer ')
    ? bearer.slice(7)
    : (req.cookies && req.cookies.access_token) || '';

  if (!token) return res.status(401).json({ message: 'Unauthorized' });

  try {
    const payload = jwt.verify(token, process.env.JWT_ACCESS_SECRET);

    // --- 세션 절대 만료 체크 ---
    const sess = typeof payload.sess === 'number'
      ? payload.sess
      : (payload.iat ? payload.iat * 1000 : Date.now());

    const now = Date.now();
    const ABSOLUTE_MAX_MS = 60 * 60 * 1000; // 1시간
    if (now - sess > ABSOLUTE_MAX_MS) {
      return res.status(401).json({ message: 'Session expired' });
    }

    // --- 슬라이딩 만료 처리 (만료 5분 전이면 새 토큰 발급) ---
    const expMs = payload.exp * 1000;
    const willExpireSoon = expMs - now <= 5 * 60 * 1000; // 5분 이내
    if (willExpireSoon && res.cookie) {
      const newAccess = signAccessToken(payload, sess);
      res.cookie('access_token', newAccess, cookieOpts());
    }

    // 사용자 정보 주입
    req.user = { id: payload.sub, username: payload.username };
    return next();
  } catch (e) {
    return res.status(401).json({ message: 'Invalid or expired token' });
  }
}

module.exports = { requireAuth };
