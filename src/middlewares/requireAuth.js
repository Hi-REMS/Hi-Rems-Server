// middlewares/requireAuth.js
const jwt = require('jsonwebtoken');

/* 세션 쿠키 옵션 */
function cookieOpts() {
  const prod = process.env.NODE_ENV === 'production';
  const domain = process.env.COOKIE_DOMAIN || undefined;
  return {
    httpOnly: true,
    secure: prod,                 // 운영환경은 HTTPS 권장
    sameSite: prod ? 'none' : 'lax',
    domain,                       // 예: .example.com
    path: '/',                    // 전체 경로에서만 유효
    // maxAge 미지정 → 세션 쿠키
  };
}

/**
 * AccessToken 발급함수
 * @param {{ sub: string|number, username: string }} payload  // ⚠️ sub, username 필수
 * @param {number=} sess  // 최초 로그인 시각(ms). 슬라이딩 갱신 시 유지
 */
function signAccessToken(payload, sess) {
  if (!payload || payload.sub == null || !payload.username) {
    throw new Error('signAccessToken: payload must include { sub, username }');
  }
  return jwt.sign(
    { username: payload.username, sess }, // 커스텀 클레임
    process.env.JWT_ACCESS_SECRET,
    {
      subject: String(payload.sub),       // 표준 sub 클레임
      expiresIn: process.env.ACCESS_TOKEN_EXPIRES || '15m',
      algorithm: 'HS256',
    }
  );
}

function requireAuth(req, res, next) {
  // 우선순위: Authorization: Bearer → 쿠키 access_token
  const bearer = req.headers.authorization || '';
  const token = bearer.startsWith('Bearer ')
    ? bearer.slice(7)
    : (req.cookies && req.cookies.access_token) || '';

  if (!token) return res.status(401).json({ message: 'Unauthorized' });

  try {
    const payload = jwt.verify(token, process.env.JWT_ACCESS_SECRET, {
      algorithms: ['HS256'],   // 알고리즘 고정
      clockTolerance: 5,       // 시계 오차 허용(초)
    });

    // --- 절대 세션 만료(예: 1시간) ---
    const sess = typeof payload.sess === 'number'
      ? payload.sess
      : (payload.iat ? payload.iat * 1000 : Date.now());

    const now = Date.now();
    const ABSOLUTE_MAX_MS = 60 * 60 * 1000; // 1h
    if (now - sess > ABSOLUTE_MAX_MS) {
      return res.status(401).json({ message: 'Session expired' });
    }

    // --- 슬라이딩 만료: 만료 5분 전이면 새 토큰을 재발급해서 쿠키에 세팅 ---
    const expMs = payload.exp * 1000;
    const willExpireSoon = expMs - now <= 5 * 60 * 1000;
    if (willExpireSoon && res.cookie) {
      const newAccess = signAccessToken(
        { sub: payload.sub, username: payload.username },
        sess
      );
      res.cookie('access_token', newAccess, cookieOpts());
    }

    // 요청 객체에 사용자 정보 주입
    req.user = { sub: payload.sub, username: payload.username };
    return next();
  } catch (e) {
    return res.status(401).json({ message: 'Invalid or expired token' });
  }
}

module.exports = { requireAuth, cookieOpts, signAccessToken };
