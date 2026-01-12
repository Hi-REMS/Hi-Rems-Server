const jwt = require('jsonwebtoken');

function getExpiresInMs() {
  const expires = process.env.ACCESS_TOKEN_EXPIRES || '60m';
  const value = parseInt(expires);
  if (expires.endsWith('m')) return value * 60 * 1000;
  if (expires.endsWith('h')) return value * 60 * 60 * 1000;
  if (expires.endsWith('s')) return value * 1000;
  return 60 * 60 * 1000;
}

function cookieOpts() {
  const prod = process.env.NODE_ENV === 'production';
  const domain = process.env.COOKIE_DOMAIN || undefined;
  return {
    httpOnly: true,
    secure: prod,
    sameSite: prod ? 'none' : 'lax',
    domain,
    path: '/',
    maxAge: getExpiresInMs(),
  };
}

function signAccessToken(payload, sess) {
  if (!payload || payload.sub == null || !payload.username) {
    throw new Error('signAccessToken: payload must include { sub, username }');
  }
  return jwt.sign(
    { 
      username: payload.username, 
      is_admin: !!payload.is_admin,
      sess 
    },
    process.env.JWT_ACCESS_SECRET,
    {
      subject: String(payload.sub),
      expiresIn: process.env.ACCESS_TOKEN_EXPIRES || '60m',
      algorithm: 'HS256',
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
    const payload = jwt.verify(token, process.env.JWT_ACCESS_SECRET, {
      algorithms: ['HS256'],
      clockTolerance: 5,
    });

    const sess = typeof payload.sess === 'number'
      ? payload.sess
      : (payload.iat ? payload.iat * 1000 : Date.now());

    const now = Date.now();
    
    const ABSOLUTE_MAX_MS = getExpiresInMs(); 
    
    if (now - sess > ABSOLUTE_MAX_MS) {
      return res.status(401).json({ message: 'Session expired (Absolute timeout)' });
    }

    const newAccess = signAccessToken(
      { sub: payload.sub, username: payload.username, is_admin: payload.is_admin },
      sess
    );

    res.setHeader('X-New-Token', newAccess);
    res.setHeader('Access-Control-Expose-Headers', 'X-New-Token');

    if (res.cookie) {
      res.cookie('access_token', newAccess, cookieOpts());
    }

    req.user = { 
      sub: payload.sub, 
      username: payload.username, 
      is_admin: !!payload.is_admin 
    };
    return next();
  } catch (e) {
    return res.status(401).json({ message: 'Invalid or expired token' });
  }
}

function requireAdmin(req, res, next) {
  requireAuth(req, res, () => {
    if (!req.user || !req.user.is_admin) {
      return res.status(403).json({ message: 'Forbidden: Admin access required' });
    }
    next();
  });
}

module.exports = { requireAuth, requireAdmin, cookieOpts, signAccessToken };