// src/middlewares/requireAuth.js
const jwt = require('jsonwebtoken');

function requireAuth(req, res, next) {
  const bearer = req.headers.authorization || '';
  const token = bearer.startsWith('Bearer ')
    ? bearer.slice(7)
    : (req.cookies && req.cookies.access_token) || '';

  if (!token) return res.status(401).json({ message: 'Unauthorized' });

  try {
    const payload = jwt.verify(token, process.env.JWT_ACCESS_SECRET);
    req.user = { id: payload.sub, username: payload.username };
    return next();
  } catch (e) {
    return res.status(401).json({ message: 'Invalid or expired token' });
  }
}

module.exports = { requireAuth };
