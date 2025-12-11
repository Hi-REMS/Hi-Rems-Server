require('dotenv').config();
const express = require('express');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const cookieParser = require('cookie-parser');
const api = require('./api');
const path = require('path');
const fs = require('fs');
const swaggerUi = require('swagger-ui-express');
const YAML = require('yamljs');

const { setupEnergyCron } = require('./jobs/energyRefresh');
const { getNormalPointsCached } = require('./jobs/normalPointCache');
const app = express();
app.use(express.json());
app.use(cookieParser());
app.set('trust proxy', 1);

const whitelist = (process.env.CORS_ORIGIN || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

app.use(cors({
  origin(origin, cb) {
    if (!origin) return cb(null, true);
    const allowed = whitelist.some(w => origin.startsWith(w));
    if (allowed) return cb(null, true);
    cb(new Error(`CORS blocked: ${origin}`));
  },
  credentials: true,
}));

app.get('/health', (_req, res) => {
  res.status(200).json({ ok: true, uptime: process.uptime() });
});

const globalLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 10000,
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => (req.path || '') === '/api/health-direct',
});
app.use(globalLimiter);

try {
  const swaggerSpec = YAML.load(path.join(__dirname, '../swagger.yaml'));
  app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpec));
  console.log('📄 Swagger UI is available at /api-docs');
} catch (err) {
  console.error('⚠️ Failed to load swagger.yaml:', err.message);
}

app.get('/swagger.yaml', (req, res) => {
  res.sendFile(path.join(__dirname, '../swagger.yaml'));
});

app.use('/api', api);

app.get('/api/health-direct', async (_req, res) => {
  try {
    const { pool } = require('./db/db.pg');
    const { rows } = await pool.query('SELECT NOW() as now');
    res.json({ ok: true, db_now: rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

setupEnergyCron();

const dist = path.join(__dirname, '../frontend/dist');
app.get(/^\/(?!api\/).*/, async (req, res, next) => {
  if (req.path.startsWith('/api-docs') || req.path === '/swagger.yaml') return next();

  try {
    let normalPoints = [];
    try {
      normalPoints = await getNormalPointsCached();
    } catch (err) {
      console.warn('[WARN] Failed to load normalPoints cache:', err.message);
    }

    const htmlPath = path.join(dist, 'index.html');
    
    if (!fs.existsSync(htmlPath)) {
       return res.status(404).send('Frontend build not found.');
    }

    let html = fs.readFileSync(htmlPath, 'utf8');
    const preloadScript = `<script>window.__NORMAL_POINTS__=${JSON.stringify(normalPoints)};</script>`;
    html = html.replace('', preloadScript);

    res.setHeader('Cache-Control', 'no-cache');
    res.type('html').send(html);
  } catch (err) {
    next(err);
  }
});

app.use((err, _req, res, _next) => {
  console.error(err);
  const status = err.status || 500;
  const body = { error: err.message || 'Server Error' };
  if (status === 422 && Array.isArray(err.matches)) body.matches = err.matches;
  res.status(status).json(body);
});

const port = Number(process.env.PORT || 3000);
app.listen(port, () => {
  console.log(`🚀 API listening on port ${port} (env: ${process.env.NODE_ENV})`);
  console.log(`📄 Swagger Docs: http://localhost:${port}/api-docs`);
});