const express = require('express');
const rateLimit = require('express-rate-limit');
const { pool } = require('../db/db.pg');
const { requireAuth } = require('../middlewares/requireAuth');

const router = express.Router();

const rd = (row) => {
  if (!row) return null;
  return {
    rtuImei: row.rtuimei,
    lastInspection: row.last_inspection ? row.last_inspection.toISOString().slice(0,10) : null,
    asNotes: row.as_notes || null,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
};

const limiter = rateLimit({
  windowMs: 60 * 1000,
  max: 200,
  standardHeaders: true,
  legacyHeaders: false,
});

router.get('/', requireAuth, limiter, async (req, res) => {
  try {
    const rtuImei = String(req.query.rtuImei || '').trim();
    if (!rtuImei) return res.status(400).json({ message: 'rtuImei required' });

    const { rows } = await pool.query(
      `SELECT rtuImei, last_inspection, as_notes, created_at, updated_at
         FROM public.facility_maintenance
        WHERE rtuImei = $1`,
      [rtuImei]
    );
    return res.json({ item: rd(rows[0]) });
  } catch (e) {
    console.error('[maintenance][GET]', e);
    return res.status(500).json({ message: 'maintenance get failed' });
  }
});

router.put('/:rtuImei', requireAuth, limiter, async (req, res) => {
  try {
    const rtuImei = String(req.params.rtuImei || '').trim();
    if (!rtuImei) return res.status(400).json({ message: 'rtuImei required' });

    const lastInspection = (req.body && req.body.lastInspection) ? String(req.body.lastInspection).slice(0,10) : null;
    const asNotes = (req.body && typeof req.body.asNotes === 'string') ? req.body.asNotes : null;

    await pool.query(
      `INSERT INTO public.facility_maintenance (rtuImei, last_inspection, as_notes)
       VALUES ($1, $2, $3)
       ON CONFLICT (rtuImei) DO UPDATE
           SET last_inspection = EXCLUDED.last_inspection,
               as_notes = EXCLUDED.as_notes,
               updated_at = now()`,
      [rtuImei, lastInspection || null, asNotes || null]
    );

    return res.json({ ok: true });
  } catch (e) {
    console.error('[maintenance][PUT]', e);
    return res.status(500).json({ message: 'maintenance upsert failed' });
  }
});

module.exports = router;
