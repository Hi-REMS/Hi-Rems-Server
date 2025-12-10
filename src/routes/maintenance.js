const express = require('express');
const rateLimit = require('express-rate-limit');
const { pool } = require('../db/db.pg');
const { requireAuth } = require('../middlewares/requireAuth');
const router = express.Router();

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
   `SELECT id, rtuImei, maintenance_date, as_notes, created_at
    FROM public.facility_maintenance_history 
    WHERE rtuImei = $1
    ORDER BY maintenance_date DESC, created_at DESC`,
   [rtuImei]
  );
    
  const records = rows.map(row => ({
        id: row.id,
        rtuImei: row.rtuimei,
        maintenanceDate: row.maintenance_date ? new Date(row.maintenance_date).toISOString().slice(0,10) : null,
        asNotes: row.as_notes || null,
        createdAt: row.created_at,
    }));

  return res.json({ records: records }); 
 } catch (e) {
  console.error('[maintenance][GET]', e);
  return res.status(500).json({ message: 'maintenance get failed' });
 }
});

router.post('/', requireAuth, limiter, async (req, res) => {
 try {
  const rtuImei = String(req.body.rtuImei || '').trim();
  if (!rtuImei) return res.status(400).json({ message: 'rtuImei required' });

  const maintenanceDate = (req.body && req.body.lastInspection) ? String(req.body.lastInspection).slice(0,10) : null;
  const asNotes = (req.body && typeof req.body.asNotes === 'string') ? req.body.asNotes : null;
    
  await pool.query(
   `INSERT INTO public.facility_maintenance_history (rtuImei, maintenance_date, as_notes)
   VALUES ($1, $2, $3)`,
   [rtuImei, maintenanceDate || null, asNotes || null]
  );

  return res.json({ ok: true, message: 'Maintenance record created' });
 } catch (e) {
  console.error('[maintenance][POST]', e);
  return res.status(500).json({ message: 'maintenance creation failed' });
 }
});

module.exports = router;