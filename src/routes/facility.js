// routes/facility.js
const express = require('express');
const { pool } = require('../db/db.pg');
const { requireAuth } = require('../middlewares/requireAuth');

const router = express.Router();

/** 단건 조회: GET /api/facility?imei=... 또는 ?rtuImei=... */
router.get('/', requireAuth, async (req, res) => {
  const rtuImei = String(req.query.rtuImei || req.query.imei || '').trim();
  if (!rtuImei) return res.status(400).json({ message: 'rtuImei required' });

  const { rows } = await pool.query(
    `SELECT rtuImei, module_capacity, install_date, monitor_start, project_name,
            contractor, as_contact, image_url, created_at, updated_at
       FROM public.facility_info
      WHERE rtuImei = $1`,
    [rtuImei]
  );
  return res.json({ item: rows[0] || null });
});

/** 업서트: PUT /api/facility/:rtuImei */
router.put('/:rtuImei', requireAuth, async (req, res) => {
  const rtuImei = String(req.params.rtuImei || '').trim();
  if (!rtuImei) return res.status(400).json({ message: 'rtuImei required' });

  const b = req.body || {};
  const userId = req.user?.sub || null;

  const fields = [
    'module_capacity','install_date','monitor_start','project_name',
    'contractor','as_contact','image_url'
  ];
  const vals = fields.map(k => (b[k] ?? null));

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const { rows: exists } = await client.query(
      'SELECT 1 FROM public.facility_info WHERE rtuImei=$1', [rtuImei]
    );

    if (exists.length) {
      await client.query(
        `UPDATE public.facility_info
            SET module_capacity=$2, install_date=$3, monitor_start=$4, project_name=$5,
                contractor=$6, as_contact=$7, image_url=$8, updated_by=$9
          WHERE rtuImei=$1`,
        [rtuImei, ...vals, userId]
      );
    } else {
      await client.query(
        `INSERT INTO public.facility_info
           (rtuImei, module_capacity, install_date, monitor_start, project_name,
            contractor, as_contact, image_url, created_by, updated_by)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9)`,
        [rtuImei, ...vals, userId]
      );
    }

    await client.query('COMMIT');
    return res.json({ ok: true });
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('[facility upsert] err:', e);
    return res.status(500).json({ message: 'facility upsert failed' });
  } finally {
    client.release();
  }
});

module.exports = router;
