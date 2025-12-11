const express = require('express');
const rateLimit = require('express-rate-limit');
const { pool } = require('../db/db.pg');
const { requireAuth } = require('../middlewares/requireAuth');
const router = express.Router();

router.get('/', requireAuth, async (req, res) => {
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
    
    const records = rows.map(row => {
        let dateStr = null;
        if (row.maintenance_date) {
            const d = new Date(row.maintenance_date);
            const offset = d.getTimezoneOffset() * 60000;
            const localDate = new Date(d.getTime() - offset);
            dateStr = localDate.toISOString().slice(0, 10);
        }

        return {
            id: row.id,
            rtuImei: row.rtuimei,
            maintenanceDate: dateStr,
            asNotes: row.as_notes || null,
            createdAt: row.created_at,
        };
    });

    return res.json({ records: records }); 
  } catch (e) {
    console.error('[maintenance][GET]', e);
    return res.status(500).json({ message: 'maintenance get failed' });
  }
});

router.post('/', requireAuth, async (req, res) => {

  const client = await pool.connect(); 
  
  try {
    console.log('[POST] /maintenance 요청 도착. Body:', req.body);

    const rtuImei = String(req.body.rtuImei || '').trim();
    if (!rtuImei) {
      return res.status(400).json({ message: 'rtuImei required' });
    }

    const maintenanceDate = (req.body && req.body.lastInspection) ? String(req.body.lastInspection).slice(0,10) : null;
    const asNotes = (req.body && typeof req.body.asNotes === 'string') ? req.body.asNotes : null;
    
    console.log('[POST] DB 저장 시도:', { rtuImei, maintenanceDate, asNotes });

    await client.query('BEGIN');

    await client.query(
      `INSERT INTO public.facility_maintenance_history (rtuImei, maintenance_date, as_notes)
       VALUES ($1, $2, $3)`,
      [rtuImei, maintenanceDate || null, asNotes || null]
    );

    await client.query(
      `INSERT INTO public.facility_maintenance (rtuimei, last_inspection, as_notes, updated_at)
       VALUES ($1, $2, $3, NOW())
       ON CONFLICT (rtuimei) 
       DO UPDATE SET 
         last_inspection = EXCLUDED.last_inspection,
         as_notes = EXCLUDED.as_notes,
         updated_at = NOW()`,
      [rtuImei, maintenanceDate || null, asNotes || null]
    );

    await client.query('COMMIT');

    console.log('[POST] DB 저장 완료 (History 추가 + Main 갱신)');

    return res.json({ ok: true, message: 'Maintenance record created and status updated' });

  } catch (e) {
    await client.query('ROLLBACK');
    console.error('[maintenance][POST] 에러 발생:', e);
    return res.status(500).json({ message: 'maintenance creation failed' });
  } finally {
    client.release();
  }
});

router.put('/:id', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    if (!id) return res.status(400).json({ message: 'Record ID required' });

    const maintenanceDate = (req.body && req.body.lastInspection) ? String(req.body.lastInspection).slice(0,10) : null;
    const asNotes = (req.body && typeof req.body.asNotes === 'string') ? req.body.asNotes : null;

    const result = await pool.query(
      `UPDATE public.facility_maintenance_history 
       SET maintenance_date = $1, as_notes = $2
       WHERE id = $3`,
      [maintenanceDate || null, asNotes || null, id]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'Record not found or no changes made' });
    }

    return res.json({ ok: true, message: 'Maintenance record updated' });
  } catch (e) {
    console.error('[maintenance][PUT] 에러 발생:', e);
    return res.status(500).json({ message: 'maintenance update failed' });
  }
});

router.delete('/:id', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    if (!id) return res.status(400).json({ message: 'Record ID required' });

    const result = await pool.query(
      `DELETE FROM public.facility_maintenance_history 
       WHERE id = $1`,
      [id]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ message: 'Record not found' });
    }

    return res.json({ ok: true, message: 'Maintenance record deleted' });
  } catch (e) {
    console.error('[maintenance][DELETE] 에러 발생:', e);
    return res.status(500).json({ message: 'maintenance deletion failed' });
  }
});

module.exports = router;