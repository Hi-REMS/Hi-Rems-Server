const express = require('express');
const { pool } = require('../db/db.pg');
const { requireAuth } = require('../middlewares/requireAuth');
const router = express.Router();
const isValidDate = (dateString) => {
  if (!dateString || dateString.trim() === '') return true;
  
  const date = new Date(dateString);
  return !isNaN(date.getTime());
};

router.get('/', requireAuth, async (req, res) => {
  try {
    const rtuImei = String(req.query.rtuImei || req.query.imei || '').trim();
    
    if (!rtuImei) {
      return res.status(400).json({ message: 'rtuImei 파라미터가 필요합니다.' });
    }

    const { rows } = await pool.query(
      `SELECT rtuImei, module_capacity, install_date, monitor_start, project_name,
              contractor, as_contact, image_url, created_at, updated_at
         FROM public.facility_info
        WHERE rtuImei = $1`,
      [rtuImei]
    );

    return res.json({ item: rows[0] || null });

  } catch (e) {
    console.error(`[시설물 조회 실패] rtuImei: ${req.query.rtuImei}`, e);
    return res.status(500).json({ message: '서버 내부 오류가 발생했습니다.' });
  }
});

/** * 시설물 등록 또는 수정 (Upsert)
 * PUT /api/facility/:rtuImei 
 */
router.put('/:rtuImei', requireAuth, async (req, res) => {
  const client = await pool.connect();

  try {
    // 1. [기본 검증] rtuImei 확인
    const rtuImei = String(req.params.rtuImei || '').trim();
    if (!rtuImei) {
      return res.status(400).json({ message: 'URL 경로에 rtuImei가 없습니다.' });
    }

    const b = req.body || {};
    const userId = req.user?.sub || 'system';

    // ---------------------------------------------------------
    // 2. [날짜 예외 처리] 날짜 형식이 이상하면 즉시 차단 (400)
    // ---------------------------------------------------------
    if (!isValidDate(b.install_date)) {
      return res.status(400).json({ 
        message: `'install_date' 형식이 올바르지 않습니다. (입력값: ${b.install_date})` 
      });
    }
    if (!isValidDate(b.monitor_start)) {
      return res.status(400).json({ 
        message: `'monitor_start' 형식이 올바르지 않습니다. (입력값: ${b.monitor_start})` 
      });
    }

    const cleanDate = (d) => (d && d.trim() !== '' ? d : null);
    
    const cleanVal = (v) => (v !== undefined && v !== '' ? v : null);

    const vals = [
      cleanVal(b.module_capacity),
      cleanDate(b.install_date),
      cleanDate(b.monitor_start),
      cleanVal(b.project_name),
      cleanVal(b.contractor),
      cleanVal(b.as_contact),
      cleanVal(b.image_url)
    ];

    await client.query('BEGIN');

    const { rows: exists } = await client.query(
      'SELECT 1 FROM public.facility_info WHERE rtuImei = $1', 
      [rtuImei]
    );

    let actionType = '';

    if (exists.length > 0) {
      actionType = '수정되었습니다';
      await client.query(
        `UPDATE public.facility_info
            SET module_capacity=$2, install_date=$3, monitor_start=$4, project_name=$5,
                contractor=$6, as_contact=$7, image_url=$8, updated_by=$9, updated_at=NOW()
          WHERE rtuImei=$1`,
        [rtuImei, ...vals, userId]
      );
    } else {
      actionType = '등록되었습니다';
      await client.query(
        `INSERT INTO public.facility_info
           (rtuImei, module_capacity, install_date, monitor_start, project_name,
            contractor, as_contact, image_url, created_by, updated_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`,
        [rtuImei, ...vals, userId]
      );
    }

    await client.query('COMMIT');
    
    return res.json({ ok: true, message: `성공적으로 ${actionType}.` });

  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    
    console.error(`[시설물 저장 실패] rtuImei: ${req.params.rtuImei}`, e);

    if (e.message && e.message.includes('invalid input syntax for type date')) {
        return res.status(400).json({ message: '날짜 형식이 데이터베이스에서 허용되지 않습니다.' });
    }

    if (e.code === '23505') {
      return res.status(409).json({ message: '동시 요청으로 인한 데이터 충돌이 발생했습니다.' });
    }
    
    return res.status(500).json({ message: '시설물 저장 중 오류가 발생했습니다.', error: e.message });

  } finally {
    client.release();
  }
});

module.exports = router;