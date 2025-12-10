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

// 1. 조회 (GET) - 타임존 버그 수정 버전 유지
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

// 2. 등록 (POST) - [수정됨] 부모 테이블(facility_maintenance)도 같이 업데이트
router.post('/', requireAuth, limiter, async (req, res) => {
  // 트랜잭션 처리를 위해 클라이언트 연결
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

    // 트랜잭션 시작 (두 테이블 작업이 모두 성공해야 함)
    await client.query('BEGIN');

    // [Step 1] 이력 테이블(History)에 기록 추가 (N 관계)
    await client.query(
      `INSERT INTO public.facility_maintenance_history (rtuImei, maintenance_date, as_notes)
       VALUES ($1, $2, $3)`,
      [rtuImei, maintenanceDate || null, asNotes || null]
    );

    // [Step 2] 메인 테이블(Maintenance) 현황 업데이트 (1 관계)
    // 설명: 해당 rtuImei가 없으면 INSERT, 있으면 최근 점검일과 비고를 UPDATE 합니다.
    // (이 기능을 위해 rtuimei 컬럼에 UNIQUE 제약조건이 필요합니다)
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

    // 트랜잭션 커밋 (저장 확정)
    await client.query('COMMIT');

    console.log('[POST] DB 저장 완료 (History 추가 + Main 갱신)');

    return res.json({ ok: true, message: 'Maintenance record created and status updated' });

  } catch (e) {
    // 에러 발생 시 롤백 (취소)
    await client.query('ROLLBACK');
    console.error('[maintenance][POST] 에러 발생:', e);
    return res.status(500).json({ message: 'maintenance creation failed' });
  } finally {
    // 연결 해제
    client.release();
  }
});

// 3. 수정 (PUT) - 기존 로직 유지
router.put('/:id', requireAuth, limiter, async (req, res) => {
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

// 4. 삭제 (DELETE) - 기존 로직 유지
router.delete('/:id', requireAuth, limiter, async (req, res) => {
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