// server/routes/user.js
const express = require('express');
const router = express.Router();
const { pool } = require('../db/db.pg');
const { mysqlPool } = require('../db/db.mysql');
const { requireAuth } = require('../middlewares/requireAuth');

/**
 * GET /user/imeis
 * 로그인한 사용자의 worker + phoneNumber로 rems_rems를 조회하여
 * 연결된 RTU의 rtuimei 목록을 반환한다.
 * 반환: { worker, phoneNumber, items:[{rtu_id, rtuImei, createdDate}], defaultImei }
 */
router.get('/imeis', requireAuth, async (req, res) => {
  const client = await pool.connect();
  let mysqlConn;
  try {
    const meSql = `SELECT worker, "phoneNumber" FROM public.members WHERE member_id = $1`;
    const { rows } = await client.query(meSql, [req.user.sub]);
    const me = rows[0];
    if (!me) return res.status(401).json({ message: 'unauthorized' });

    const worker = String(me.worker || '').trim();
    const phoneRaw = String(me.phoneNumber || '').trim();
    const phoneDigits = phoneRaw.replace(/\D/g, '');

    mysqlConn = await mysqlPool.getConnection();
    const q = `
      SELECT rr.rtu_id, rr.worker, rr.phoneNumber, rr.createdDate, r.rtuimei
      FROM rems_rems rr
      JOIN rtu_rtu r ON r.id = rr.rtu_id
      WHERE rr.worker = ?
        AND REPLACE(rr.phoneNumber, '-', '') = ?
      ORDER BY rr.createdDate DESC
      LIMIT 50
    `;
    const [rows2] = await mysqlConn.query(q, [worker, phoneDigits]);

    const items = rows2.map(r => ({
      rtu_id: r.rtu_id,
      rtuImei: r.rtuimei,
      createdDate: r.createdDate
    }));

    const defaultImei = items[0]?.rtuImei || null;

    return res.json({
      worker, phoneNumber: phoneRaw,
      items,
      defaultImei
    });
  } catch (e) {
    console.error('[GET /user/imeis] err:', e);
    return res.status(500).json({ message: 'failed to resolve imeis' });
  } finally {
    client.release();
    if (mysqlConn) mysqlConn.release();
  }
});

module.exports = router;
