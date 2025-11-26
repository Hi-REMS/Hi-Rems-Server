const express = require('express');
const router = express.Router();
const { pool } = require('../db/db.pg');
const { requireAuth } = require('../middlewares/requireAuth');

async function requireAdmin(req, res, next) {
  try {
    const { sub } = req.user || {};
    if (!sub) return res.status(401).json({ message: 'Unauthorized' });

    const { rows } = await pool.query(
      'SELECT worker FROM public.members WHERE member_id = $1',
      [sub]
    );
    const me = rows[0];

    if (!me || me.worker !== '관리자') {
      return res.status(403).json({ message: '관리자 권한이 필요합니다.' });
    }
    next();
  } catch (e) {
    console.error('[requireAdmin] error:', e);
    res.status(500).json({ message: '관리자 권한 확인 중 오류가 발생했습니다.' });
  }
}

router.get('/', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT member_id, username, worker, "phoneNumber", created_at
         FROM public.members
         ORDER BY created_at DESC`
    );
    res.json(rows);
  } catch (e) {
    console.error('[members] list failed:', e);
    res.status(500).json({ message: '회원 목록 조회 실패' });
  }
});

router.put('/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const { worker, username, phoneNumber } = req.body || {};

    if (username) {
      const { rows: dup } = await pool.query(
        `SELECT member_id FROM public.members
          WHERE username = $1 AND member_id <> $2`,
        [username, id]
      );

      if (dup.length > 0) {
        return res
          .status(409)
          .json({ message: '이미 존재하는 이메일입니다.' });
      }
    }

    await pool.query(
      `UPDATE public.members
          SET worker = COALESCE($1, worker),
              username = COALESCE($2, username),
              "phoneNumber" = COALESCE($3, "phoneNumber")
        WHERE member_id = $4`,
      [worker, username, phoneNumber, id]
    );

    const { rows } = await pool.query(
      `SELECT member_id, username, worker, "phoneNumber", created_at
         FROM public.members
        WHERE member_id = $1`,
      [id]
    );

    if (!rows.length) {
      return res
        .status(404)
        .json({ message: '해당 회원을 찾을 수 없습니다.' });
    }

    res.json({
      ok: true,
      user: rows[0],
      message: '회원 정보가 수정되었습니다.',
    });
  } catch (e) {
    console.error('[members] update failed:', e);
    res.status(500).json({ message: '회원 정보 수정 중 오류가 발생했습니다.' });
  }
});

module.exports = router;
