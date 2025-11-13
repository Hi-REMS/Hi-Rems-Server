const express = require('express');
const router = express.Router();
const { pool } = require('../db/db.pg');
const { requireAuth } = require('../middlewares/requireAuth');

// ──────────────────────────────────────────────
// (옵션) 관리자 여부 체크
// ──────────────────────────────────────────────
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

// ──────────────────────────────────────────────
// 1️⃣ 회원 목록 조회 (관리자만)
// ──────────────────────────────────────────────
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

// ──────────────────────────────────────────────
// 2️⃣ 회원 정보 수정 (worker, username, phoneNumber 등)
// ──────────────────────────────────────────────
router.put('/:id', requireAuth, requireAdmin, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const { worker, username, phoneNumber } = req.body || {};

    // 🔹 1) 이메일 중복 체크 (자기 자신 제외)
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

    // 🔹 2) 회원 정보 업데이트
    await pool.query(
      `UPDATE public.members
          SET worker = COALESCE($1, worker),
              username = COALESCE($2, username),
              "phoneNumber" = COALESCE($3, "phoneNumber")
        WHERE member_id = $4`,
      [worker, username, phoneNumber, id]
    );

    // 🔹 3) 수정된 사용자 정보 조회 후 반환
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
