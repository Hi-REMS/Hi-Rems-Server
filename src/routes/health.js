const express = require('express');
const router = express.Router();
const { pool } = require('../db/db.pg');
const { mysqlPool } = require('../db/db.mysql');

router.get('/', async (_req, res) => {
  try {
    const { rows } = await pool.query('SELECT NOW() AS now');
    res.json({ ok: true, db_now: rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

module.exports = router;
