const express = require('express');
const router = express.Router();
const { pool } = require('../db/db.pg');
const rateLimit = require('express-rate-limit');


router.get('/', async (req, res, next) => {
  try {
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = Math.max(parseInt(req.query.offset || '0',  10), 0);
    const { rows } = await pool.query(
      'SELECT id, product, qty, status FROM orders ORDER BY id DESC LIMIT $1 OFFSET $2',
      [limit, offset]
    );

    res.json(rows);
  } catch (e) {
    next(e);
  }
});

module.exports = router;
