// src/db/db.routes.test.js

const express = require('express');
const router = express.Router();

const { pool } = require('./db.pg');
const { mysqlPool } = require('./db.mysql');

// PostgreSQL 핑
router.get('/ping-db', async (_req, res, next) => {
  try {
    const { rows } = await pool.query(
      'SELECT 1 AS ok, NOW() AS now, version() AS version'
    );
    res.json(rows[0]);
  } catch (e) { next(e); }
});

// MySQL/MariaDB 핑
router.get('/ping-mysql', async (_req, res, next) => {
  try {
    const [rows] = await mysqlPool.query(
      'SELECT 1 AS ok, NOW() AS `now`, VERSION() AS `version`'
    );
    res.json(rows[0]);
  } catch (e) { next(e); }
});

module.exports = router;
