const express = require('express');
const router = express.Router();
const { pool } = require('../db/db.pg');
const rateLimit = require('express-rate-limit');
const logsLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 20,
  message: { error: 'Too many requests — try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});

router.get('/', logsLimiter, async (req, res, next) => {
  try {
    const limit  = Math.min(parseInt(req.query.limit  || '50', 10), 200);
    const offset = Math.max(parseInt(req.query.offset || '0',  10), 0);
    const { imei, deviceModel, msgType } = req.query;

    const conds = [];
    const args  = [];
    if (imei)        { args.push(imei);        conds.push(`"rtuImei" = $${args.length}`); }
    if (deviceModel) { args.push(deviceModel); conds.push(`"deviceModel" = $${args.length}`); }
    if (msgType)     { args.push(msgType);     conds.push(`"msgType" = $${args.length}`); }
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';

    args.push(limit);  const limIdx = args.length;
    args.push(offset); const offIdx = args.length;

    const sql = `
      SELECT id, time, "deviceModel", "msgType", "opMode", "multiId", "rtuImei",
             "bodyLength",
             LEFT(body, 80) || CASE WHEN LENGTH(body) > 80 THEN '…' ELSE '' END AS body_preview
      FROM public."log_rtureceivelog"
      ${where}
      ORDER BY time DESC
      LIMIT $${limIdx} OFFSET $${offIdx}
    `;

    const { rows } = await pool.query(sql, args);
    res.json(rows);
  } catch (e) {
    next(e);
  }
});

module.exports = router;
