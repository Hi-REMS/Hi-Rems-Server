// src/energy/devices.js
const { pool } = require('../db/db.pg');
const { mysqlPool } = require('../db/db.mysql');

const USE_PG_ALIAS = String(process.env.USE_PG_ALIAS || 'false') === 'true';

const isImeiLike = (s) =>
  typeof s === 'string' && s.replace(/[^0-9A-Fa-f\-]/g, '').length >= 8;

async function searchPgAliases(q) {
  if (!USE_PG_ALIAS) return [];
  try {
    const text = `
      WITH hits AS (
        SELECT rtu_imei FROM public.device_info  WHERE lower(display_name) LIKE $1
        UNION
        SELECT rtu_imei FROM public.device_alias WHERE lower(alias)        LIKE $1
      )
      SELECT DISTINCT rtu_imei FROM hits LIMIT 50
    `;
    const { rows } = await pool.query(text, [`%${q.toLowerCase()}%`]);
    return rows.map(r => r.rtu_imei).filter(Boolean);
  } catch (err) {
    return [];
  }
}

function hasMysqlConfig() {
  const { MYSQL_HOST, MYSQL_USER, MYSQL_DB } = process.env;
  return Boolean(MYSQL_HOST && MYSQL_USER && MYSQL_DB);
}

async function searchMariaByName(q) {
  if (!hasMysqlConfig()) return [];

  const like = `%${q}%`;
  let conn;
  try {
    conn = await mysqlPool.getConnection();
  
    const exactSql = `
      SELECT DISTINCT r.rtuimei
      FROM rtu_rtu r
      JOIN rems_rems rr ON rr.rtu_id = r.id
      WHERE rr.worker = ?
      LIMIT 50
    `;
    const [exactRows] = await conn.query(exactSql, [q]);
    if (exactRows?.length) return exactRows.map(r => r.rtuimei).filter(Boolean);

    const likeSql = `
      SELECT DISTINCT r.rtuimei
      FROM rtu_rtu r
      LEFT JOIN rems_rems rr ON rr.rtu_id = r.id
      WHERE
        rr.worker LIKE ?
      LIMIT 50
    `;
    const params = [like, like, like, like, like];
    const [rows] = await conn.query(likeSql, params);
    return rows.map(r => r.rtuimei).filter(Boolean);
  } catch (err) {
    console.warn('[mysql] name search skipped:', err.message);
    return [];
  } finally {
    if (conn) conn.release();
  }f
}

async function resolveImeis(qRaw) {
  const q = (qRaw || '').trim();
  if (!q) return { imeis: [] };

  if (isImeiLike(q)) return { imeis: [q] };

  const pgHits = await searchPgAliases(q);
  const mariaHits = await searchMariaByName(q);
  const all = Array.from(new Set([...pgHits, ...mariaHits])).filter(Boolean);
  return { imeis: all };
}

async function resolveOneImeiOrThrow(qRaw) {
  const q = (qRaw || '').trim();
  const { imeis } = await resolveImeis(q);

  if (imeis.length === 0) {
    const e = new Error('해당 이름/IMEI로 매칭되는 장비가 없습니다.');
    e.status = 404;
    throw e;
  }

  if (imeis.length > 1) {
    let conn;
    try {
      conn = await mysqlPool.getConnection();
      const placeholders = imeis.map(() => '?').join(',');
      const [rows] = await conn.query(
        `
        SELECT r.rtuimei AS imei, rr.worker, rr.address, rr.facCompany, rr.monitorCompany
        FROM rtu_rtu r
        JOIN rems_rems rr ON rr.rtu_id = r.id
        WHERE r.rtuimei IN (${placeholders})
        `
        , imeis
      );
      const e = new Error('여러 장비가 매칭되었습니다. 더 구체적인 이름을 선택하세요.');
      e.status = 422;
      e.matches = rows.map(r => ({
        imei: r.imei,
        name: r.worker,
        address: r.address,
        facCompany: r.facCompany,
        monitorCompany: r.monitorCompany,
      }));
      throw e;
    } finally {
      if (conn) conn.release();
    }
  }

  const imei = imeis[0];
  let name = null;
  try {
    const { rows } = await pool.query(
      `SELECT worker FROM public.imei_meta WHERE imei = $1`,
      [imei]
    );
    if (rows.length > 0 && rows[0].worker) {
      name = rows[0].worker;
    }
  } catch (e) {

  }

  return { imei, name };
}

module.exports = { resolveImeis, resolveOneImeiOrThrow, isImeiLike };