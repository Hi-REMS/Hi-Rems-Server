// src/energy/devices.js
const { pool } = require('../db/db.pg');          // Postgres (옵션)
const { mysqlPool } = require('../db/db.mysql');  // ✅ 기존 MySQL 풀 재사용

// 환경변수로 Postgres 별칭 검색 사용 여부 제어 (기본: 끔)
const USE_PG_ALIAS = String(process.env.USE_PG_ALIAS || 'false') === 'true';

const isImeiLike = (s) =>
  typeof s === 'string' && s.replace(/[^0-9A-Fa-f\-]/g, '').length >= 8;

/* ---------- (옵션) Postgres 별칭/표준명 검색 ---------- */
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
    // 해당 테이블이 없으면 그냥 패스
    // console.warn('[pg] alias search skipped:', err.message);
    return [];
  }
}

/* ---------- MySQL에서 이름/주소/회사명으로 rtuimei 찾기 ----------
 * 실제 스키마:
 *   - DB: alliothub
 *   - 테이블: rems_rems (worker, address, facCompany, monitorCompany, rtu_id)
 *   - 테이블: rtu_rtu   (id, rtuimei, ...)
 */
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

    // 1) 이름 정확 일치 (대소문자 무시; 한글은 기본 collation에서 대소문 구분 없음)
    const exactSql = `
      SELECT DISTINCT r.rtuimei
      FROM rtu_rtu r
      JOIN rems_rems rr ON rr.rtu_id = r.id
      WHERE rr.worker = ?
      LIMIT 50
    `;
    const [exactRows] = await conn.query(exactSql, [q]);
    if (exactRows?.length) return exactRows.map(r => r.rtuimei).filter(Boolean);

    // 2) 부분 일치 (이름/주소/회사/모니터링사/IMEI)
    const likeSql = `
      SELECT DISTINCT r.rtuimei
      FROM rtu_rtu r
      LEFT JOIN rems_rems rr ON rr.rtu_id = r.id
      WHERE
        r.rtuimei LIKE ? OR
        rr.worker LIKE ? OR
        rr.address LIKE ? OR
        rr.facCompany LIKE ? OR
        rr.monitorCompany LIKE ?
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
  }
}

/* ---------- 입력값을 IMEI 배열로 해석 ---------- */
async function resolveImeis(qRaw) {
  const q = (qRaw || '').trim();
  if (!q) return { imeis: [] };

  // IMEI처럼 보이면 그대로 사용
  if (isImeiLike(q)) return { imeis: [q] };

  // 1) (옵션) Postgres 별칭/표준명
  const pgHits = await searchPgAliases(q);
  // 2) MySQL(rem s_rems + rtu_rtu)에서 이름/주소/회사명 LIKE
  const mariaHits = await searchMariaByName(q);

  // 합치고 중복 제거
  const all = Array.from(new Set([...pgHits, ...mariaHits])).filter(Boolean);
  return { imeis: all };
}

/* ---------- 단일 IMEI 강제 (0개:404, 2개+:422 후보 제공) ---------- */
/* ---------- 단일 IMEI 강제 (0개:404, 2개+:422 후보 제공) ---------- */
async function resolveOneImeiOrThrow(qRaw) {
  const q = (qRaw || '').trim();
  const { imeis } = await resolveImeis(q);

  if (imeis.length === 0) {
    const e = new Error('해당 이름/IMEI로 매칭되는 장비가 없습니다.');
    e.status = 404;
    throw e;
  }

  // ⚙️ 후보가 여러 개인 경우, 세부 정보 조회
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
      // ✅ 프론트에서 모달에 표시할 상세정보 포함
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

  return imeis[0];
}


module.exports = { resolveImeis, resolveOneImeiOrThrow, isImeiLike };
