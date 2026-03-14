const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });

const fs = require('fs');
const csv = require('csv-parser');
const argon2 = require('argon2');
const { pool } = require('../src/db/db.pg');
const { mysqlPool } = require('../src/db/db.mysql');

const readCsv = (fileName) => {
  const filePath = path.join(__dirname, fileName);
  return new Promise((resolve, reject) => {
    const results = [];
    if (!fs.existsSync(filePath)) {
      return reject(new Error(`파일을 찾을 수 없습니다: ${filePath}`));
    }
    fs.createReadStream(filePath)
      .pipe(csv({ mapHeaders: ({ header }) => header.replace(/^\ufeff/, '').trim() }))
      .on('data', (data) => results.push(data))
      .on('error', (err) => reject(err))
      .on('end', () => resolve(results));
  });
};

async function seedData() {
  console.log("데이터 시딩을 시작");
  
  try {
    const userRows = await readCsv('user_setup.csv');
    const logRows = await readCsv('body_logs.csv');
    console.log(`파일 로드 완료: 사용자(${userRows.length}건), 로그(${logRows.length}건)`);

    await pool.query(`DELETE FROM public.log_rtureceivelog WHERE "rtuImei" LIKE '11-22-33%';`);
    await pool.query(`DELETE FROM public.log_remssendlog WHERE "rtuImei" LIKE '11-22-33%';`);
    console.log("기존 PostgreSQL 테스트 데이터 삭제 완료");

    for (const row of userRows) {
      try {
        const hashedPassword = await argon2.hash(row.password, {
          type: argon2.argon2id, memoryCost: 19456, timeCost: 2, parallelism: 1,
        });

        await pool.query(
          `INSERT INTO public.members (username, password, worker, "phoneNumber", is_admin)
           VALUES ($1, $2, $3, $4, $5) ON CONFLICT (username) DO NOTHING`,
          [row.userId, hashedPassword, row.worker, row.phoneNumber, false]
        );

        await pool.query(
          `INSERT INTO public.imei_meta (imei, worker, "phoneNumber", address, energy_hex, type_hex, sido, sigungu, lat, lon)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) 
           ON CONFLICT (imei) DO UPDATE SET energy_hex=$5, type_hex=$6`,
          [row.imei, row.worker, row.phoneNumber, row.address, row.energy_hex, row.type_hex, '경상남도', '창원시 의창구', 35.2476, 128.6258]
        );

        await pool.query(
          `INSERT INTO public.log_remssendlog ("rtuImei", "cid", "time", "multiId", "result")
           VALUES ($1, $2, NOW(), 0, '1')`,
          [row.imei, row.cid]
        );


        await mysqlPool.query(
          `INSERT INTO alliothub.rems_rems (cid, authKey, worker, multiId, address, createdDate, rtu_id)
           VALUES (?, ?, ?, ?, ?, NOW(), ?)
           ON DUPLICATE KEY UPDATE address = VALUES(address), worker = VALUES(worker)`,
          [row.cid, 'SEED_AUTH_KEY', row.worker, 0, row.address, 1]
        );

        console.log(`처리 완료: IMEI(${row.imei}) -> CID(${row.cid})`);

      } catch (e) {
        console.error(`사용자(${row.userId}) 또는 IMEI(${row.imei}) 처리 중 에러:`, e.message);
      }
    }
    console.log("사용자, 기기, 시설(MariaDB) 정보 연동 완료");

    const baselineImeis = new Set();
    for (const log of logRows) {
      if (!baselineImeis.has(log.imei)) {
        const bodyLen = log.body.split(' ').length;
        const baseInsertQuery = `
          INSERT INTO public.log_rtureceivelog 
          ("rtuImei", "body", "time", "deviceModel", "msgType", "seqSendTime", "opMode", "multiId", "bodyLength", "bodyOptionId") 
          VALUES ($1, $2, $3, '00001', '1', '00:00:00', '0', 0, $4, 0)`;

        await pool.query(baseInsertQuery, [log.imei, log.body, '2026-02-01 00:00:00', bodyLen]);
        await pool.query(baseInsertQuery, [log.imei, log.body, '2026-02-26 00:00:00', bodyLen]);
        
        baselineImeis.add(log.imei);
      }
    }

    console.log("로그 데이터 삽입 중");
    for (const log of logRows) {
      const bodyLen = log.body.split(' ').length;
      await pool.query(
        `INSERT INTO public.log_rtureceivelog ("rtuImei", "body", "time", "deviceModel", "msgType", "seqSendTime", "opMode", "multiId", "bodyLength", "bodyOptionId") 
         VALUES ($1, $2, $3, '00001', '1', '00:00:00', $4, 0, $5, 0)`,
        [log.imei, log.body, log.time, log.opMode || '0', bodyLen]
      );
    }

try {
      await pool.query(`CALL refresh_continuous_aggregate('public.log_rtureceivelog_daily', '2026-01-31', '2026-03-17')`);
      console.log("통계 데이터(Daily) 동기화 완료");

      await pool.query(`REFRESH MATERIALIZED VIEW public.mv_energy_recent;`);
      console.log("실시간 분석 뷰(mv_energy_recent) 갱신 완료");

      await pool.query(`DELETE FROM public.energy_nationwide_cache;`);
      console.log("기존 통계 캐시(energy_nationwide_cache) 삭제 완료");

    } catch (e) {
      console.error("통계/뷰/캐시 갱신 실패:", e.message);
    }

    console.log("✨ 모든 시딩 작업 및 데이터 동기화가 성공적으로 완료되었습니다.");

  } catch (err) {
    console.error("작업 중 오류 발생:");
    console.error(err); 
  } finally {
    if (pool) await pool.end().catch(() => {});
    if (mysqlPool) await mysqlPool.end().catch(() => {});
    console.log("DB 연결 종료");
    process.exit(0);
  }
}

seedData();