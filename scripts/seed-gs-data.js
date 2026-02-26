require('dotenv').config();
const fs = require('fs');
const csv = require('csv-parser');
const argon2 = require('argon2');
const { pool } = require('../src/db/db.pg');
const { mysqlPool } = require('../src/db/db.mysql');

const readCsv = (filePath) => {
  return new Promise((resolve, reject) => {
    const results = [];
    if (!fs.existsSync(filePath)) return reject(new Error(`${filePath} 파일이 없습니다.`));
    fs.createReadStream(filePath)
      .pipe(csv({ mapHeaders: ({ header }) => header.replace(/^\ufeff/, '').trim() }))
      .on('data', (data) => results.push(data))
      .on('error', (err) => reject(err))
      .on('end', () => resolve(results));
  });
};

async function seedData() {
  try {
    await pool.query(`DELETE FROM public.log_rtureceivelog WHERE "rtuImei" LIKE '11-22-33%';`);

    const userRows = await readCsv('user_setup.csv');
    const logRows = await readCsv('body_logs.csv');

    for (const row of userRows) {
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
    }
    
    const baselineImeis = new Set();
    for (const log of logRows) {
      if (!baselineImeis.has(log.imei)) {
        const bodyLen = log.body.split(' ').length;
        
        await pool.query(
          `INSERT INTO public.log_rtureceivelog ("rtuImei", "body", "time", "deviceModel", "msgType", "seqSendTime", "opMode", "multiId", "bodyLength", "bodyOptionId") 
           VALUES ($1, $2, '2026-02-01 00:00:00', '00001', '1', '00:00:00', '0', 0, $3, 0)`,
          [log.imei, log.body, bodyLen]
        );


        await pool.query(
          `INSERT INTO public.log_rtureceivelog ("rtuImei", "body", "time", "deviceModel", "msgType", "seqSendTime", "opMode", "multiId", "bodyLength", "bodyOptionId") 
           VALUES ($1, $2, '2026-02-26 00:00:00', '00001', '1', '00:00:00', '0', 0, $3, 0)`,
          [log.imei, log.body, bodyLen]
        );
        
        baselineImeis.add(log.imei);
      }
    }

    for (const log of logRows) {
      const bodyLen = log.body.split(' ').length;
      await pool.query(
        `INSERT INTO public.log_rtureceivelog ("rtuImei", "body", "time", "deviceModel", "msgType", "seqSendTime", "opMode", "multiId", "bodyLength", "bodyOptionId") 
         VALUES ($1, $2, $3, '00001', '1', '00:00:00', $4, 0, $5, 0)`,
        [log.imei, log.body, log.time, log.opMode || '0', bodyLen]
      );
    }

    try {
      await pool.query(`CALL refresh_continuous_aggregate('public.log_rtureceivelog_daily', '2026-01-31', '2026-02-03')`);
      await pool.query(`CALL refresh_continuous_aggregate('public.log_rtureceivelog_daily', '2026-02-25', '2026-02-28')`);
      console.log("동기화 완료");
    } catch (e) {
      console.error("갱신 실패:", e.message);
    }

  } catch (err) {
    console.error("오류 발생:", err.message);
  } finally {
    await pool.end().catch(() => {});
    if (mysqlPool) await mysqlPool.end().catch(() => {});
    process.exit(0);
  }
}

seedData();