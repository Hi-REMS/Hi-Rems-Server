// src/db/init.mysql.js
const { mysqlPool } = require('./db.mysql');

const initMysql = async () => {
  let connection;
  try {
    connection = await mysqlPool.getConnection();
    console.log('테이블 초기화 확인 중...');

    await connection.query(`
      CREATE TABLE IF NOT EXISTS rtu_rtu (
        id BIGINT NOT NULL AUTO_INCREMENT,
        rtuImei VARCHAR(23) NOT NULL,
        koremsParsingLogicId INT NOT NULL,
        koremsExternalApiId INT NOT NULL,
        koremsPowerplantId INT NOT NULL,
        status VARCHAR(1) NOT NULL,
        createdDate DATETIME(6) NOT NULL,
        externalApiId INT NOT NULL,
        deviceDivision VARCHAR(5) NOT NULL,
        businessId INT NOT NULL,
        serviceId INT NOT NULL,
        fwVersion VARCHAR(11) NULL,
        masterServerId INT NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY (rtuImei),
        INDEX (koremsParsingLogicId),
        INDEX (koremsExternalApiId),
        INDEX (status),
        INDEX (externalApiId),
        INDEX (deviceDivision),
        INDEX (businessId),
        INDEX (serviceId),
        INDEX (fwVersion)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    await connection.query(`
      CREATE TABLE IF NOT EXISTS rems_rems (
        id BIGINT NOT NULL AUTO_INCREMENT,
        cid VARCHAR(25) NOT NULL,
        authKey VARCHAR(100) NOT NULL,
        worker VARCHAR(25) NOT NULL,
        phoneNumber VARCHAR(25) NULL,
        address VARCHAR(100) NULL,
        facCompany VARCHAR(25) NULL,
        monitorCompany VARCHAR(25) NULL,
        multiId INT UNSIGNED NOT NULL,
        token VARCHAR(100) NULL,
        createdDate DATETIME(6) NOT NULL,
        rtu_id BIGINT NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY (cid),
        INDEX (rtu_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    console.log(' 생성 완료');
  } catch (err) {
    console.error(' MariaDB 초기화 중 에러 발생:', err.message);
  } finally {
    if (connection) connection.release();
  }
};

module.exports = { initMysql };

if (require.main === module) {
  require('dotenv').config();
  initMysql()
    .then(() => {
      console.log('MariaDB 스크립트 실행 종료');
      process.exit(0);
    })
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
}