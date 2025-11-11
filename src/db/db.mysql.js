/*
커넥션 풀 생성하는 모듈, Promise 기반 API로 await를 써서 query, getConnection 등을 호출
*/
const mysql = require('mysql2/promise');
require('dotenv').config(); // ✅ .env 파일 로드

/*
환경변수 불러오기
MYSQL_SSL = true 일 시 SSL 연결을 강제로
MYSQL_CONN_LIMIT은 커넥션 풀에서 동시에 사용할 수 있는 최대 연결 개수를 10개로 제한
*/
const {
  MYSQL_HOST,
  MYSQL_PORT = '3307',
  MYSQL_DB,
  MYSQL_USER,
  MYSQL_PASS,
  MYSQL_SSL = 'false',
  MYSQL_CONN_LIMIT = '10',
} = process.env;

/*
커넥션 풀 생성
- host가 지정되지 않으면 localhost → socket 모드로 연결돼 ECONNREFUSED 발생 가능
- 따라서 host 기본값을 127.0.0.1로 강제
*/
const mysqlPool = mysql.createPool({
  host: MYSQL_HOST || '127.0.0.1', // ✅ localhost 대신 127.0.0.1로 강제 (TCP)
  port: Number(MYSQL_PORT) || 3307,
  user: MYSQL_USER || 'root',
  password: MYSQL_PASS || '',
  database: MYSQL_DB || 'rems',
  connectionLimit: Number(MYSQL_CONN_LIMIT) || 10,
  waitForConnections: true,
  ssl: MYSQL_SSL === 'true' ? { rejectUnauthorized: true } : undefined, // 옵션 SSL
});

/*
모듈 내보내기
다른 파일에서 const { mysqlPool } = require('./db.mysql') 형식으로 사용
*/
module.exports = { mysqlPool };
