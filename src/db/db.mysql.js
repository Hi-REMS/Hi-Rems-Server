/*
커넥션 풀 생성하는 모듈, Promise 기반 API로 await를 써서 query, getConnetion 등을 호출
*/
const mysql = require('mysql2/promise');

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
rejectUnauthorized : false는 자체 서명 인증서 같은 경우 검증을 무시
*/
const ssl =
  String(MYSQL_SSL).toLowerCase() === 'true'
    ? { rejectUnauthorized: false }
    : undefined;

/*
커넥션 풀 생성
*/
const mysqlPool = mysql.createPool({
  host: MYSQL_HOST,
  port: Number(MYSQL_PORT),
  user: MYSQL_USER,
  password: MYSQL_PASS,
  database: MYSQL_DB,
  connectionLimit: Number(MYSQL_CONN_LIMIT),
});

/*
모듈 내보내기
다른 파일에서 const { mysqlPool } = require('./db.mysql') 형식으로 사용
*/
module.exports = { mysqlPool };
