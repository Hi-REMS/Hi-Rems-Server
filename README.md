## HI-REMS-SERVER

### 🌍 Hi-REMS (Hybrid Energy Remote Monitoring System)

> **"지속 가능한 에너지를 위한 통합 원격 관제 플랫폼"**

**Hi-REMS**는 태양광, 태양열, 지열, 풍력, 연료전지, ESS 등 이기종 신재생 에너지 설비의 데이터를 실시간으로 수집·분석하여, 최적의 에너지 효율 관리와 안정적인 설비 운영을 지원하는 통합 모니터링 솔루션입니다.

---

Hi-REMS 시스템은 대용량 시계열 데이터 처리와 실시간 관제를 위해 최적화된 하이브리드 아키텍처를 채택하고 있습니다.

---

### 로컬 설치 환경

1. node 설치 
>  https://nodejs.org/ko/download

2. 패키지 설치
> npm install


3. 환경 변수 설정
> .env 파일에 적용된 값들을 복사하여 .env 파일을 생성해서 적용해야 합니다. 로컬 환경에 맞게 DB 접속 정보를 수정해야 합니다.

4. node src/app.js
> 프로젝트 실행

5. PostgreSQL에 접속하여 데이터베이스를 생성하고 필요한 확장을 활성화해야 합니다.
> CREATE DATABASE alliothub
> CAEATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
> 프로젝트에서 사용하는 테이블(members, imei_meta, log_rtureceivelog)와 연속 집계 뷰(log_rtureceivelog_daily) 스키마를 생성해야 합니다.

6. 서버 실행
> node src/app.js

### 더미데이터 및 스크립트 실행

1. 더미데이터 파일은 다음과 같습니다
- `body_logs.csv` : 실제 데이터를 사용하는 데이터 : `imei`,`time`,`body`,`opMode` 필드가 있으며 실제 핵심 데이터는 body
- `user_setup_csv` : 사용자 데이터

2. 서버가 정상적으로 실행되면, 이전에 작성했던 시딩 스크립트 실행합니다. 스크립트 파일을 실행하면 INSERT 쿼리가 작동하며 데이터가 채워집니다.
> node scripts/seed-gs-data.js

3. 더미데이터를 추가하고 싶으면 body_logs.csv에 형식에 맞춰 추가하면 됩니다. (관련 서식은 가이드라인 프로토콜을 참고하세요.)
[text](vscode-local:/c%3A/Users/user/Downloads/%EC%8B%A0%EC%9E%AC%EC%83%9D%EC%97%90%EB%84%88%EC%A7%80_%ED%91%9C%EC%A4%80%ED%94%84%EB%A1%9C%ED%86%A0%EC%BD%9C_%EA%B0%80%EC%9D%B4%EB%93%9C%EB%9D%BC%EC%9D%B8_%EB%AA%A8%EB%8B%88%ED%84%B0%EB%A7%81%EC%97%85%EC%B2%B4%EC%9A%A9HTTPS_v1_0_3.pdf)

#### HTTPS 전송 데이터 형태 (body)

| 위치 (Byte) | 필드명 | 크기 | 설명 |
| :--- | :--- | :--- | :--- |
| **1st** | **Command** | 1 Byte | 데이터 명령 코드 (예: 0x14) |
| **2nd** | **Energy Source** | 1 Byte | 에너지원 구분 코드 (예: 0x01 태양광) |
| **3rd** | **Type** | 1 Byte | 설비 타입 정보 (예: 단상/삼상 구분) |
| **4th** | **Multi** | 1 Byte | 멀티/단독운전 구분 인덱스 |
| **5th** | **Error Code** | 1 Byte | 장비 에러 상태 코드 (0x00: 정상, 0x39: 통신이상 등) |
| **6th ~ 31st** | **Data** | 26 Byte | 인버터 측정 데이터 및 상세 정보 |

> body_logs.csv에 적용된 더미데이터 같은 경우 `14 01 01 00 00 01 5e 00 32 04 e2 00 dc 00 1a 04 e2 03 e7 02 58 00 00 00 00 00 0f 42 40 00 01`로 되어 있습니다.
| 커맨드 | 에너지원 | 타입 | 멀티 | 에러코드 | 데이터 |
> | 14 | 01 | 01 | 00 | 00 | 01 5e 00 32 04 e2 00 dc 00 1a 04 e2 03 e7 02 58 00 00 00 00 00 0f 42 40 00 01 |

4. 더미데이터를 조회할 시 오프라인으로 나오는 경우는 90분 이상 경과 시 오프라인으로 나오기 때문에 더미데이터를 최근 시간에 맞게 넣어주셔야 합니다.
> ex ) 현재 시간이 18:00이고 데이터 time 필드의 가장 최근 시간이 2026-02-26 14:00이면 조회 시 오프라인으로 나옵니다.

