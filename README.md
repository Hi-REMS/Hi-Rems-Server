## HI-REMS-SERVER

### 🌍 Hi-REMS (Hybrid Energy Remote Monitoring System)

> **"지속 가능한 에너지를 위한 통합 원격 관제 플랫폼"**

**Hi-REMS**는 태양광, 태양열, 지열, 풍력, 연료전지, ESS 등 이기종 신재생 에너지 설비의 데이터를 실시간으로 수집·분석하여, 최적의 에너지 효율 관리와 안정적인 설비 운영을 지원하는 통합 모니터링 솔루션입니다.

---

Hi-REMS 시스템은 대용량 시계열 데이터 처리와 실시간 관제를 위해 최적화된 하이브리드 아키텍처를 채택하고 있습니다.

```mermaid
graph LR
    A[⚡ RTU / Sensors] -->|Hex Packet| B(TCP/IP Network)
    B -->|Ingestion| C[🚀 Backend Server]
    C -->|Parse & Analyze| D[(🐘 PostgreSQL \nTimescaleDB)]
    C -.->|Sync Metadata| E[(🐬 MySQL \nLegacy Data)]
    C -->|REST API| F[💻 Web Dashboard \nVue.js]
    C -->|OpenAPI| G[📱 3rd Party App]
