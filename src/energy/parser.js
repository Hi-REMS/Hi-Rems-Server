// src/energy/parser.js
// 에너지 계측 장치로부터 수집된 Hex 프레임을 파싱하는 모듈
// - 지원 에너지 타입: 태양광 단상(0x01/0x01), 태양광 삼상(0x01/0x02)                      --> 이 분 추가로 지열 태양열 확장 필요
// - 프레임을 바이트 배열로 변환 후, 전압/전류/출력/주파수/누적Wh/상태 플래그를 추출
// - 미지원 포맷은 메타 정보만 반환
// - 출력은 { ok, command, energy, type, metrics } 형태

// 상태 코드 매핑
const STATUS_MAP = {
  0: '인버터 미동작',
  1: '태양전지 과전압',
  2: '태양전지 저전압',
  3: '태양전지 과전류',
  4: '인버터 IGBT 에러',
  5: '인버터 과온',
  6: '계통 과전압',
  7: '계통 저전압',
  8: '계통 과전류',
  9: '계통 과주파수',
  10: '계통 저주파수',
  11: '단독운전(정전)',
  12: '지락(누전)',
};

// 에너지 타입 코드 매핑
const ENERGY_NAME = {
  0x01: '태양광',
  0x02: '태양열',
  0x03: '지열',
  0x04: '풍력',
  0x06: '연료전지',
  0x07: 'ESS',
};

// 문자열 → 바이트 배열 변환 유틸
const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
const toBytes = (s) => clean(s).split(' ').map((h) => parseInt(h, 16));

// 바이트 배열 읽기 유틸 (Unsigned 16/32/64bit)
const u16 = (a, i) => ((a[i] << 8) | a[i + 1]) >>> 0;
const u32 = (a, i) =>
  (((a[i] << 24) | (a[i + 1] << 16) | (a[i + 2] << 8) | a[i + 3]) >>> 0) >>> 0;
const u64 = (a, i) =>
  (BigInt(a[i]) << 56n) |
  (BigInt(a[i + 1]) << 48n) |
  (BigInt(a[i + 2]) << 40n) |
  (BigInt(a[i + 3]) << 32n) |
  (BigInt(a[i + 4]) << 24n) |
  (BigInt(a[i + 5]) << 16n) |
  (BigInt(a[i + 6]) << 8n) |
  BigInt(a[i + 7]);

// 배율 값 해석
const getMulti = (m) => (m === 0x00 ? 1 : m === 0x01 ? 2 : m === 0x02 ? 3 : 1);

// 상태 플래그 → 설명 리스트 변환
const getStatuses = (flags) => {
  const L = [];
  for (let i = 0; i < 16; i++) {
    if (((flags >> i) & 1) && STATUS_MAP[i]) L.push(STATUS_MAP[i]);
  }
  return L;
};

// 누적 Wh 읽기: 길이에 따라 u64 우선, 부족하면 u32, 없으면 null
function readCumulativeWh(bytes, idx, need64MinLen, need32MinLen) {
  if (bytes.length >= need64MinLen) return u64(bytes, idx);
  if (bytes.length >= need32MinLen) return BigInt(u32(bytes, idx));
  return null;
}

// 프레임 파서 메인 함수
function parseFrame(hex) {
  const b = toBytes(hex);
  if (b.length < 5) return { ok: false, reason: 'short' };

  // 명령어 확인 (0x14만 지원)
  const command = b[0];
  if (command !== 0x14) {
    return { ok: false, reason: 'unsupported_command', command };
  }

  // 공통 메타 정보 추출
  const energy = b[1];
  const type = b[2];
  const multi = b[3];
  const err = b[4];

  const out = {
    ok: true,
    command,
    energy,
    energyName: ENERGY_NAME[energy] || 'Unknown',
    type,
    typeName:
      energy === 0x01 && type === 0x01
        ? '태양광 단상'
        : energy === 0x01 && type === 0x02
        ? '태양광 삼상'
        : 'Unknown',
    multi: getMulti(multi),
    errCode: err,
    error: err ? '에러' : '',
    metrics: {},
  };

  // ───────── 태양광 단상 (0x01/0x01) ─────────
  if (energy === 0x01 && type === 0x01) {
    if (err !== 0x00) return { ...out, ok: false, reason: 'inverter_error' };
    if (b.length < 21) return { ...out, ok: false, reason: 'short_single' };

    // 기본 계측 값
    const pvVoltage = u16(b, 5);
    const pvCurrent = u16(b, 7);
    const pvOutputW = u16(b, 9);          // 문서상 PV 출력 (2byte)
    const sysV = u16(b, 11);
    const sysI = u16(b, 13);
    const curW = u16(b, 15);
    const pf = u16(b, 17) / 10.0;
    const hz = u16(b, 19) / 10.0;

    // 누적 Wh: 29바이트 이상이면 u64, 25바이트 이상이면 u32
    const cumulativeWh = readCumulativeWh(b, 21, 29, 25);

    // 상태 플래그 (단상: 바이트 29–30)
    const flags = b.length >= 31 ? u16(b, 29) : 0;

    // 🔧 보강: pvPowerW 별칭 + 결측/0일 경우 전압×전류로 채움
    let pvPowerW = (pvOutputW ?? null);
    if (pvPowerW == null || pvPowerW === 0) {
      if (pvVoltage != null && pvCurrent != null) {
        pvPowerW = pvVoltage * pvCurrent; // 단상은 단순 V*A
      }
    }

    out.metrics = {
      pvVoltage,
      pvCurrent,
      pvOutputW,             // 원본 필드(호환용)
      pvPowerW,              // 🔥 새 별칭(서비스/프론트에서 이걸 우선 사용)
      systemVoltage: sysV,
      systemCurrent: sysI,
      currentOutputW: curW,
      powerFactor: pf,
      frequencyHz: hz,
      cumulativeWh,
      statusFlags: flags,
      statusList: getStatuses(flags),
    };
    return out;
  }


  // ───────── 태양광 삼상 (0x01/0x02) ─────────
  if (energy === 0x01 && type === 0x02) {
    if (err !== 0x00) return { ...out, ok: false, reason: 'inverter_error' };
    if (b.length < 33) return { ...out, ok: false, reason: 'short_three' };

    // 기본 계측 값
    const pvVoltage = u16(b, 5);
    const pvCurrent = u16(b, 7);
    const pvOutputW = u32(b, 9);          // 문서상 PV 출력 (4byte)
    const rV = u16(b, 13), sV = u16(b, 15), tV = u16(b, 17);
    const rI = u16(b, 19), sI = u16(b, 21), tI = u16(b, 23);
    const curW = u32(b, 25);
    const pf = u16(b, 29) / 10.0;
    const hz = u16(b, 31) / 10.0;

    // 누적 Wh: 41바이트 이상이면 u64
    const cumulativeWh = readCumulativeWh(b, 33, 41, 0);

    // 상태 플래그 (삼상: 바이트 41–42)
    const flags = b.length >= 43 ? u16(b, 41) : 0;

    // 🔧 보강: pvPowerW 별칭 + 결측/0일 경우 전압×전류 합산
    let pvPowerW = (pvOutputW ?? null);
    if (pvPowerW == null || pvPowerW === 0) {
      if (
        rV != null && sV != null && tV != null &&
        rI != null && sI != null && tI != null
      ) {
        // 선간전압 * 상전류의 합(역률은 계통 출력쪽이라 PV출력 보강에는 미적용)
        pvPowerW = (rV * rI) + (sV * sI) + (tV * tI);
      } else if (pvVoltage != null && pvCurrent != null) {
        // 최소 보장: PV 평균전압 * 합전류
        pvPowerW = pvVoltage * pvCurrent;
      }
    }

    out.metrics = {
      pvVoltage,
      pvCurrent,
      pvOutputW,            // 원본 필드(호환용)
      pvPowerW,             // 🔥 새 별칭
      systemR_V: rV,
      systemS_V: sV,
      systemT_V: tV,
      systemR_I: rI,
      systemS_I: sI,
      systemT_I: tI,
      currentOutputW: curW,
      powerFactor: pf,
      frequencyHz: hz,
      cumulativeWh,
      statusFlags: flags,
      statusList: getStatuses(flags),
    };
    return out;
  }


  // TODO: 0x02(태양열)/0x03(지열) 포맷 확정되면 추가
  return out; // Unknown → 메타만 반환
}

module.exports = { parseFrame };
