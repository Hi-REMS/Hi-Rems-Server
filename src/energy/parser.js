// src/energy/parser.js
// 에너지 계측 장치로부터 수집된 Hex 프레임을 파싱하는 모듈
// - 지원 에너지 타입:
//   · 태양광 단상 (energy=0x01, type=0x01)
//   · 태양광 삼상 (energy=0x01, type=0x02)
//   · 태양열 강제순환식 (energy=0x02, type=0x01)
//   · 태양열 자연순환식 (energy=0x02, type=0x02)
//   · 지열 히트펌프   (energy=0x03, type=0x01)
//   · 지열 부하측     (energy=0x03, type=0x02)
//   · 풍력            (energy=0x04, type=0x01)
// - 출력: { ok, command, energy, energyName, type, typeName, multi, errCode, metrics{...} }
//   ※ 누적 에너지는 전기/열/풍력 모두 metrics.cumulativeWh(BigInt, Wh) 로 통일

const BUILD = 'parser-geo+wind+fuelcell+ess-2025-10-31b';


const KCAL_PER_KWH = 860.42065;
const HEATPUMP_STATE = { 0: '미작동', 1: '냉방', 2: '난방' };

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

const THERMAL_FAULT_MAP = {
  0: '장비 미작동', 
};
const GEOTHERMAL_FAULT_MAP = {
  0: '히트펌프 미작동', 
};
const WIND_FAULT_MAP = {
  0: '인버터 미동작',
};

const FUELCELL_FAULT_MAP = {
  0: '장비 미작동',
};
const ESS_FAULT_MAP = {
  0: '장비 미작동',
};

const ERR_LABEL = {
  0x39: 'serial_comm_failure',
};

const ENERGY_NAME = {
  0x01: '태양광',
  0x02: '태양열',
  0x03: '지열',
  0x04: '풍력',
  0x06: '연료전지',
  0x07: 'ESS',
};

const clean = (s) => (s || '').replace(/\s+/g, ' ').trim();
const toBytes = (s) => clean(s).split(' ').map((h) => parseInt(h, 16));

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

// 멀티 슬롯
const getMulti = (m) =>
  (m === 0x00 ? 1 : m === 0x01 ? 2 : m === 0x02 ? 3 : m === 0x03 ? 4 : 1);

const getStatuses = (flags) => faultBitsToList(flags, STATUS_MAP);

function faultBitsToList(flags, map = {}) {
  const L = [];
  for (let i = 0; i < 16; i++) {
    if (((flags >> i) & 1) === 1) {
      L.push(map[i] || `비정의 비트#${i}`);
    }
  }
  return L;
}

function readCumulativeWh(bytes, idx, need64MinLen, need32MinLen) {
  if (bytes.length >= need64MinLen) return u64(bytes, idx);
  if (bytes.length >= need32MinLen) return BigInt(u32(bytes, idx));
  return null;
}

function temp10_from2bytes(a, i) {
  const b0 = a[i];
  const b1 = a[i + 1];
  const signNibble = (b0 & 0xF0) >>> 4;
  const mag = ((b0 & 0x0F) << 8) | b1; 
  const val = mag / 10;
  return signNibble === 0x0 ? val : -val;
}

// 태양열 파서 (강제/자연)
function parseSolarThermalForced(bytes, off = 5) {
  const inlet    = temp10_from2bytes(bytes, off + 0); // 집열기 입구
  const outlet   = temp10_from2bytes(bytes, off + 2); // 집열기 출구
  const tankTop  = temp10_from2bytes(bytes, off + 4); // 축열조 상부
  const tankBot  = temp10_from2bytes(bytes, off + 6); // 축열조 하부
  const flowLpm  = u32(bytes, off + 8)  / 10;         // 유량(LPM, 10배)
  const prodKcal = Number(u64(bytes, off + 12)) / 100; // 누적 생산(kcal, 100배)
  const coldT    = temp10_from2bytes(bytes, off + 20); // 급수
  const hotT     = temp10_from2bytes(bytes, off + 22); // 급탕
  const useFlow  = u32(bytes, off + 24) / 10;          // 소비 유량(LPM, 10배)
  const useKcal  = Number(u64(bytes, off + 28)) / 100; // 누적 사용(kcal, 100배)
  const fault    = u16(bytes, off + 36);
  const faultList = faultBitsToList(fault, THERMAL_FAULT_MAP);

  const cumulativeKwh = (prodKcal > 0 ? prodKcal : useKcal) / KCAL_PER_KWH;
  const cumulativeWhBI = BigInt(Math.max(0, Math.round(cumulativeKwh * 1000)));

  const deltaT = (outlet ?? 0) - (inlet ?? 0);
  const operating = ((fault & 0x0001) === 0) && (
    (flowLpm > 0) || (useFlow > 0) || (Math.abs(deltaT) >= 1) || (prodKcal > 0) || (useKcal > 0)
  );

  return {
    inlet, outlet, tankTop, tankBot,
    flowLpm, coldT, hotT, useFlow,
    producedKcal: prodKcal,
    usedKcal: useKcal,
    producedKwh: Number((prodKcal / KCAL_PER_KWH).toFixed(3)),
    usedKwh: Number((useKcal / KCAL_PER_KWH).toFixed(3)),
    faultCode: fault,
    faultList,
    isOperating: operating,
    cumulativeWh: cumulativeWhBI,
  };
}

function parseSolarThermalNatural(bytes, off = 5) {
  const coldT    = temp10_from2bytes(bytes, off + 0);  // 급수배관
  const hotT     = temp10_from2bytes(bytes, off + 2);  // 급탕배관
  const flowLpm  = u32(bytes, off + 4)  / 10;          // 유량(LPM, 10배)
  const useKcal  = Number(u64(bytes, off + 8)) / 100;  // 누적 사용(kcal, 100배)
  const fault    = u16(bytes, off + 16);
  const faultList = faultBitsToList(fault, THERMAL_FAULT_MAP);

  const cumulativeKwh = useKcal / KCAL_PER_KWH;
  const cumulativeWhBI = BigInt(Math.max(0, Math.round(cumulativeKwh * 1000)));

  const deltaT = (hotT ?? 0) - (coldT ?? 0);
  const operating = ((fault & 0x0001) === 0) && (
    (flowLpm > 0) || (Math.abs(deltaT) >= 1) || (useKcal > 0)
  );

  return {
    coldT, hotT, flowLpm,
    usedKcal: useKcal,
    usedKwh: Number((useKcal / KCAL_PER_KWH).toFixed(3)),
    faultCode: fault,
    faultList,
    isOperating: operating,
    cumulativeWh: cumulativeWhBI,
  };
}

// 지열 파서
function parseGeothermalHeatPumpExact(bytes, off = 5) {
  if (bytes.length < off + 41) return { short: true };

  const voltageV   = u16(bytes, off + 0);
  const currentA   = u16(bytes, off + 2);
  const outputW    = u16(bytes, off + 4);
  const heatW      = u32(bytes, off + 6);

  const prodKwh10  = u64(bytes, off + 10);
  const useElec10  = u64(bytes, off + 18);
  const stateRaw   = bytes[off + 26];

  const srcInC     = temp10_from2bytes(bytes, off + 27);
  const srcOutC    = temp10_from2bytes(bytes, off + 29);
  const loadInC    = temp10_from2bytes(bytes, off + 31);
  const loadOutC   = temp10_from2bytes(bytes, off + 33);

  const flowLpm    = u32(bytes, off + 35) / 10;
  const faultFlags = u16(bytes, off + 39);
  const faultList  = faultBitsToList(faultFlags, GEOTHERMAL_FAULT_MAP);

  const producedKwh   = Number(prodKwh10) / 10;
  const usedElecKwh   = Number(useElec10) / 10;
  const cumulativeWh  = BigInt(Math.max(0, Math.round(producedKwh * 1000)));

  const operatingHP =
    (stateRaw !== 0) &&
    ((faultFlags & 0x0001) === 0) &&
    (flowLpm > 0 || heatW > 0 || outputW > 0);

  return {
    voltageV, currentA, outputW, heatW,
    producedKwh, usedElecKwh,
    state: HEATPUMP_STATE[stateRaw] ?? String(stateRaw),
    stateRaw,
    sourceInTempC: srcInC,
    sourceOutTempC: srcOutC,
    loadInTempC: loadInC,
    loadOutTempC: loadOutC,
    flowLpm,
    faultFlags,
    faultList,
    isOperating: operatingHP,
    cumulativeWh,
  };
}

function parseGeothermalLoadExact(bytes, off = 5) {
  if (bytes.length < off + 34) return { short: true };

  const loadInC       = temp10_from2bytes(bytes, off + 0);
  const loadOutC      = temp10_from2bytes(bytes, off + 2);
  const loadFlowLpm   = u32(bytes, off + 4) / 10;
  const loadUsedKwh10 = u64(bytes, off + 8);

  const tapFeedC      = temp10_from2bytes(bytes, off + 16);
  const tapHotC       = temp10_from2bytes(bytes, off + 18);
  const tapFlowLpm    = u32(bytes, off + 20) / 10;
  const tapUsedKwh10  = u64(bytes, off + 24);

  const faultFlags    = u16(bytes, off + 32);
  const faultList     = faultBitsToList(faultFlags, GEOTHERMAL_FAULT_MAP);

  const loadUsedKwh = Number(loadUsedKwh10) / 10;
  const tapUsedKwh  = Number(tapUsedKwh10) / 10;

  const representativeKwh = Math.max(loadUsedKwh, tapUsedKwh);
  const cumulativeWh = BigInt(Math.max(0, Math.round(representativeKwh * 1000)));

  const operatingLoad =
    ((faultFlags & 0x0001) === 0) &&
    (loadFlowLpm > 0 || tapFlowLpm > 0);

  return {
    loadInTempC: loadInC,
    loadOutTempC: loadOutC,
    loadFlowLpm,
    loadUsedKwh,
    tapFeedTempC: tapFeedC,
    tapHotTempC: tapHotC,
    tapFlowLpm,
    tapUsedKwh,
    faultFlags,
    faultList,
    isOperating: operatingLoad,
    cumulativeWh,
  };
}

// 풍력 파서 
function parseWindExact(bytes, off = 5) {
  if (bytes.length < off + 24) return { short: true };

  const preVoltageV   = u16(bytes, off + 0);
  const preCurrentA   = u16(bytes, off + 2);
  const preOutputW    = u16(bytes, off + 4);

  const postVoltageV  = u16(bytes, off + 6);
  const postCurrentA  = u16(bytes, off + 8);
  const postOutputW   = u16(bytes, off + 10);

  const frequencyHz   = u16(bytes, off + 12) / 10.0;
  const cumulativeWh  = u64(bytes, off + 14);
  const faultFlags    = u16(bytes, off + 22);
  const faultList     = faultBitsToList(faultFlags, WIND_FAULT_MAP);

  const isOperating = ((faultFlags & 0x0001) === 0) &&
                      ((preOutputW || postOutputW || preVoltageV || postVoltageV) > 0);

  return {
    preVoltageV, preCurrentA, preOutputW,
    postVoltageV, postCurrentA, postOutputW,
    frequencyHz,
    cumulativeWh,
    faultFlags, faultList,
    isOperating,
  };
}

// 메인 파서
function parseFrame(hex) {
  const b = toBytes(hex);
  if (b.length < 5) return { ok: false, reason: 'short' };

  const command = b[0];
  if (command !== 0x14) {
    return { ok: false, reason: 'unsupported_command', command };
  }

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
      energy === 0x01 && type === 0x01 ? '태양광 단상' :
      energy === 0x01 && type === 0x02 ? '태양광 삼상' :
      energy === 0x02 && type === 0x01 ? '태양열 강제순환식' :
      energy === 0x02 && type === 0x02 ? '태양열 자연순환식' :
      energy === 0x03 && type === 0x01 ? '지열 히트펌프' :
      energy === 0x03 && type === 0x02 ? '지열 부하측' :
      energy === 0x04 && type === 0x01 ? '풍력' :
      energy === 0x06 && type === 0x01 ? '연료전지' :
      energy === 0x07 && type === 0x01 ? 'ESS' :
      'Unknown',
    multi: getMulti(multi),
    errCode: err,
    error: err ? (ERR_LABEL[err] || '에러') : '',
    metrics: {},
  };

  if (err !== 0x00) {
    return { ...out, ok: false, reason: ERR_LABEL[err] || 'device_error' };
  }

  // ───────── 태양광 단상 (0x01/0x01)
  if (energy === 0x01 && type === 0x01) {
    if (b.length < 21) return { ...out, ok: false, reason: 'short_single' };

    const pvVoltage = u16(b, 5);
    const pvCurrent = u16(b, 7);
    const pvOutputW = u16(b, 9);
    const sysV = u16(b, 11);
    const sysI = u16(b, 13);
    const curW = u16(b, 15);
    const pf = u16(b, 17) / 10.0;
    const hz = u16(b, 19) / 10.0;

    const cumulativeWh = readCumulativeWh(b, 21, 29, 25);
    const flags = b.length >= 31 ? u16(b, 29) : 0;

    let pvPowerW = pvOutputW ?? null;
    if (pvPowerW == null || pvPowerW === 0) {
      if (pvVoltage != null && pvCurrent != null) pvPowerW = pvVoltage * pvCurrent;
    }

    const statusList = getStatuses(flags);
    const isOperating =
      ((flags & 0x0001) === 0) &&
      ((pvPowerW || 0) > 0 || (curW || 0) > 0 || (sysV || 0) > 0);

    out.metrics = {
      pvVoltage,
      pvCurrent,
      pvOutputW,
      pvPowerW,
      systemVoltage: sysV,
      systemCurrent: sysI,
      currentOutputW: curW,
      powerFactor: pf,
      frequencyHz: hz,
      cumulativeWh,
      statusFlags: flags,
      statusList,
      isOperating,
    };
    return out;
  }

  // ───────── 태양광 삼상 (0x01/0x02)
  if (energy === 0x01 && type === 0x02) {
    if (b.length < 33) return { ...out, ok: false, reason: 'short_three' };

    const pvVoltage = u16(b, 5);
    const pvCurrent = u16(b, 7);
    const pvOutputW = u32(b, 9);
    const rV = u16(b, 13), sV = u16(b, 15), tV = u16(b, 17);
    const rI = u16(b, 19), sI = u16(b, 21), tI = u16(b, 23);
    const curW = u32(b, 25);
    const pf = u16(b, 29) / 10.0;
    const hz = u16(b, 31) / 10.0;

    const cumulativeWh = readCumulativeWh(b, 33, 41, 0);
    const flags = b.length >= 43 ? u16(b, 41) : 0;

    let pvPowerW = pvOutputW ?? null;
    if (pvPowerW == null || pvPowerW === 0) {
      if (
        rV != null && sV != null && tV != null &&
        rI != null && sI != null && tI != null
      ) {
        pvPowerW = (rV * rI) + (sV * sI) + (tV * tI);
      } else if (pvVoltage != null && pvCurrent != null) {
        pvPowerW = pvVoltage * pvCurrent;
      }
    }

    const statusList = getStatuses(flags);
    const isOperating =
      ((flags & 0x0001) === 0) &&
      ((pvPowerW || 0) > 0 || (curW || 0) > 0 || ((rV || 0) + (sV || 0) + (tV || 0) > 0));

    out.metrics = {
      pvVoltage,
      pvCurrent,
      pvOutputW,
      pvPowerW,
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
      statusList,
      isOperating,
    };
    return out;
  }

  // ───────── 태양열 (0x02/0x01, 0x02)
  if (energy === 0x02) {
    if (type === 0x01) { // 강제순환식(38B)
      if (b.length < 5 + 38) return { ...out, ok: false, reason: 'short_thermal_forced' };
      const m = parseSolarThermalForced(b, 5);
      out.metrics = {
        inletTempC: m.inlet,
        outletTempC: m.outlet,
        tankTopTempC: m.tankTop,
        tankBottomTempC: m.tankBot,
        flowLpm: m.flowLpm,
        coldTempC: m.coldT,
        hotTempC: m.hotT,
        consumedFlowLpm: m.useFlow,
        producedKcal: m.producedKcal,
        usedKcal: m.usedKcal,
        producedKwh: m.producedKwh,
        usedKwh: m.usedKwh,
        faultCode: m.faultCode,
        faultList: m.faultList,
        isOperating: m.isOperating,
        cumulativeWh: m.cumulativeWh,
      };
      return out;
    }

    if (type === 0x02) { // 자연순환식(18B)
      if (b.length < 5 + 18) return { ...out, ok: false, reason: 'short_thermal_natural' };
      const m = parseSolarThermalNatural(b, 5);
      out.metrics = {
        coldTempC: m.coldT,
        hotTempC: m.hotT,
        flowLpm: m.flowLpm,
        usedKcal: m.usedKcal,
        usedKwh: m.usedKwh,
        faultCode: m.faultCode,
        faultList: m.faultList,
        isOperating: m.isOperating,
        cumulativeWh: m.cumulativeWh,
      };
      return out;
    }
  }

  // ───────── 지열 (0x03/0x01, 0x02)
  if (energy === 0x03) {
    if (type === 0x01) { // 히트펌프(41B)
      if (b.length < 5 + 41) return { ...out, ok: false, reason: 'short_geothermal_hp' };
      const m = parseGeothermalHeatPumpExact(b, 5);
      if (m.short) return { ...out, ok: false, reason: 'short_geothermal_hp' };
      out.metrics = m;
      return out;
    }

    if (type === 0x02) { // 부하측(34B)
      if (b.length < 5 + 34) return { ...out, ok: false, reason: 'short_geothermal_load' };
      const m = parseGeothermalLoadExact(b, 5);
      if (m.short) return { ...out, ok: false, reason: 'short_geothermal_load' };
      out.metrics = m;
      return out;
    }
  }

  // ───────── 풍력 (0x04/0x01)
  if (energy === 0x04) {
    if (type === 0x00) {
      return { ...out, ok: false, reason: 'wind_heartbeat_only', metrics: {} };
    }
    if (type === 0x01) {
      if (b.length < 5 + 24) return { ...out, ok: false, reason: 'short_wind' };
      const m = parseWindExact(b, 5);
      if (m.short) return { ...out, ok: false, reason: 'short_wind' };
      out.metrics = m;
      return out;
    }
  }

  // ───────── 연료전지 (0x06/0x01)
  if (energy === 0x06 && type === 0x01) {
    if (b.length < 5 + 56) return { ...out, ok: false, reason: 'short_fuelcell' };
    const m = parseFuelCellExact(b, 5);
    if (m.short) return { ...out, ok: false, reason: 'short_fuelcell' };
    out.metrics = m;
    return out;
  }

  // ───────── ESS (0x07/0x01)
  if (energy === 0x07 && type === 0x01) {
    if (b.length < 5 + 31) return { ...out, ok: false, reason: 'short_ess' };
    const m = parseESSExact(b, 5);
    if (m.short) return { ...out, ok: false, reason: 'short_ess' };
    out.metrics = m;
    return out;
  }
  return out;
}



function parseFuelCellExact(bytes, off = 5) {
  if (bytes.length < off + 56) return { short: true };

  const preVoltageV   = u16(bytes, off + 0);
  const preCurrentA   = u16(bytes, off + 2);
  const preOutputW    = u16(bytes, off + 4);

  const postVoltageV  = u16(bytes, off + 6);
  const postCurrentA  = u16(bytes, off + 8);
  const postOutputW   = u16(bytes, off + 10);

  const heatGenW      = u16(bytes, off + 12);
  const producedKwh10 = u64(bytes, off + 14);
  const usedHeatKwh10 = u64(bytes, off + 22);
  const usedElecKwh10 = u64(bytes, off + 30);
  const feedTempC     = temp10_from2bytes(bytes, off + 38);
  const outletTempC   = temp10_from2bytes(bytes, off + 40);
  const efficiencyPct = u16(bytes, off + 42) / 10;
  const freqHz        = u16(bytes, off + 44) / 10;
  const cumulativeWh  = u64(bytes, off + 46);
  const faultFlags    = u16(bytes, off + 54);
  const faultList     = faultBitsToList(faultFlags, FUELCELL_FAULT_MAP);

  const producedKwh   = Number(producedKwh10) / 10;
  const usedHeatKwh   = Number(usedHeatKwh10) / 10;
  const usedElecKwh   = Number(usedElecKwh10) / 10;
  const cumulativeWhBI = BigInt(Math.max(0, Math.round(Number(cumulativeWh))));

  const isOperating = ((faultFlags & 0x0001) === 0) &&
                      (preVoltageV > 0 || postVoltageV > 0 || postOutputW > 0);

  return {
    preVoltageV, preCurrentA, preOutputW,
    postVoltageV, postCurrentA, postOutputW,
    heatGenerationW: heatGenW,
    producedKwh, usedHeatKwh, usedElecKwh,
    feedTempC, outletTempC,
    efficiencyPct, freqHz,
    cumulativeWh: cumulativeWhBI,
    faultFlags, faultList,
    isOperating,
  };
}

// ESS 파서 (0x07/0x01) — tail-robust

function parseESSExact(bytes, off = 5) {
  if (bytes.length < off + 10) return { short: true };

  const faultPos = bytes.length - 2;
  const cumPos   = bytes.length - 10;
  const faultFlags   = u16(bytes, faultPos);
  const cumulativeWh = u64(bytes, cumPos);

  let cursor = off;
  const safeU16 = (i) => (i + 1 < bytes.length ? u16(bytes, i) : null);

  const frequencyHz      = (faultPos - 12 >= off) ? safeU16(faultPos - 12) / 10 : null;
  const inverterOutputW  = (faultPos - 10 >= off) ? safeU16(faultPos - 10) : null;

  const battVoltageV = (bytes.length >= off + 6)  ? safeU16(off + 0) : null;
  const battCurrentA = (bytes.length >= off + 8)  ? safeU16(off + 2) : null;
  const gridVoltageV = (bytes.length >= off + 10) ? safeU16(off + 4) : null;
  const gridCurrentA = (bytes.length >= off + 12) ? safeU16(off + 6) : null;
  const socPct       = (bytes.length >= off + 14) ? (safeU16(off + 8) / 10) : null;

  const faultList = faultBitsToList(faultFlags, ESS_FAULT_MAP);
  const isOperating =
    ((faultFlags & 0x0001) === 0) &&
    ((Number(inverterOutputW) || 0) > 0 || (Number(gridVoltageV) || 0) > 0);

  return {
    inverterOutputW: inverterOutputW ?? null,
    frequencyHz:     frequencyHz ?? null,
    batteryVoltageV: battVoltageV ?? null,
    batteryCurrentA: battCurrentA ?? null,
    gridVoltageV:    gridVoltageV ?? null,
    gridCurrentA:    gridCurrentA ?? null,
    socPct:          Number.isFinite(socPct) ? socPct : null,
    cumulativeWh,
    faultFlags,
    faultList,
    isOperating,
  };
}

module.exports = { parseFrame, BUILD };
