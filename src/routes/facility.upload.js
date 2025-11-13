// src/routes/facility.upload.js
const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const { requireAuth } = require('../middlewares/requireAuth');

const router = express.Router();

// ──────────────────────────────────────────────
// 📁 업로드 디렉토리 설정
// ──────────────────────────────────────────────
const uploadDir = '/var/www/html/uploads/facility';
fs.mkdirSync(uploadDir, { recursive: true });

// ──────────────────────────────────────────────
// 🧠 한국시간 기준 날짜/시간 포맷 함수
// ──────────────────────────────────────────────
function getKSTTimestamp() {
  const now = new Date();

  // UTC → 한국시간 (KST: UTC+9)
  const kst = new Date(now.getTime() + 9 * 60 * 60 * 1000);

  const yyyy = kst.getUTCFullYear();
  const mm = String(kst.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(kst.getUTCDate()).padStart(2, '0');
  const hh = String(kst.getUTCHours()).padStart(2, '0');
  const mi = String(kst.getUTCMinutes()).padStart(2, '0');
  const ss = String(kst.getUTCSeconds()).padStart(2, '0');

  return `${yyyy}${mm}${dd}_${hh}${mi}${ss}`;
}

// ──────────────────────────────────────────────
// ⚙️ multer 스토리지 설정
// ──────────────────────────────────────────────
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname);
    const imei = (req.body.rtuImei || 'unknown').trim();
    const timestamp = getKSTTimestamp();
    cb(null, `${imei}-${timestamp}${ext}`); // IMEI-YYYYMMDD_HHmmss.png
  },
});

const upload = multer({ storage });

// ──────────────────────────────────────────────
// 📤 업로드 라우트
// ──────────────────────────────────────────────
router.post('/upload', requireAuth, upload.single('file'), async (req, res) => {
  try {
    const publicUrl = `/uploads/facility/${req.file.filename}`;
    res.json({ ok: true, url: publicUrl });
  } catch (err) {
    console.error('[facility upload] error:', err);
    res.status(500).json({ message: 'Upload failed' });
  }
});

module.exports = router;
