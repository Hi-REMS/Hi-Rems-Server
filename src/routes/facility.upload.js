const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const { requireAuth } = require('../middlewares/requireAuth');
const router = express.Router();
const uploadDir = '/var/www/html/uploads/facility';
fs.mkdirSync(uploadDir, { recursive: true });

function getKSTTimestamp() {
  const now = new Date();

  const kst = new Date(now.getTime() + 9 * 60 * 60 * 1000);

  const yyyy = kst.getUTCFullYear();
  const mm = String(kst.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(kst.getUTCDate()).padStart(2, '0');
  const hh = String(kst.getUTCHours()).padStart(2, '0');
  const mi = String(kst.getUTCMinutes()).padStart(2, '0');
  const ss = String(kst.getUTCSeconds()).padStart(2, '0');

  return `${yyyy}${mm}${dd}_${hh}${mi}${ss}`;
}

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname);
    const imei = (req.body.rtuImei || 'unknown').trim();
    const timestamp = getKSTTimestamp();
    cb(null, `${imei}-${timestamp}${ext}`);
  },
});

const upload = multer({ storage });

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
