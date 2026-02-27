const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const { requireAuth } = require('../middlewares/requireAuth');
const router = express.Router();


const uploadDir = process.env.UPLOAD_DIR || path.join(__dirname, '../../uploads/facility');

if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir, { recursive: true });
}

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
    destination: (req, file, cb) => {
        cb(null, uploadDir);
    },
    filename: (req, file, cb) => {
        const ext = path.extname(file.originalname);
        const imei = (req.body.rtuImei || 'unknown').trim();
        const timestamp = getKSTTimestamp();
        cb(null, `${imei}-${timestamp}${ext}`);
    },
});

const upload = multer({
    storage,
    limits: { 
        fileSize: 50 * 1024 * 1024
    },
    fileFilter: (req, file, cb) => {
        const filetypes = /jpeg|jpg|png|gif|webp/;
        const mimetype = filetypes.test(file.mimetype);
        const extname = filetypes.test(path.extname(file.originalname).toLowerCase());

        if (mimetype && extname) {
            return cb(null, true);
        }
        cb(new Error('이미지 파일(jpg, jpeg, png, gif, webp)만 업로드 가능합니다.'));
    }
});

router.post('/upload', requireAuth, (req, res) => {
    upload.single('file')(req, res, (err) => {
        if (err instanceof multer.MulterError) {
            if (err.code === 'LIMIT_FILE_SIZE') {
                return res.status(400).json({ 
                    ok: false, 
                    message: '파일 크기가 너무 큽니다. 최대 50MB까지 가능합니다.' 
                });
            }
            return res.status(400).json({ ok: false, message: err.message });
        } else if (err) {
            return res.status(400).json({ ok: false, message: err.message });
        }

        if (!req.file) {
            return res.status(400).json({ ok: false, message: '업로드된 파일이 없습니다.' });
        }

        try {
            const publicUrl = `/uploads/facility/${req.file.filename}`;
            res.json({ ok: true, url: publicUrl });
        } catch (err) {
            console.error('[facility upload] error:', err);
            res.status(500).json({ message: '서버 내부 오류로 업로드에 실패했습니다.' });
        }
    });
});

module.exports = router;