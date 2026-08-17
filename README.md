# Worker Download

Queue-based download worker สำหรับ [VdoHide](https://vdohide.xyz) — claim งานจาก `video_process` ที่ enqueuer (vdohide-service) เติมไว้ ดาวน์โหลด/ประมวลผลวิดีโอ แล้วบันทึกลง storage ที่พร้อมเล่น

> แทนที่ `server-download` เดิมที่ scan หาไฟล์เอง — ตัวนี้รับงานจากคิวอย่างเดียว

## Features

- **Queue-based** — atomic claim จาก `video_process` (pending → processing) ตาม priority ไม่มีทางแย่งงานกัน
- **Sources** — Upload (S3/local ingest), Google Drive (OAuth2), MissAV (playlist), XVideos/PornHub (scraper), Direct URL, HLS/m3u8
- **Storage upload** — เลือก S3 ที่รับ `storage + video` และมี `originUrl` ก่อน อัพเป็น `{fileId}/file_original.mp4` → media → `ready`; หากไม่มีจึง fallback ไป local หรือ S3 temp (`{date}/{fileId}_file_original.mp4` → ingest `processed` → `ready_original`)
- **Auto Retry + Backoff** — fail → กลับเป็น pending ใน doc เดิม (1m, 2m) ครบ 3 ครั้ง → failed ถาวร + file → `error`
- **Instant Cancel** — admin เซ็ต `status: cancelled` → watcher (5s) จุดระเบิด context → HTTP/ffmpeg/S3 หยุดทันที + เก็บกวาด temp
- **Graceful Shutdown** — SIGTERM → คืนงานเข้าคิว (Release) + mark worker offline
- **Heartbeat** — รายงานเข้า `workers` ทุก 1 นาที (idle/busy/paused, disk ≥90% = paused + enable=false)
- **Step-only DB writes** — progress % ออก log เท่านั้น (throttle 10%) DB เขียนแค่ขอบ step (33/66/100)
- **Log per job** — จบงาน → อัพ `logs/process/<slug>.log` ขึ้น S3 ที่ `logs/download/` แล้วลบ local
- **Clone propagation** — ไฟล์ที่ `clonedFrom` ต้นฉบับ ได้ status/media ตามอัตโนมัติ

## Requirements

- **FFmpeg** + **FFprobe** (ต้องอยู่ใน PATH)
- **MongoDB** (vdohide platform database — replica set)
- **vdohide-service** รันอยู่ (enqueuer เติมคิว + reaper)

---

## Installation (Linux Server)

### One-line install

```bash
curl -fsSL https://raw.githubusercontent.com/zergolf1994/worker-download/main/install.sh | sudo -E bash -s -- \
    --database-url "mongodb+srv://user:pass@cluster.mongodb.net/platform" \
    -n 1
```

### Options

| Option | Default | คำอธิบาย |
|---|---|---|
| `-n, -w, --count` | `1` | จำนวน worker instances |
| `--database-url` | `""` | MongoDB connection string (`DATABASE_URL`) |
| `--storage-id` | `""` | Local storage ID สำหรับ fallback เมื่อไม่มี S3 `storage + video` |
| `--storage-path` | `/home/files` | Local storage path |
| `--scraper-url` | `""` | Scraper API (ไม่ตั้ง = อ่านจาก `settings.url_scraping`) |
| `--uninstall` | — | ถอนการติดตั้ง |

### After install

```bash
# ดู logs
journalctl -u "worker-download@*" -f

# ดู worker 1
journalctl -u "worker-download@1" -f

# Restart workers
for i in $(seq 1 2); do systemctl restart worker-download@$i; done

# Stop workers (SIGTERM → คืนงานเข้าคิวก่อนปิด)
for i in $(seq 1 2); do systemctl stop worker-download@$i; done
```

---

## Download Latest Release

```bash
# Linux amd64
curl -L https://github.com/zergolf1994/worker-download/releases/latest/download/linux -o worker-download
chmod +x worker-download

# Linux ARM64
curl -L https://github.com/zergolf1994/worker-download/releases/latest/download/linux-arm64 -o worker-download
chmod +x worker-download
```

---

## Configuration (.env)

```env
# Required
DATABASE_URL=mongodb+srv://user:pass@cluster.mongodb.net/platform

# Optional — self-hosted local fallback
STORAGE_ID=your-storage-uuid
STORAGE_PATH=/home/files

# Optional — Worker ID (default: download_hostname@1)
WORKER_ID=download_myhost@1

# Optional — Scraper URL (ไม่ตั้ง = อ่านจาก settings.url_scraping)
SCRAPER_URL=http://localhost:8081

# Optional — log file (default: logs/worker-download.log)
LOG_PATH=logs/worker-download.log
```

---

## Development

```bash
git clone https://github.com/zergolf1994/worker-download.git
cd worker-download

# สร้าง .env แล้วใส่ DATABASE_URL

# Run
go run ./cmd

# Build (Windows exe + copy .env → .build/)
build.bat
```

---

## Release

```bash
git tag v1.0.0
git push origin v1.0.0
```

GitHub Actions build + release อัตโนมัติ:
- `linux` — Linux amd64 binary
- `linux-arm64` — Linux ARM64 binary

---

## Architecture

```
vdohide-service (Node)                    worker-download (Go, ตัวนี้)
├── enqueuer (ทุก 20s)                    ├── heartbeat (ทุก 1m → workers)
│   slot ว่าง → files waiting             ├── job loop
│   → insert video_process pending        │   ResumeOwn → Claim (atomic, priority)
└── reaper                                │   → download → merge/encode → probe
    processing ค้าง (claimedAt เก่า)       │   → upload S3 video | local | S3 temp
    → คืน pending                          │   → ingest/media + propagate clones
                                          │   → Complete | RetryOrFail | Release
                                          └── cancel watcher (ทุก 5s ระหว่างมีงาน)
```

## Job Lifecycle

```
pending ──claim──▶ processing ──สำเร็จ──▶ completed
   ▲                   │
   │◀── retry (backoff 1m/2m, ≤3) ── fail
   │◀── Release (shutdown / disk เต็ม / reaper)
   │
   └── admin เซ็ต cancelled ──▶ หยุดทุก I/O ใน ≤5s + cleanup
       fail ครั้งที่ 3 ──▶ failed ถาวร + file → error (+ clones)
```

## Collections Used

| Collection | การใช้งาน |
|---|---|
| `video_process` | คิวงาน — claim/settle/timeline (contract ตรงกับ vdohide-service) |
| `workers` | heartbeat, สถานะ, system info |
| `files` | อ่าน source, update status (ready/ready_original/error) |
| `ingests` | อ่าน path ไฟล์ upload / สร้าง record `processed` ให้ HLS |
| `storages` | S3 storage/video, S3 temp และ local storage |
| `medias` | media original สำหรับ S3 video/local + clone |
| `oauths` | OAuth2 token สำหรับ GDrive |
| `settings` | `download_config.enabled` (kill switch), `url_scraping` |

> ⚠ **Index ทั้งหมดเป็นของฝั่ง vdohide-service (mongoose)** — repo นี้ไม่สร้าง index เอง
> ⚠ ค่า enum ทุกตัวใน `internal/core/enums/` ต้อง match กับ `vdohide-service/src/core/enums/` ไฟล์ต่อไฟล์
