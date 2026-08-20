#!/bin/bash

# Worker Download Installation Script
# Usage: curl -fsSL https://raw.githubusercontent.com/zergolf1994/worker-download/main/install.sh | sudo -E bash -s -- [OPTIONS]

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Defaults
WORKER_COUNT=1
UNINSTALL=false
DATABASE_URL=""
STORAGE_ID=""
STORAGE_PATH="/home/files"
SCRAPER_URL=""
DASHBOARD_PORT="8885"
MEDIA_LAYOUT="muxed"

APP_NAME="worker-download"
APP_DIR="/opt/$APP_NAME"
SERVICE_NAME="worker-download"
GITHUB_REPO="zergolf1994/worker-download"
RELEASES_URL="https://github.com/$GITHUB_REPO/releases/latest/download"

print_status()  { echo -e "${GREEN}[INFO]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --uninstall)         UNINSTALL=true; shift ;;
        --count|-w|-n)       WORKER_COUNT="$2"; shift 2 ;;
        --database-url)      DATABASE_URL="$2"; shift 2 ;;
        --mongodb-uri)       DATABASE_URL="$2"; shift 2 ;; # alias เดิม
        --storage-id)        STORAGE_ID="$2"; shift 2 ;;
        --storage-path)      STORAGE_PATH="$2"; shift 2 ;;
        --scraper-url)       SCRAPER_URL="$2"; shift 2 ;;
		--dashboard-port)     DASHBOARD_PORT="$2"; shift 2 ;;
		--media-layout)       MEDIA_LAYOUT="$2"; shift 2 ;;
        -h|--help)
            echo "Worker Download Installer"
            echo ""
            echo "Usage: curl -fsSL https://raw.githubusercontent.com/$GITHUB_REPO/main/install.sh | sudo -E bash -s -- [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --uninstall          Uninstall completely"
            echo "  --count NUM          Number of worker instances (default: 1)"
            echo "  -w, -n NUM           Alias for --count"
            echo "  --database-url URI   MongoDB connection string (DATABASE_URL)"
            echo "  --storage-id ID      Local storage ID (optional — ไม่ตั้ง = ใช้ S3 temp)"
            echo "  --storage-path DIR   Local storage path (default: /home/files)"
            echo "  --scraper-url URL    Scraper API URL (optional — ไม่ตั้ง = อ่านจาก settings)"
			echo "  --dashboard-port N   Realtime monitor port (default: 8885)"
			echo "  --media-layout MODE  muxed or separated (default: muxed)"
            echo "  -h, --help           Show this help"
            echo ""
            echo "Examples:"
            echo "  # Install with 1 worker"
            echo "  curl -fsSL https://raw.githubusercontent.com/$GITHUB_REPO/main/install.sh | sudo -E bash -s -- \\"
            echo "      --database-url \"mongodb+srv://user:pass@host/db\""
            echo ""
            echo "  # Install with 2 workers"
            echo "  curl -fsSL https://raw.githubusercontent.com/$GITHUB_REPO/main/install.sh | sudo -E bash -s -- \\"
            echo "      --database-url \"mongodb+srv://user:pass@host/db\" --count 2"
            echo ""
            echo "  # Uninstall"
            echo "  curl -fsSL https://raw.githubusercontent.com/$GITHUB_REPO/main/install.sh | sudo bash -s -- --uninstall"
            exit 0 ;;
        *)
            print_error "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ "$MEDIA_LAYOUT" != "muxed" && "$MEDIA_LAYOUT" != "separated" ]]; then
    print_error "--media-layout must be muxed or separated"
    exit 1
fi

# ─── Uninstall ────────────────────────────────────────────────
if [ "$UNINSTALL" = true ]; then
    print_warning "⚠️  Starting Uninstallation..."
    for i in $(seq 1 20); do
        systemctl stop "${SERVICE_NAME}@${i}"    2>/dev/null || true
        systemctl disable "${SERVICE_NAME}@${i}" 2>/dev/null || true
    done
    systemctl stop "${SERVICE_NAME}"    2>/dev/null || true
    systemctl disable "${SERVICE_NAME}" 2>/dev/null || true
    [ -f "/etc/systemd/system/${SERVICE_NAME}@.service" ] && rm "/etc/systemd/system/${SERVICE_NAME}@.service"
    [ -f "/etc/systemd/system/${SERVICE_NAME}.service"  ] && rm "/etc/systemd/system/${SERVICE_NAME}.service"
    systemctl daemon-reload
    [ -d "$APP_DIR" ] && rm -rf "$APP_DIR"
    print_status "✅ Uninstalled successfully!"
    exit 0
fi

# Check root
if [ "$(id -u)" -ne 0 ]; then
    print_error "This script must be run as root (use sudo)"
    exit 1
fi

print_status "🚀 Starting Installation... (Workers: $WORKER_COUNT)"

# ─── System Dependencies ──────────────────────────────────────
# แค่ curl + ffmpeg(+ffprobe) — ไม่ต้องมี Node แล้ว (SCP ถูกถอดออก อัพขึ้น S3 โดยตรง)
print_status "Installing system dependencies (curl, ffmpeg)..."
if command -v apt-get &>/dev/null; then
    apt-get update -qq
    apt-get install -y -qq curl ffmpeg
elif command -v yum &>/dev/null; then
    yum install -y curl ffmpeg
elif command -v dnf &>/dev/null; then
    dnf install -y curl ffmpeg
fi

for cmd in curl ffmpeg ffprobe; do
    if ! command -v $cmd &>/dev/null; then
        print_error "$cmd not found. Please install it manually."
        exit 1
    fi
done

# ─── Stop existing services ───────────────────────────────────
print_status "Stopping existing services..."
systemctl stop ${SERVICE_NAME}@* 2>/dev/null || true
systemctl stop ${SERVICE_NAME}   2>/dev/null || true

# ─── Create app directory ─────────────────────────────────────
print_status "Creating app directory: $APP_DIR"
mkdir -p "$APP_DIR"
cd "$APP_DIR"

# ─── Download binary ──────────────────────────────────────────
ARCH=$(uname -m)
if [ "$ARCH" = "x86_64" ]; then
    BINARY="linux"
elif [ "$ARCH" = "aarch64" ]; then
    BINARY="linux-arm64"
else
    print_error "Unsupported architecture: $ARCH"
    exit 1
fi

print_status "Downloading binary ($BINARY) from latest release..."
curl -fsSL "$RELEASES_URL/$BINARY" -o "$APP_DIR/$APP_NAME"
chmod +x "$APP_DIR/$APP_NAME"
print_status "Binary downloaded."

# ─── Create .env ─────────────────────────────────────────────
# ⚠ ตัวโปรแกรมอ่าน DATABASE_URL (ไม่ใช่ MONGODB_URI แบบระบบเก่า)
print_status "Creating .env file..."
cat > "$APP_DIR/.env" <<EOF
DATABASE_URL=$DATABASE_URL
STORAGE_ID=$STORAGE_ID
STORAGE_PATH=$STORAGE_PATH
SCRAPER_URL=$SCRAPER_URL
DASHBOARD_PORT=$DASHBOARD_PORT
MEDIA_LAYOUT=$MEDIA_LAYOUT
EOF

# ─── Systemd service template ─────────────────────────────────
print_status "Creating systemd service template..."

cat > /etc/systemd/system/${SERVICE_NAME}@.service <<EOF
[Unit]
Description=Worker Download %i
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/$APP_NAME
Restart=always
RestartSec=5
EnvironmentFile=$APP_DIR/.env
Environment="WORKER_ID=download_$(hostname)@%i"
# SIGTERM → worker คืนงานเข้าคิว (Release) + mark ตัวเอง offline ก่อนปิด
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF

# ─── Enable & start workers ───────────────────────────────────
systemctl daemon-reload
print_status "Starting $WORKER_COUNT worker(s)..."
for i in $(seq 1 $WORKER_COUNT); do
    systemctl enable ${SERVICE_NAME}@$i
    systemctl start  ${SERVICE_NAME}@$i
    sleep 0.3
done

# ─── Verify ───────────────────────────────────────────────────
sleep 2
RUNNING=0
for i in $(seq 1 $WORKER_COUNT); do
    systemctl is-active --quiet ${SERVICE_NAME}@$i && RUNNING=$((RUNNING+1))
done

echo ""
echo "============================================"
if [ $RUNNING -eq $WORKER_COUNT ]; then
    print_status "✅ Installation completed successfully!"
else
    print_warning "$RUNNING of $WORKER_COUNT workers running — check logs below"
    journalctl -u "${SERVICE_NAME}@1" -n 15 --no-pager
fi
echo "============================================"
echo ""
echo "  Directory:  $APP_DIR"
echo "  Workers:    $RUNNING / $WORKER_COUNT running"
echo "  Dashboard:  http://0.0.0.0:$DASHBOARD_PORT (served by worker @1)"
echo ""
echo "  Commands:"
echo "    View logs:   journalctl -u \"${SERVICE_NAME}@*\" -f"
echo "    Worker 1:    journalctl -u \"${SERVICE_NAME}@1\" -f"
echo "    Restart all: for i in \$(seq 1 $WORKER_COUNT); do systemctl restart ${SERVICE_NAME}@\$i; done"
echo "    Stop all:    for i in \$(seq 1 $WORKER_COUNT); do systemctl stop ${SERVICE_NAME}@\$i; done"
echo "    Uninstall:   curl -fsSL https://raw.githubusercontent.com/$GITHUB_REPO/main/install.sh | sudo bash -s -- --uninstall"
echo "============================================"
