#!/bin/bash
set -e

echo "🚀 Frontend + Backend..."

cd /opt/app/frontend

# Fix proxy + build frontend
npm config set proxy http://webproxy:8080
all -r requirements.txt -q
python main.py &  # Run in background
BACKEND_PID=$!
echo "✅ Backend started (PID $BACKEND_PID)"

# === FRONTEND ===
cd /opt/app/frontend
export NODE_OPTIONS="--max-old-space-size=6144"
npm ci
npm run build

echo "🌐 Starting frontend on port 8080..."
npm run serve  # Stays in foreground (keeps container alive)
