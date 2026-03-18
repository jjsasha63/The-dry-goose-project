#!/bin/bash
set -e

echo "🚀 Starting backend + frontend..."

# === BACKEND ===
cd /opt/app
source .venv/bin/activate 2>/dev/null || true
pip install -r requirements.txt -q
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
