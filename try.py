#!/bin/bash
set -e

echo "🚀 Creating venv + Backend + Frontend..."

cd /opt/app

# === CREATE VENV (if missing) ===
if [ ! -d "venv" ]; then
  echo "Creating venv..."
  /opt/conda/bin/python -m venv venv
  source venv/bin/activate
  pip install --upgrade pip
else
  source venv/bin/activate
fi

# Install requirements (user mode)
pip install -r requirements.txt --user --no-cache-dir

# Backend
python main.py &
BACKEND_PID=$!
echo "Backend PID: $BACKEND_PID"

# === FRONTEND ===
cd /opt/app/frontend
export NODE_OPTIONS="--max-old-space-size=6144"
npm ci
npm run build

echo "Frontend on 8080..."
npm run serve
