#!/bin/bash
set -e

echo "🚀 Starting backend + frontend..."

# === BACKEND ===
cd /opt/app

# The AICE system creates "venv" (no dot), so activate that:
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    # Fallback if venv isn't found
    export PATH="/opt/conda/bin:$PATH"
fi

# Install requirements using the active Python
python -m pip install -r requirements.txt -q

# Run backend in background
python main.py &
BACKEND_PID=$!
echo "✅ Backend started (PID $BACKEND_PID)"

# === FRONTEND ===
cd /opt/app/frontend
export NODE_OPTIONS="--max-old-space-size=6144"

# Proxy settings for UBS network
npm config set proxy http://webproxy:8080
npm config set https-proxy http://webproxy:8080
npm config set strict-ssl false
export HTTP_PROXY=http://webproxy:8080
export HTTPS_PROXY=http://webproxy:8080

npm ci
npm run build

echo "🌐 Starting frontend on port 8080..."
npm run serve
