#!/bin/bash
set -e

echo "🚀 Building + serving with npm preview..."

cd /opt/app/frontend

# Fix npm proxy (UBS)
npm config set proxy http://webproxy:8080
npm config set https-proxy http://webproxy:8080
npm config set strict-ssl false

export HTTP_PROXY=http://webproxy:8080
export HTTPS_PROXY=http://webproxy:8080
export NODE_OPTIONS="--max-old-space-size=6144"

# Install + build + serve
npm ci
npm run build
npm run preview  # Vite production server (dist/)
