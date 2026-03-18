#!/bin/bash
set -e

cd /opt/app/frontend

export NODE_OPTIONS="--max-old-space-size=6144"

npm ci
npm run build

cd dist

python3 << 'EOF'
import http.server
import socketserver
import os

PORT = 8080
DIRECTORY = '.'

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def do_GET(self):
        if self.path == '/healthz' or self.path == '/healthz/':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
            return

        path = self.translate_path(self.path)
        if os.path.exists(path) and not os.path.isdir(path):
            return super().do_GET()

        self.path = '/index.html'
        return super().do_GET()

with socketserver.TCPServer(('0.0.0.0', PORT), Handler) as httpd:
    print(f"Serving dist on http://0.0.0.0:{PORT}")
    httpd.serve_forever()
EOF
