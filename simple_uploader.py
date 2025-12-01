"""
Simple uploader that saves files locally and serves them via a web server
"""

import os
import uuid
import logging
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from typing import Optional

import config

logger = logging.getLogger(__name__)


class SimpleUploader:
    def __init__(self, upload_dir: str = "uploads", port: int = 8080):
        self.upload_dir = os.path.abspath(upload_dir)
        self.port = port
        os.makedirs(self.upload_dir, exist_ok=True)

        # Start web server in background
        self.start_web_server()

    def start_web_server(self):
        """Start a simple HTTP server to serve uploaded files."""

        def run_server():
            os.chdir(self.upload_dir)
            server = HTTPServer(('localhost', self.port), SimpleHTTPRequestHandler)
            server.serve_forever()

        thread = threading.Thread(target=run_server, daemon=True)
        thread.start()
        logger.info(f"Web server started on http://localhost:{self.port}")

    def upload_bytes(self, filename: str, bytes_content: bytes, mime_type: str = "image/png") -> str:
        """Save file locally and return a web-accessible URL."""
        # Generate unique filename
        file_id = uuid.uuid4().hex
        ext = os.path.splitext(filename)[1] or ".png"
        new_filename = f"{file_id}{ext}"

        # Save file
        filepath = os.path.join(self.upload_dir, new_filename)
        with open(filepath, "wb") as f:
            f.write(bytes_content)

        # Return URL
        return f"http://localhost:{self.port}/{new_filename}"


# Global instance
simple_uploader = SimpleUploader(port=config.LOCAL_SERVER_PORT if hasattr(config, 'LOCAL_SERVER_PORT') else 8080)