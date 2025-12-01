# drive_uploader.py
import io
import logging
import time
from typing import Optional

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.errors import HttpError

import config

logger = logging.getLogger(__name__)

DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive']

def build_drive_service():
    creds = ServiceAccountCredentials.from_json_keyfile_name(config.GOOGLE_CREDENTIALS_FILE, DRIVE_SCOPES)
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    return service

def upload_bytes(filename: str, bytes_content: bytes, mime_type: str = "image/png", max_retries: int = 3) -> str:
    """
    Upload bytes to Drive folder and return a shareable link.
    Returns webViewLink if available; otherwise constructs a drive file URL.
    """
    service = build_drive_service()
    folder_id = config.DRIVE_UPLOAD_FOLDER_ID or None

    file_metadata = {'name': filename}
    if folder_id:
        file_metadata['parents'] = [folder_id]

    media = MediaIoBaseUpload(io.BytesIO(bytes_content), mimetype=mime_type, resumable=False)

    for attempt in range(max_retries):
        try:
            file = service.files().create(body=file_metadata, media_body=media, fields='id,webViewLink').execute()
            file_id = file.get('id')
            web_view = file.get('webViewLink')
            # Set permission if requested
            if config.DRIVE_PUBLIC_LINK:
                try:
                    permission = {'type': 'anyone', 'role': 'reader'}
                    service.permissions().create(fileId=file_id, body=permission).execute()
                except HttpError as e:
                    logger.exception("Failed to set permission: %s", e)
            if web_view:
                return web_view
            return f"https://drive.google.com/file/d/{file_id}/view"
        except HttpError as e:
            logger.warning("Drive upload attempt %s failed: %s", attempt+1, e)
            time.sleep(1 + attempt*2)
        except Exception as e:
            logger.exception("Drive upload error: %s", e)
            time.sleep(1 + attempt*2)
    raise RuntimeError("Failed to upload to Drive after retries")
