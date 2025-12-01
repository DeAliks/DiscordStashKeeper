"""
Uploader using OAuth 2.0 (requires user authentication)
"""

import io
import os
import pickle
import logging
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError

import config

logger = logging.getLogger(__name__)

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']


def get_credentials():
    """Gets valid user credentials from storage."""
    creds = None
    # The file token.pickle stores the user's access and refresh tokens.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # You need to download credentials.json from Google Cloud Console
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials_oauth.json', SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds


def upload_bytes(filename: str, bytes_content: bytes, mime_type: str = "image/png") -> str:
    """Upload bytes to Drive using OAuth credentials."""
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)

    file_metadata = {'name': filename}
    if config.DRIVE_UPLOAD_FOLDER_ID:
        file_metadata['parents'] = [config.DRIVE_UPLOAD_FOLDER_ID]

    media = MediaIoBaseUpload(io.BytesIO(bytes_content), mimetype=mime_type)

    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,webViewLink'
        ).execute()

        # Make public if needed
        if config.DRIVE_PUBLIC_LINK:
            permission = {
                'type': 'anyone',
                'role': 'reader'
            }
            service.permissions().create(
                fileId=file['id'],
                body=permission
            ).execute()

        return file.get('webViewLink', f"https://drive.google.com/file/d/{file['id']}/view")

    except Exception as e:
        logger.exception("OAuth Drive upload failed: %s", e)
        raise