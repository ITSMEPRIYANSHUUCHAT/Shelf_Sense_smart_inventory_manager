#!/usr/bin/env python3
import os
import io
import json
import logging
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

TOKEN_PATH = os.getenv("DRIVE_TOKEN_PATH", "/opt/airflow/token.json")
FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
OUTPUT_PATH = os.getenv("DRIVE_DATA_DIR", "/opt/airflow/data") + "/result.json"
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]  # minimal scope for download

def get_drive_service():
    if not os.path.exists(TOKEN_PATH):
        raise FileNotFoundError(f"OAuth token not found at {TOKEN_PATH}. Create token.json via InstalledAppFlow.")
    creds = Credentials.from_authorized_user_file(TOKEN_PATH, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def download_from_drive():
    if not FOLDER_ID:
        log.error("DRIVE_FOLDER_ID not set in environment.")
        return

    svc = get_drive_service()

    q = f"'{FOLDER_ID}' in parents and name = 'result.json'"
    log.info("Searching for result.json in folder %s", FOLDER_ID)
    res = svc.files().list(q=q, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    files = res.get("files", [])
    if not files:
        log.warning("No result.json found in Drive folder %s", FOLDER_ID)
        return

    file_id = files[0]["id"]
    log.info("Found result.json (id=%s). Downloading to %s", file_id, OUTPUT_PATH)

    request = svc.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    try:
        while not done:
            status, done = downloader.next_chunk()
            if status:
                log.info("Download progress: %d%%", int(status.progress() * 100))
        fh.seek(0)
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        with open(OUTPUT_PATH, "wb") as out:
            out.write(fh.read())
        log.info("Downloaded result.json to %s", OUTPUT_PATH)
    except Exception as e:
        log.exception("Failed to download result.json: %s", e)

if __name__ == "__main__":
    download_from_drive()
