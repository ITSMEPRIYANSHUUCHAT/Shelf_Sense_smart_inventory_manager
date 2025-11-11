#!/usr/bin/env python3
import os
import glob
import logging
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaFileUpload
from dotenv import load_dotenv
import time

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

TOKEN_PATH = os.getenv("DRIVE_TOKEN_PATH", "/opt/airflow/token.json")
FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
DATA_DIR = os.getenv("DRIVE_DATA_DIR", "/opt/airflow/data") + "/processed"
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

MAX_RETRIES = 5
BASE_BACKOFF = 2

def get_drive_service():
    if not os.path.exists(TOKEN_PATH):
        raise FileNotFoundError(f"OAuth token not found at {TOKEN_PATH}. Create token.json via InstalledAppFlow.")
    creds = Credentials.from_authorized_user_file(TOKEN_PATH, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def upload_file(service, local_path, folder_id):
    name = os.path.basename(local_path)
    media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
    metadata = {"name": name, "parents": [folder_id]}
    request = service.files().create(body=metadata, media_body=media, fields="id", supportsAllDrives=True)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("Uploading %s (attempt %d/%d)", name, attempt, MAX_RETRIES)
            response = None
            while response is None:
                status, response = request.next_chunk()
                if status:
                    log.info("Progress %s: %d%%", name, int(status.progress() * 100))
            file_id = response.get("id")
            log.info("Uploaded %s -> %s", name, file_id)
            return file_id
        except Exception as e:
            backoff = BASE_BACKOFF * (2 ** (attempt - 1))
            log.warning("Upload failed for %s (attempt %d). Retrying in %d seconds. Error: %s", name, attempt, backoff, e)
            time.sleep(backoff)
    raise RuntimeError(f"Failed to upload {name} after {MAX_RETRIES} attempts")

def upload_to_drive():
    if not FOLDER_ID:
        log.error("DRIVE_FOLDER_ID not set")
        return
    if not os.path.isdir(DATA_DIR):
        log.info("No processed directory found at %s", DATA_DIR)
        return

    service = get_drive_service()
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    if not files:
        log.info("No CSV files to upload in %s", DATA_DIR)
        return

    for p in files:
        try:
            upload_file(service, p, FOLDER_ID)
        except Exception as e:
            log.exception("Error uploading %s: %s", p, e)

if __name__ == "__main__":
    upload_to_drive()
