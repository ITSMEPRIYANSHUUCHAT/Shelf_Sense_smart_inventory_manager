# check_flag.py
import os
import time
import json
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

load_dotenv()

def check_flag(token_path=None, folder_id=None, max_attempts=10, sleep_seconds=30):
    """
    Polls Google Drive for a file named 'ready.txt' in DRIVE_FOLDER_ID using OAuth token.json.
    Raises RuntimeError on unrecoverable failures (missing env, missing token, auth failure, timeout).
    Returns True on success.
    """
    token_path = token_path or os.getenv('OAUTH_TOKEN_PATH', './token.json')
    folder_id = folder_id or os.getenv('DRIVE_FOLDER_ID')

    if not folder_id:
        raise RuntimeError("Error: DRIVE_FOLDER_ID not set in .env or environment.")

    if not os.path.exists(token_path):
        raise RuntimeError(f"Error: OAuth token missing at {token_path}. Run generate_token.py for initial setup.")

    # load credentials
    try:
        creds = Credentials.from_authorized_user_file(
            token_path, scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
    except Exception as e:
        raise RuntimeError(f"Failed to load credentials from {token_path}: {e}")

    # refresh if expired or close to expiry
    try:
        if not creds.valid:
            if creds.expired and creds.refresh_token:
                print("Credentials expired — refreshing via refresh_token.")
                creds.refresh(Request())
                # save refreshed token back to disk so next runs will be valid
                with open(token_path, 'w') as f:
                    f.write(creds.to_json())
                print("Refreshed credentials saved.")
            else:
                raise RuntimeError("Credentials invalid and no refresh_token available. Regenerate token using generate_token.py")
    except Exception as e:
        raise RuntimeError(f"Failed to refresh credentials: {e}")

    # build service (disable discovery cache to avoid warnings)
    try:
        service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    except Exception as e:
        raise RuntimeError(f"Failed to build Drive service: {e}")

    query = f"'{folder_id}' in parents and name='ready.txt' and trashed=false"

    for i in range(1, max_attempts + 1):
        try:
            results = service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get('files', [])
            if files:
                print("Flag ready.txt found in shelfstorage—proceeding to next steps")
                return True
            else:
                print(f"Waiting for manual Colab run... (attempt {i}/{max_attempts})")
                time.sleep(sleep_seconds)
        except HttpError as he:
            # catch auth 401 or other errors
            status = getattr(he, 'status_code', None)
            print(f"Drive API HttpError (status={status}): {he}")
            # if 401, attempt one refresh and retry immediately
            if status == 401 and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    with open(token_path, 'w') as f:
                        f.write(creds.to_json())
                    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
                    print("Refreshed credentials after 401 and retried.")
                    continue
                except Exception as e:
                    raise RuntimeError(f"Refresh after 401 failed: {e}")
            else:
                # for other transient errors, wait and retry
                time.sleep(sleep_seconds)
                continue
        except Exception as e:
            print("Unexpected error while querying Drive:", e)
            time.sleep(sleep_seconds)
            continue

    # not found after polling -> raise so Airflow marks task failed
    raise RuntimeError("Timeout: ready.txt not found—check shelfstorage folder in Drive.")


if __name__ == "__main__":
    # exit non-zero if check fails (useful for local debugging / scripts)
    check_flag()
