import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials  # OAuth token (headless)
import time

load_dotenv()

def check_flag():
    token_path = os.getenv('OAUTH_TOKEN_PATH', './token.json')  # FIXED: OAuth token path
    folder_id = os.getenv('DRIVE_FOLDER_ID')

    if not folder_id:
        print("Error: DRIVE_FOLDER_ID not set in .env.")
        return False

    if not os.path.exists(token_path):
        print(f"Error: OAuth token missing at {token_path}. Run generate_token.py for initial setup.")
        return False

    creds = Credentials.from_authorized_user_file(
        token_path, scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    service = build('drive', 'v3', credentials=creds)

    max_attempts = 10  # 5 mins total
    for i in range(1, max_attempts + 1):
        query = f"'{folder_id}' in parents and name='ready.txt'"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        if files:
            print(f"Flag ready.txt found in shelfstorage—proceeding to next steps")
            return True
        else:
            print(f"Waiting for manual Colab run... (attempt {i}/{max_attempts})")
            time.sleep(30)

    print("Timeout: ready.txt not found—check shelfstorage folder in Drive.")
    return False

if __name__ == "__main__":
    check_flag()