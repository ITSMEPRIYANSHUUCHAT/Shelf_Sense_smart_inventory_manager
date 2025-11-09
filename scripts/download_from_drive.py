import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaIoBaseDownload
import io

load_dotenv()

def download_from_drive():
    key_path = os.getenv('DRIVE_SERVICE_ACCOUNT_KEY_PATH')
    folder_id = os.getenv('DRIVE_FOLDER_ID')
    output_path = '/tmp/result.json'

    if not key_path or not os.path.exists(key_path):
        print(f"Error: Service account key missing at {key_path}. Check .env or file name.")
        return

    if not folder_id:
        print("Error: DRIVE_FOLDER_ID not set in .env.")
        return

    creds = Credentials.from_service_account_file(key_path, scopes=['https://www.googleapis.com/auth/drive'])
    service = build('drive', 'v3', credentials=creds)

    # Find result.json in shelfstorage folder
    query = f"'{folder_id}' in parents and name = 'result.json'"
    results = service.files().list(q=query, fields="files(id)").execute()
    if results.get('files', []):
        file_id = results['files'][0]['id']
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        fh.seek(0)
        with open(output_path, 'wb') as f:
            f.write(fh.read())
        print("Downloaded result.json from Drive shelfstorage folder")
    else:
        print("No result.json found in Drive—check Colab output")

if __name__ == "__main__":
    download_from_drive()