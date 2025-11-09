import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaFileUpload
import glob

load_dotenv()

def upload_to_drive():
    key_path = os.getenv('DRIVE_SERVICE_ACCOUNT_KEY_PATH')
    folder_id = os.getenv('DRIVE_FOLDER_ID')
    data_dir = '/opt/airflow/data/processed'

    if not key_path or not os.path.exists(key_path):
        print(f"Error: Service account key missing at {key_path}. Create it in Google Cloud IAM.")
        return

    if not folder_id:
        print("Error: DRIVE_FOLDER_ID not set in .env.")
        return

    creds = Credentials.from_service_account_file(key_path, scopes=['https://www.googleapis.com/auth/drive'])
    service = build('drive', 'v3', credentials=creds)

    # Upload all CSV files to shelfstorage folder
    for file in glob.glob(os.path.join(data_dir, '*.csv')):
        file_name = os.path.basename(file)
        file_metadata = {'name': file_name, 'parents': [folder_id]}
        media = MediaFileUpload(file, mimetype='text/csv')
        try:
            file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print(f"Uploaded {file_name} to Drive shelfstorage folder")
        except Exception as e:
            print(f"Upload error for {file_name}: {e}")

if __name__ == "__main__":
    upload_to_drive()