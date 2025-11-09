import os
from dotenv import load_dotenv

load_dotenv()

print("All Keys Loaded:")
print("DRIVE_KEY:", os.getenv('DRIVE_SERVICE_ACCOUNT_KEY_PATH'))
print("DRIVE_FOLDER:", os.getenv('DRIVE_FOLDER_ID'))
print("MONGO_URI:", os.getenv('MONGO_URI'))
print("MONGO_USER:", os.getenv('MONGO_USER'))
print("MONGO_PASS:", os.getenv('MONGO_PASS'))
print("MONGO_CLUSTER:", os.getenv('MONGO_CLUSTER'))

# Test if file exists
key_path = os.getenv('DRIVE_SERVICE_ACCOUNT_KEY_PATH')
if key_path and os.path.exists(key_path):
    print("Key file exists!")
else:
    print("Key file missing—check path!")