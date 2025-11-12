from google_auth_oauthlib.flow import InstalledAppFlow
import json

flow = InstalledAppFlow.from_client_secrets_file(
    'client_secrets.json',
    scopes = ['https://www.googleapis.com/auth/drive']
)
creds = flow.run_local_server(port=0)
print("✅ Token obtained and saved to token.json")
with open('token.json', 'w') as token:
    token.write(creds.to_json())
