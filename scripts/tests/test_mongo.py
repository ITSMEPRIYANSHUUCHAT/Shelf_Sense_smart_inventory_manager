from dotenv import load_dotenv
from pymongo import MongoClient
from urllib.parse import quote_plus
import os

load_dotenv()

MONGO_URI = os.getenv('MONGO_URI')  # Your full URI from .env, e.g., mongodb+srv://user:pass@cluster.mmq51.mongodb.net/shelfsense?retryWrites=true&w=majority

# Escape if needed (handles special chars)
if '@' in MONGO_URI:
    # Parse and re-build to escape
    from urllib.parse import urlparse
    parsed = urlparse(MONGO_URI)
    user_pass = quote_plus(parsed.username) + ':' + quote_plus(parsed.password)
    MONGO_URI = f"mongodb+srv://{user_pass}@{parsed.hostname}/{parsed.path[1:]}?{parsed.query}"


client = MongoClient(MONGO_URI)
db = client['shelf_sense_db']
print(client.server_info())
