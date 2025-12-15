import os
from dotenv import load_dotenv

load_dotenv()

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'forum_scraper')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

# Scraper configuration
BASE_URL = "https://gendercriticalresources.com/Support"
THREAD_URL_TEMPLATE = BASE_URL + "/showthread.php?tid={tid}&page={page}"
# Starting thread ID and ending thread ID
START_TID = 1
END_TID = 1000  # Adjust as needed

# Rate limiting
DELAY_BETWEEN_REQUESTS = 1  # seconds
MAX_RETRIES = 3
TIMEOUT = 30

# User agent to mimic a browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Cookies for authentication
# Load from environment variable COOKIES which should be in format:
# "name1=value1; name2=value2; ..."
# To get your cookies:
# 1. Log in to the forum in your browser
# 2. Open Developer Tools (F12)
# 3. Go to Network tab
# 4. Reload a forum page
# 5. Click on any request and find the 'Cookie' header in the request headers
# 6. Copy the entire cookie string (without the 'Cookie: ' prefix)
COOKIES_STRING = os.getenv('COOKIES', '')
# Parse into a dictionary
COOKIES = {}
if COOKIES_STRING:
    for cookie in COOKIES_STRING.split(';'):
        cookie = cookie.strip()
        if '=' in cookie:
            name, value = cookie.split('=', 1)
            COOKIES[name] = value
