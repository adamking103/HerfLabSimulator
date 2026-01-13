import pandas as pd
import requests
from bs4 import BeautifulSoup

# A real game URL (Duke vs Maine) to test
TEST_URL = "https://kenpom.com/game.php?g=68" 

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "https://kenpom.com/"
}

print(f"🕵️ Testing connection to: {TEST_URL}")
try:
    response = requests.get(TEST_URL, headers=HEADERS)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # parsing tables
    tables = pd.read_html(str(soup))
    
    print(f"✅ Found {len(tables)} tables on the page.\n")
    
    for i, df in enumerate(tables):
        print(f"--- TABLE {i} COLUMNS ---")
        print(list(df.columns))
        print("First row of data:")
        print(df.head(1).to_string())
        print("\n")
        
except Exception as e:
    print(f"❌ CRASHED: {e}")
