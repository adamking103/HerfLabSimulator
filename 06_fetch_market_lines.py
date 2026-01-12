import requests
import pandas as pd
import os
from datetime import datetime, timedelta, timezone

# ==========================================
# CONFIGURATION
# ==========================================
API_KEY = "YOUR_ODDS_API_KEY_HERE"  # <--- PASTE YOUR KEY HERE
SPORT = 'basketball_ncaab'
REGIONS = 'us,us2'
MARKETS = 'spreads,totals'
ODDS_FORMAT = 'american'
DATE_FORMAT = 'iso'

# ==========================================
# FETCH LOGIC
# ==========================================
def get_odds():
    # Get 48-hour window (matches your successful test)
    now = datetime.now(timezone.utc)
    end_time = now + timedelta(hours=48)
    
    start_str = now.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    print(f"🔌 Connecting to The Odds API ({SPORT})...")
    
    url = f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds'
    params = {
        'apiKey': API_KEY,
        'regions': REGIONS,
        'markets': MARKETS,
        'oddsFormat': ODDS_FORMAT,
        'dateFormat': DATE_FORMAT,
        'commenceTimeFrom': start_str,
        'commenceTimeTo': end_str
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        print(f"❌ Error fetching odds: {response.status_code}")
        print(response.text)
        return None
        
    return response.json()

def process_odds(data):
    rows = []
    print(f"🔄 Processing {len(data)} games...")
    
    # Priority: Kambi Books (Oaklawn/Bally) -> Sharp Books -> Retail
    preferred_books = ['ballybet', 'betrivers', 'draftkings', 'fanduel', 'mgm', 'bovada']
    
    for game in data:
        home_team = game['home_team']
        away_team = game['away_team']
        commence_time = game['commence_time']
        
        bookmakers = game['bookmakers']
        if not bookmakers: continue
            
        selected_book = None
        
        # 1. Try to find a preferred book
        for pref in preferred_books:
            for book in bookmakers:
                if book['key'] == pref:
                    selected_book = book
                    break
            if selected_book: break
            
        # 2. Fallback to first available if none found
        if not selected_book:
            selected_book = bookmakers[0]
        
        # Extract Lines
        spread_home = None
        total = None
        
        for market in selected_book['markets']:
            if market['key'] == 'spreads':
                for outcome in market['outcomes']:
                    if outcome['name'] == home_team:
                        spread_home = outcome['point']
            elif market['key'] == 'totals':
                if market['outcomes']:
                    total = market['outcomes'][0]['point']
                
        rows.append({
            'Date': commence_time,
            'Visitor': away_team,
            'Home': home_team,
            'Market_Spread_Home': spread_home,
            'Market_Total': total,
            'Sportsbook': selected_book['title']
        })
        
    return pd.DataFrame(rows)

if __name__ == "__main__":
    data = get_odds()
    if data:
        df = process_odds(data)
        if not df.empty:
            # 1. Save standard file for the App to read
            filename = "Today_Market_Lines.csv"
            df.to_csv(filename, index=False)
            
            # 2. Save timestamped backup for history
            os.makedirs("logs", exist_ok=True)
            backup_name = f"logs/Lines_{datetime.now().strftime('%Y-%m-%d')}.csv"
            df.to_csv(backup_name, index=False)
            
            print(f"✅ SUCCESS: Saved {len(df)} games to {filename}")
            print(f"   (Primary Source: {df['Sportsbook'].mode()[0] if not df.empty else 'N/A'})")
        else:
            print("⚠️ No lines available right now.")
