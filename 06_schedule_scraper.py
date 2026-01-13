"""
03_schedule_scraper.py (API REPAIR VERSION)
===========================================
THE BIBLE - Step 3: Schedule Manager
1. Fixes 'Pending' names in your corrupt file.
2. Grabs all new games instantly (Fast enough to run daily).
"""

import pandas as pd
import requests
import logging
from datetime import date, timedelta
import time

# --- CONFIGURATION ---
OUTPUT_FILE = "master_game_logs_2026.csv"
START_DATE = date(2025, 11, 4)  # Start of season
END_DATE = date.today()         # Today
# ---------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger()

def get_dates():
    delta = END_DATE - START_DATE
    return [START_DATE + timedelta(days=i) for i in range(delta.days + 1)]

def scrape_api_scoreboard(day_obj):
    date_str = day_obj.strftime("%Y%m%d")
    url = f"http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&groups=50&limit=1000"
    
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        games = []
        
        for event in data.get('events', []):
            game_id = event['id']
            comps = event['competitions'][0]['competitors']
            
            # GET REAL NAMES
            team_a = comps[0]['team']['displayName']
            team_b = comps[1]['team']['displayName']
            
            # Handle Home/Away
            if comps[0]['homeAway'] == 'away':
                away_team, home_team = team_a, team_b
            else:
                away_team, home_team = team_b, team_a
            
            link = f"https://www.espn.com/mens-college-basketball/matchup?gameId={game_id}"
            
            games.append({
                'Date': day_obj.strftime("%Y-%m-%d"),
                'GameID': game_id,
                'Team': away_team, 
                'Opponent': home_team,
                'Location': 'N',
                'Link': link
            })
        return games
    except Exception as e:
        logger.error(f"❌ Error on {date_str}: {e}")
        return []

def main():
    logger.info("🚀 Starting Schedule Repair...")
    
    # 1. Scrape everything fresh (Fastest way to fix 'Pending' names)
    all_games = []
    dates = get_dates()
    
    # Batch scrape
    for i, day in enumerate(dates):
        games = scrape_api_scoreboard(day)
        if games:
            all_games.extend(games)
        
        # Show progress every 10 days
        if i % 10 == 0: 
            logger.info(f"📅 Processed up to {day}...")

    if not all_games:
        logger.error("❌ No games found! API might be blocked.")
        return

    # 2. Save and Overwrite the corrupt file
    df = pd.DataFrame(all_games)
    df.to_csv(OUTPUT_FILE, index=False)
    logger.info(f"✅ REPAIR COMPLETE. Saved {len(df)} clean records to {OUTPUT_FILE}")
    logger.info("👉 You can now run Step 06 (Stats Scraper).")

if __name__ == "__main__":
    main()
