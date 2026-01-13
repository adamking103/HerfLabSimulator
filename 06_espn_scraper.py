"""
06_espn_scraper.py
==================
THE BIBLE - Step 6: Stats Engine
Reads your valid schedule file and scrapes Scores + Efficiency.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
import logging
import re
import os

# --- CONFIGURATION ---
INPUT_FILE = "master_game_logs_2026.csv"   # The file you just fixed
OUTPUT_FILE = "master_box_scores_2026.csv" # The file 'The Bible' reads
# ---------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def extract_game_id(url):
    if pd.isna(url): return None
    match = re.search(r'gameId[=/]?(\d+)', str(url))
    if match: return match.group(1)
    return None

def parse_stat_val(val_str):
    """Parses '28-57' into (28, 57)."""
    val_str = str(val_str).replace(' ', '')
    if '-' in val_str:
        parts = val_str.split('-')
        try: return float(parts[0]), float(parts[1])
        except: return 0.0, 0.0
    try: return float(val_str), 0.0
    except: return 0.0, 0.0

def calculate_four_factors(team_stats, opp_stats):
    try:
        fga = team_stats['fga']
        fgm = team_stats['fgm']
        f3pm = team_stats['3ptm']
        fta = team_stats['fta']
        to = team_stats['to']
        orb = team_stats['orb']
        opp_drb = opp_stats['drb']

        # Possessions Formula
        poss = fga - orb + to + (0.475 * fta)
        if poss == 0: return {}

        return {
            'Possessions': round(poss, 1),
            'eFG%': round(((fgm + 0.5 * f3pm) / fga) * 100, 1) if fga > 0 else 0,
            'TO%': round((to / poss) * 100, 1),
            'OR%': round((orb / (orb + opp_drb)) * 100, 1) if (orb + opp_drb) > 0 else 0,
            'FTR': round((fta / fga) * 100, 1) if fga > 0 else 0
        }
    except: return {}

def scrape_espn_game(game_id):
    url = f"https://www.espn.com/mens-college-basketball/matchup?gameId={game_id}"
    try:
        time.sleep(random.uniform(0.5, 1.2)) # Polite delay
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')

        # 1. GET SCORES
        scores = soup.find_all('div', class_='Gamestrip__Score')
        if not scores: scores = soup.find_all(class_='score')
        if len(scores) < 2: return None
        
        try:
            score_away = int(scores[0].text.strip())
            score_home = int(scores[1].text.strip())
        except: return None

        # 2. GET STATS
        rows = soup.find_all('tr')
        s_away = {'fgm':0,'fga':0,'3ptm':0,'fta':0,'to':0,'orb':0,'drb':0}
        s_home = {'fgm':0,'fga':0,'3ptm':0,'fta':0,'to':0,'orb':0,'drb':0}

        def get_vals(row):
            cols = row.find_all('td')
            if len(cols) < 3: return None, None
            return cols[1].text.strip(), cols[2].text.strip()

        for row in rows:
            txt = row.text.lower()
            if 'fg' in txt and '%' not in txt:
                v1, v2 = get_vals(row)
                if v1:
                    s_away['fgm'], s_away['fga'] = parse_stat_val(v1)
                    s_home['fgm'], s_home['fga'] = parse_stat_val(v2)
            elif '3pt' in txt and '%' not in txt:
                v1, v2 = get_vals(row)
                if v1:
                    s_away['3ptm'], _ = parse_stat_val(v1)
                    s_home['3ptm'], _ = parse_stat_val(v2)
            elif 'ft' in txt and '%' not in txt:
                v1, v2 = get_vals(row)
                if v1:
                    _, s_away['fta'] = parse_stat_val(v1)
                    _, s_home['fta'] = parse_stat_val(v2)
            elif 'turnovers' in txt:
                v1, v2 = get_vals(row)
                if v1:
                    s_away['to'], _ = parse_stat_val(v1)
                    s_home['to'], _ = parse_stat_val(v2)
            elif 'offensive rebounds' in txt:
                v1, v2 = get_vals(row)
                if v1:
                    s_away['orb'], _ = parse_stat_val(v1)
                    s_home['orb'], _ = parse_stat_val(v2)
            elif 'defensive rebounds' in txt:
                v1, v2 = get_vals(row)
                if v1:
                    s_away['drb'], _ = parse_stat_val(v1)
                    s_home['drb'], _ = parse_stat_val(v2)

        # 3. CALCULATE FACTORS
        final_away = calculate_four_factors(s_away, s_home)
        final_home = calculate_four_factors(s_home, s_away)
        
        final_away['Score'] = score_away
        final_home['Score'] = score_home
        
        return [final_away, final_home]

    except Exception: return None

def main():
    logger.info("🚀 Starting Stats Scraper...")
    
    if not os.path.exists(INPUT_FILE):
        logger.error(f"❌ {INPUT_FILE} not found!")
        return

    df = pd.read_csv(INPUT_FILE)
    results = []
    
    # Optional: Load existing results to skip re-scraping
    processed_ids = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            existing = pd.read_csv(OUTPUT_FILE)
            if 'GameID' in existing.columns:
                processed_ids = set(existing['GameID'].astype(str))
                logger.info(f"⏩ Resuming... {len(processed_ids)} games already scraped.")
        except: pass

    logger.info(f"📋 Found {len(df)} games in schedule.")
    
    for i, row in df.iterrows():
        url = row.get('Link', row.get('Url', ''))
        game_id = extract_game_id(url)
        if not game_id or str(game_id) in processed_ids: continue
        
        team = row['Team']
        opp = row['Opponent']
        
        logger.info(f"[{i+1}/{len(df)}] Scraping {team} vs {opp}...")
        
        data = scrape_espn_game(game_id)
        
        if data:
            # data[0] is Away, data[1] is Home
            # We assume Schedule lists [Away] vs [Home] in that order
            my_stats = data[0]
            opp_stats = data[1]

            record = {
                'GameID': game_id,
                'Date': row['Date'],
                'Team': team,
                'Opponent': opp,
                'Location': 'N',
                'TeamScore': my_stats.get('Score', 0),
                'OpponentScore': opp_stats.get('Score', 0),
                'Possessions': my_stats.get('Possessions', 0),
                'eFG%': my_stats.get('eFG%', 0),
                'TO%': my_stats.get('TO%', 0),
                'OR%': my_stats.get('OR%', 0),
                'FTR': my_stats.get('FTR', 0)
            }
            results.append(record)
        
        # Save every 20 games
        if len(results) > 0 and len(results) % 20 == 0:
            pd.DataFrame(results).to_csv(OUTPUT_FILE, mode='a', header=not os.path.exists(OUTPUT_FILE), index=False)
            results = [] # Clear buffer
            logger.info("💾 Autosaved batch.")

    # Final flush
    if results:
        pd.DataFrame(results).to_csv(OUTPUT_FILE, mode='a', header=not os.path.exists(OUTPUT_FILE), index=False)
        logger.info("✅ DONE! All stats saved.")

if __name__ == "__main__":
    main()
