"""
06_espn_scraper.py
==================
THE BIBLE - Step 6: ESPN Box Score Scraper (Team Comparison Method)
Scrapes Scores from the header and Four Factors from the 'Team Stats' comparison table.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
import logging
import re

# --- CONFIGURATION ---
INPUT_FILE = "master_game_logs_2026.csv"
OUTPUT_FILE = "master_box_scores_2026.csv"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def extract_game_id(url):
    """Finds the digits in an ESPN URL."""
    if pd.isna(url): return None
    match = re.search(r'gameId[=/]?(\d+)', str(url))
    if match: return match.group(1)
    match = re.search(r'/(\d+)', str(url))
    return match.group(1) if match else None

def parse_stat_val(val_str):
    """Parses '28-57' into (28, 57) or '12' into (12, 0)."""
    val_str = str(val_str).replace(' ', '')
    if '-' in val_str:
        parts = val_str.split('-')
        return float(parts[0]), float(parts[1])
    try:
        return float(val_str), 0.0
    except:
        return 0.0, 0.0

def calculate_four_factors(team_stats, opp_stats):
    """Calculates efficiency metrics."""
    try:
        # Unpack
        fga = team_stats['fga']
        fgm = team_stats['fgm']
        f3pm = team_stats['3ptm']
        fta = team_stats['fta']
        to = team_stats['to']
        orb = team_stats['orb']
        opp_drb = opp_stats['drb'] # Needed for OR%

        # 1. Possessions
        # Formula: FGA - ORB + TO + (0.475 * FTA)
        poss = fga - orb + to + (0.475 * fta)
        if poss == 0: return {}

        # 2. eFG%
        efg = (fgm + (0.5 * f3pm)) / fga if fga > 0 else 0

        # 3. TO%
        to_pct = to / poss

        # 4. OR%
        or_pct = orb / (orb + opp_drb) if (orb + opp_drb) > 0 else 0

        # 5. FTR
        ftr = fta / fga if fga > 0 else 0

        return {
            'Possessions': round(poss, 1),
            'eFG%': round(efg * 100, 1),
            'TO%': round(to_pct * 100, 1),
            'OR%': round(or_pct * 100, 1),
            'FTR': round(ftr * 100, 1)
        }
    except Exception as e:
        return {}

def scrape_espn_game(game_id):
    url = f"https://www.espn.com/mens-college-basketball/matchup?gameId={game_id}"
    try:
        time.sleep(random.uniform(0.5, 1.5))
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')

        # --- 1. GET SCORES (From Header) ---
        # ESPN Matchup pages have the score in the "Gamestrip"
        scores = soup.find_all('div', class_='Gamestrip__Score')
        if not scores or len(scores) < 2:
            # Fallback: look for generic .score class
            scores = soup.find_all(class_='score')
        
        if len(scores) < 2:
            return None # Can't find scores, skip
            
        score_away = int(scores[0].text.strip())
        score_home = int(scores[1].text.strip())

        # --- 2. GET TEAM STATS TABLE ---
        # We look for the table row-by-row based on text keywords
        # This is more robust than looking for specific classes that change
        
        rows = soup.find_all('tr')
        
        # Init raw stats containers
        # index 0 = Away, index 1 = Home (standard ESPN order)
        s_away = {'fgm':0, 'fga':0, '3ptm':0, 'fta':0, 'to':0, 'orb':0, 'drb':0}
        s_home = {'fgm':0, 'fga':0, '3ptm':0, 'fta':0, 'to':0, 'orb':0, 'drb':0}

        def get_vals(row):
            cols = row.find_all('td')
            # Col 0 = Label, Col 1 = Away, Col 2 = Home
            if len(cols) < 3: return None, None
            return cols[1].text.strip(), cols[2].text.strip()

        for row in rows:
            txt = row.text.lower()
            
            # Field Goals (FG)
            if 'fg' in txt and '%' not in txt:
                v1, v2 = get_vals(row)
                s_away['fgm'], s_away['fga'] = parse_stat_val(v1)
                s_home['fgm'], s_home['fga'] = parse_stat_val(v2)
                
            # 3 Pointers (3PT)
            elif '3pt' in txt and '%' not in txt:
                v1, v2 = get_vals(row)
                s_away['3ptm'], _ = parse_stat_val(v1)
                s_home['3ptm'], _ = parse_stat_val(v2)

            # Free Throws (FT)
            elif 'ft' in txt and '%' not in txt:
                v1, v2 = get_vals(row)
                _, s_away['fta'] = parse_stat_val(v1)
                _, s_home['fta'] = parse_stat_val(v2)

            # Turnovers
            elif 'turnovers' in txt and 'points' not in txt:
                v1, v2 = get_vals(row)
                s_away['to'], _ = parse_stat_val(v1)
                s_home['to'], _ = parse_stat_val(v2)
            
            # Offensive Rebounds
            elif 'offensive rebounds' in txt:
                v1, v2 = get_vals(row)
                s_away['orb'], _ = parse_stat_val(v1)
                s_home['orb'], _ = parse_stat_val(v2)

            # Defensive Rebounds
            elif 'defensive rebounds' in txt:
                v1, v2 = get_vals(row)
                s_away['drb'], _ = parse_stat_val(v1)
                s_home['drb'], _ = parse_stat_val(v2)

        # --- 3. CALCULATE FACTORS ---
        final_away = calculate_four_factors(s_away, s_home)
        final_home = calculate_four_factors(s_home, s_away)
        
        # Add Scores
        final_away['Score'] = score_away
        final_home['Score'] = score_home
        
        return [final_away, final_home]

    except Exception as e:
        return None

def main():
    logger.info("🚀 Starting ESPN Team Stats Scraper...")
    
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        logger.error(f"❌ {INPUT_FILE} not found!")
        return

    results = []
    total = len(df)
    
    for i, row in df.iterrows():
        team = row['Team']
        opp = row['Opponent']
        url = row.get('Link', row.get('Url', ''))
        
        game_id = extract_game_id(url)
        if not game_id: continue
            
        logger.info(f"[{i+1}/{total}] Scraping {team} vs {opp} (ID: {game_id})")
        
        data = scrape_espn_game(game_id)
        
        if data:
            # Assume index 0 is Team (Away) and 1 is Opp (Home)
            # This is a simplification. Ideally we fuzzy match names.
            # But usually ESPN links match the schedule structure.
            
            # Simple fuzzy check:
            # If the user's schedule says "at Duke", Duke is Home (index 1).
            is_home_game = False
            if 'Location' in row and (row['Location'] == 'H' or row['Location'] == 'Home'):
                is_home_game = True
            
            if is_home_game:
                my_stats = data[1] # Home
                opp_stats = data[0] # Away
            else:
                my_stats = data[0] # Away
                opp_stats = data[1] # Home

            record = {
                'Date': row['Date'],
                'Team': team,
                'Opponent': opp,
                'Location': row.get('Location', 'N'),
                'TeamScore': my_stats.get('Score', 0),
                'OpponentScore': opp_stats.get('Score', 0),
                'Possessions': my_stats.get('Possessions', 0),
                'eFG%': my_stats.get('eFG%', 0),
                'TO%': my_stats.get('TO%', 0),
                'OR%': my_stats.get('OR%', 0),
                'FTR': my_stats.get('FTR', 0)
            }
            results.append(record)
        
        if len(results) % 10 == 0:
            pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)

    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    logger.info(f"✅ DONE. Saved {len(results)} records to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
