"""
01_master_scraper_with_stats.py
===============================
THE GOLDEN SCRIPT
1. Uses the 'Team Schedule' method to find games (Reliable).
2. Visits EACH game's box score to get real Four Factors (Accurate).
3. Saves the final 'master_box_scores_2026.csv'.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import logging
import random

# --- CONFIGURATION ---
SEASON_YEAR = 2026
OUTPUT_FILE = "master_box_scores_2026.csv"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

# --- MATH HELPER ---
def parse_stat_val(val_str):
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

# --- BOX SCORE SCRAPER ---
def get_real_stats(game_id, session):
    """Visits the game page to get REAL H-K column data."""
    url = f"https://www.espn.com/mens-college-basketball/matchup?gameId={game_id}"
    try:
        # Polite delay
        time.sleep(random.uniform(0.1, 0.3))
        r = session.get(url, timeout=10)
        soup = BeautifulSoup(r.content, 'html.parser')
        
        # We only need the stats table, we already have score/teams/date from schedule
        rows = soup.find_all('tr')
        
        # Containers: [Away, Home]
        stats = [{'fgm':0,'fga':0,'3ptm':0,'fta':0,'to':0,'orb':0,'drb':0}, 
                 {'fgm':0,'fga':0,'3ptm':0,'fta':0,'to':0,'orb':0,'drb':0}]
        
        def get_vals(row):
            cols = row.find_all('td')
            if len(cols) < 3: return None, None
            return cols[1].text.strip(), cols[2].text.strip()

        found_stats = False
        for row in rows:
            txt = row.text.lower()
            if 'fg' in txt and '%' not in txt:
                v1, v2 = get_vals(row)
                if v1:
                    stats[0]['fgm'], stats[0]['fga'] = parse_stat_val(v1)
                    stats[1]['fgm'], stats[1]['fga'] = parse_stat_val(v2)
                    found_stats = True
            elif '3pt' in txt and '%' not in txt:
                v1, v2 = get_vals(row)
                if v1:
                    stats[0]['3ptm'], _ = parse_stat_val(v1)
                    stats[1]['3ptm'], _ = parse_stat_val(v2)
            elif 'ft' in txt and '%' not in txt:
                v1, v2 = get_vals(row)
                if v1:
                    _, stats[0]['fta'] = parse_stat_val(v1)
                    _, stats[1]['fta'] = parse_stat_val(v2)
            elif 'turnovers' in txt:
                v1, v2 = get_vals(row)
                if v1:
                    stats[0]['to'], _ = parse_stat_val(v1)
                    stats[1]['to'], _ = parse_stat_val(v2)
            elif 'offensive rebounds' in txt:
                v1, v2 = get_vals(row)
                if v1:
                    stats[0]['orb'], _ = parse_stat_val(v1)
                    stats[1]['orb'], _ = parse_stat_val(v2)
            elif 'defensive rebounds' in txt:
                v1, v2 = get_vals(row)
                if v1:
                    stats[0]['drb'], _ = parse_stat_val(v1)
                    stats[1]['drb'], _ = parse_stat_val(v2)
        
        if not found_stats: return None, None
        
        # Calculate Factors
        away_factors = calculate_four_factors(stats[0], stats[1])
        home_factors = calculate_four_factors(stats[1], stats[0])
        
        return away_factors, home_factors

    except: return None, None

# --- MAIN SCHEDULE LOOP ---
def get_master_team_list():
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?limit=400"
    try:
        resp = requests.get(url, timeout=15)
        data = resp.json()
        teams = {}
        for t in data['sports'][0]['leagues'][0]['teams']:
            teams[t['team']['displayName']] = t['team']['id']
        return teams
    except: return {}

def scrape_team_schedule(team_name, tid, session):
    url = f"https://www.espn.com/mens-college-basketball/team/schedule/_/id/{tid}/season/{SEASON_YEAR}"
    try:
        r = session.get(url, timeout=10)
        soup = BeautifulSoup(r.content, 'html.parser')
        rows = soup.find_all('tr', class_='Table__TR')
        games = []
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 3: continue
            
            # Check for Link (Game ID)
            result_cell = cells[2]
            link_tag = result_cell.find('a', href=True)
            if not link_tag: continue
            
            game_url = link_tag['href']
            # Extract ID
            match_id = re.search(r'gameId[=/]?(\d+)', game_url)
            if not match_id: continue
            game_id = match_id.group(1)
            
            # Basic Info
            date_str = cells[0].get_text(strip=True)
            opp_text = cells[1].get_text(strip=True)
            opp_name = re.sub(r'^\d+\s*', '', opp_text).replace('@', '').replace('vs', '').strip()
            
            # Parse Score
            result_text = result_cell.get_text(strip=True)
            match_score = re.search(r'([WL])\s*(\d+)-(\d+)', result_text)
            if not match_score: continue
            
            res = match_score.group(1)
            s1 = int(match_score.group(2))
            s2 = int(match_score.group(3))
            
            if res == 'W':
                tm_score, opp_score = max(s1, s2), min(s1, s2)
            else:
                tm_score, opp_score = min(s1, s2), max(s1, s2)

            # --- THE MAGIC: GET REAL STATS NOW ---
            # We skip this step if we want speed, but you want DATA.
            # We call the box scraper helper.
            # But wait: Box scraper returns [Away, Home]. 
            # We need to know if 'team_name' was Away or Home.
            # Schedule page usually says "@ Opponent" (Away) or "vs Opponent" (Home)
            
            is_away = '@' in cells[1].get_text()
            
            # We can lazily fetch the stats. 
            # To avoid scraping the same game twice (once for Team A, once for Team B),
            # we could cache it. But for simplicity, we'll just fetch.
            
            # Optimization: ONLY fetch if we don't have it? 
            # Let's just return the metadata and fetch stats in the main loop to handle duplicates.
            
            games.append({
                'GameID': game_id,
                'Date': date_str,
                'Team': team_name,
                'Opponent': opp_name,
                'Location': 'A' if is_away else 'H',
                'TeamScore': tm_score,
                'OpponentScore': opp_score
            })
            
        return games
    except: return []

def main():
    logger.info("🚀 Starting 01_master_scraper_with_stats (The Fix)...")
    
    session = requests.Session()
    session.headers.update(HEADERS)
    
    teams = get_master_team_list()
    logger.info(f"📋 Found {len(teams)} teams. Scanning schedules...")
    
    # 1. Get List of ALL Games (Metadata Only)
    all_game_meta = []
    processed_games = set() # Track GameIDs to avoid double-scraping
    
    count = 0
    for name, tid in teams.items():
        count += 1
        if count % 50 == 0: logger.info(f"Scanning schedules: {count}/{len(teams)} teams...")
        
        team_games = scrape_team_schedule(name, tid, session)
        for g in team_games:
            # We only want to process each GameID once to save time
            # But we need records for BOTH teams in the final file.
            # So we will store unique GameIDs to scrape, then generate both rows.
            if g['GameID'] not in processed_games:
                processed_games.add(g['GameID'])
                all_game_meta.append(g)
        
        # Polite delay
        time.sleep(0.1)

    logger.info(f"✅ Found {len(processed_games)} unique games. Now fetching stats...")
    
    # 2. Fetch Stats for Unique Games
    final_results = []
    
    for i, g in enumerate(all_game_meta):
        if i % 20 == 0: 
            logger.info(f"[{i+1}/{len(all_game_meta)}] Scraping box score: {g['Team']} vs {g['Opponent']}")
            # Autosave
            if final_results:
                pd.DataFrame(final_results).to_csv(OUTPUT_FILE, index=False)
        
        stats_away, stats_home = get_real_stats(g['GameID'], session)
        
        if stats_away and stats_home:
            # We need to map these back to the perspective of the teams.
            # The 'g' record tells us who 'Team' is and if they were Away/Home.
            
            # If g['Location'] == 'A', then g['Team'] is Away.
            # So g['Team'] gets stats_away.
            
            # We create TWO rows for the final file: one for Team, one for Opponent.
            
            # Row 1: The 'Team' from our meta list
            row1 = g.copy()
            # If Team was Away
            if g['Location'] == 'A':
                my_s = stats_away
                opp_s = stats_home
            else: # Team was Home
                my_s = stats_home
                opp_s = stats_away
                
            row1.update(my_s) # Add Poss, eFG, etc.
            final_results.append(row1)
            
            # Row 2: The 'Opponent'
            row2 = {
                'GameID': g['GameID'],
                'Date': g['Date'],
                'Team': g['Opponent'],
                'Opponent': g['Team'],
                'Location': 'H' if g['Location'] == 'A' else 'A',
                'TeamScore': g['OpponentScore'],
                'OpponentScore': g['TeamScore']
            }
            row2.update(opp_s)
            final_results.append(row2)
            
    # Final Save
    if final_results:
        df = pd.DataFrame(final_results)
        df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"✅ DONE! Saved {len(df)} rows with REAL STATS to {OUTPUT_FILE}")
    else:
        logger.error("❌ No stats scraped.")

if __name__ == "__main__":
    main()
