"""
06_espn_scraper_full.py
=======================
THE BIBLE - Single Step Restore
1. Finds all games from Nov 2025 to Today (API).
2. Immediately scrapes Scores & Efficiency for each game.
3. Saves directly to 'master_box_scores_2026.csv'.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
import logging
from datetime import date, timedelta

# --- CONFIGURATION ---
OUTPUT_FILE = "master_box_scores_2026.csv"
START_DATE = date(2025, 11, 1)  # Start of Season
END_DATE = date.today()         # Today
# ---------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# --- PART 1: FIND THE GAMES (API) ---
def get_games_for_date(date_obj):
    """Uses ESPN API to get Game IDs and Team Names for a specific date."""
    date_str = date_obj.strftime("%Y%m%d")
    url = f"http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&groups=50&limit=1000"
    
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        games = []
        
        for event in data.get('events', []):
            game_id = event['id']
            status = event['status']['type']['state']
            
            # Skip games that haven't finished ('post' means finished)
            if status != 'post': 
                continue

            comps = event['competitions'][0]['competitors']
            
            # Identify Home vs Away
            # comps list order varies, so check the 'homeAway' key
            home_team = next((c['team']['displayName'] for c in comps if c['homeAway'] == 'home'), "Unknown")
            away_team = next((c['team']['displayName'] for c in comps if c['homeAway'] == 'away'), "Unknown")
            
            games.append({
                'GameID': game_id,
                'Date': date_obj.strftime("%Y-%m-%d"),
                'AwayTeam': away_team,
                'HomeTeam': home_team
            })
        return games
    except Exception:
        return []

# --- PART 2: SCRAPE THE STATS (HTML) ---
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

def get_box_score(game_id):
    """Visits the matchup page and extracts Team Comparison stats."""
    url = f"https://www.espn.com/mens-college-basketball/matchup?gameId={game_id}"
    try:
        # Polite delay
        time.sleep(random.uniform(0.3, 0.8)) 
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')

        # 1. Get Scores
        scores = soup.find_all('div', class_='Gamestrip__Score')
        if not scores: scores = soup.find_all(class_='score')
        if len(scores) < 2: return None
        
        score_away = int(scores[0].text.strip())
        score_home = int(scores[1].text.strip())

        # 2. Get Stats Table
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
            elif 'turnovers' in txt and 'points' not in txt:
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

        # 3. Calculate
        stats_away = calculate_four_factors(s_away, s_home)
        stats_home = calculate_four_factors(s_home, s_away)
        
        stats_away['Score'] = score_away
        stats_home['Score'] = score_home
        
        return stats_away, stats_home

    except Exception:
        return None

# --- PART 3: MAIN LOOP ---
def main():
    logger.info("🚀 Starting Full Restoration (Schedule + Stats)...")
    
    all_results = []
    
    # 1. Iterate Dates
    delta = END_DATE - START_DATE
    dates = [START_DATE + timedelta(days=i) for i in range(delta.days + 1)]
    
    total_games_found = 0
    
    for i, day in enumerate(dates):
        # 2. Get Games for this day
        games = get_games_for_date(day)
        if not games: continue
        
        logger.info(f"📅 {day}: Processing {len(games)} games...")
        
        # 3. Scrape Stats for each game
        for g in games:
            stats = get_box_score(g['GameID'])
            if stats:
                stats_away, stats_home = stats
                
                # Record 1: The Away Team
                all_results.append({
                    'Date': g['Date'],
                    'Team': g['AwayTeam'],
                    'Opponent': g['HomeTeam'],
                    'Location': 'N', # Neutral/Away simplified
                    'TeamScore': stats_away.get('Score'),
                    'OpponentScore': stats_home.get('Score'),
                    **stats_away
                })
                
                # Record 2: The Home Team
                all_results.append({
                    'Date': g['Date'],
                    'Team': g['HomeTeam'],
                    'Opponent': g['AwayTeam'],
                    'Location': 'H',
                    'TeamScore': stats_home.get('Score'),
                    'OpponentScore': stats_away.get('Score'),
                    **stats_home
                })
                
        # Autosave every day
        total_games_found += len(games)
        pd.DataFrame(all_results).to_csv(OUTPUT_FILE, index=False)
        
    logger.info(f"✅ RESTORE COMPLETE! Saved {len(all_results)} records.")

if __name__ == "__main__":
    main()
