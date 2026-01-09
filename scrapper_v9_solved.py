"""
06_espn_box_scraper_v9_SOLVED.py
=========================================
THE BIBLE - PHASE 2: BOX SCORE ENGINE (SOLVED)
=========================================
FIX:
  - Updated parser to handle ESPN's verbose stat labels:
    "fieldGoalsMade-fieldGoalsAttempted" instead of "fieldGoals"
"""

import requests
import pandas as pd
import time
import os
import logging
import re

# ==============================================================================
# CONFIGURATION
# ==============================================================================
SEASON_YEAR = 2026
OUTPUT_FILE = "master_box_scores_2026.csv"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
REQUEST_DELAY = 0.05

# Logger Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_d1_teams():
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?limit=400"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        teams = resp.json()['sports'][0]['leagues'][0]['teams']
        return {t['team']['displayName']: t['team']['id'] for t in teams}
    except:
        return {}

def get_completed_games(team_id):
    url = f"http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{team_id}/schedule?season={SEASON_YEAR}"
    valid_ids = set()
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        data = resp.json()
        if 'events' in data:
            for event in data['events']:
                try:
                    if event['competitions'][0]['status']['type']['name'] == 'STATUS_FINAL':
                        valid_ids.add(event['id'])
                except:
                    continue
    except:
        pass
    return valid_ids

def fetch_box_score(game_id):
    url = f"http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={game_id}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        return resp.json() if resp.status_code == 200 else None
    except:
        return None

def parse_stats_v9(statistics):
    """
    Updated to handle the long keys found in the diagnostic scan.
    """
    stats = {}
    
    for item in statistics:
        label = item['name']
        val = item['displayValue']
        
        # --- 1. Handle "Made-Att" (e.g. 25-60) ---
        if '-' in val: 
            parts = val.split('-')
            try:
                made, att = int(parts[0]), int(parts[1])
                
                # CHECK FOR THE LONG KEYS FOUND IN DIAGNOSTIC
                if 'fieldGoalsMade-fieldGoalsAttempted' in label: 
                    stats['FGM'], stats['FGA'] = made, att
                elif 'threePointFieldGoalsMade-threePointFieldGoalsAttempted' in label:
                    stats['3PM'], stats['3PA'] = made, att
                elif 'freeThrowsMade-freeThrowsAttempted' in label:
                    stats['FTM'], stats['FTA'] = made, att
            except:
                continue
                
        # --- 2. Handle Single Numbers ---
        else:
            try:
                num = int(val)
                if label in ['rebounds', 'totalRebounds']: stats['REB'] = num
                elif label in ['offensiveRebounds']: stats['OR'] = num
                elif label in ['defensiveRebounds']: stats['DR'] = num
                elif label in ['turnovers']: stats['TO'] = num
                elif label in ['fouls']: stats['PF'] = num
            except:
                continue
    return stats

def calculate_factors(t_stats, o_stats):
    if not t_stats or 'FGA' not in t_stats or t_stats['FGA'] == 0: return None
    
    poss = t_stats.get('FGA',0) - t_stats.get('OR',0) + t_stats.get('TO',0) + (0.475 * t_stats.get('FTA',0))
    if poss <= 0: poss = 1
    
    efg = (t_stats.get('FGM',0) + 0.5 * t_stats.get('3PM',0)) / t_stats.get('FGA',1)
    to_rate = t_stats.get('TO',0) / poss
    
    or_chances = t_stats.get('OR',0) + o_stats.get('DR',0)
    or_rate = t_stats.get('OR',0) / or_chances if or_chances > 0 else 0
        
    ftr = t_stats.get('FTA',0) / t_stats.get('FGA',1)
    
    return {
        'Possessions': round(poss, 1),
        'eFG%': round(efg * 100, 1),
        'TO%': round(to_rate * 100, 1),
        'OR%': round(or_rate * 100, 1),
        'FTR': round(ftr * 100, 1)
    }

# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print("\n--- RUNNING VERSION 9 (SOLVED) ---\n")
    
    logger.info("1. Fetching Teams...")
    teams = get_d1_teams()
    logger.info(f"   Found {len(teams)} teams.")
    
    logger.info("2. Scanning Schedules (Only Completed Games)...")
    all_game_ids = set()
    
    # Check if we already have a CSV to save time
    existing_ids = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            df = pd.read_csv(OUTPUT_FILE)
            existing_ids = set(df['GameID'].astype(str))
            logger.info(f"   Resuming... skipping {len(existing_ids)} games already saved.")
        except: pass

    # Scan teams
    for i, (name, tid) in enumerate(teams.items()):
        if i % 20 == 0: print(f"   Scanning {i}/{len(teams)}: {name}...")
        gids = get_completed_games(tid)
        all_game_ids.update(gids)
        time.sleep(0.01)
        
    logger.info(f"   Total Completed Games Found: {len(all_game_ids)}")
    
    # Remove duplicates
    games_to_scrape = list(all_game_ids - existing_ids)
    logger.info(f"   Starting Scrape on {len(games_to_scrape)} NEW games...")
    
    new_rows = []
    success_count = 0
    
    for i, gid in enumerate(games_to_scrape):
        if i % 25 == 0:
            print(f"   Processing {i}/{len(games_to_scrape)} | Success: {success_count}")
            
        data = fetch_box_score(gid)
        if not data or 'boxscore' not in data or 'teams' not in data['boxscore']: continue
        
        try:
            t1_info = data['boxscore']['teams'][0]
            t2_info = data['boxscore']['teams'][1]
            
            # USE NEW PARSER v9
            t1_stats = parse_stats_v9(t1_info['statistics'])
            t2_stats = parse_stats_v9(t2_info['statistics'])
            
            if not t1_stats or not t2_stats: continue
            
            t1_factors = calculate_factors(t1_stats, t2_stats)
            t2_factors = calculate_factors(t2_stats, t1_stats)
            
            if not t1_factors or not t2_factors: continue
            
            # Metadata
            date = data['header']['competitions'][0]['date'][:10]
            neutral = data['header']['competitions'][0]['neutralSite']
            competitors = data['header']['competitions'][0]['competitors']
            home_id = next((c['id'] for c in competitors if c['homeAway'] == 'home'), None)
            
            if t1_info['team']['id'] == home_id: loc1 = "Home"; loc2 = "Away"
            else: loc1 = "Away"; loc2 = "Home"
            if neutral: loc1 = "Neutral"; loc2 = "Neutral"
            
            new_rows.append({'GameID': gid, 'Date': date, 'Team': t1_info['team']['displayName'], 'Opponent': t2_info['team']['displayName'], 'Location': loc1, **t1_factors})
            new_rows.append({'GameID': gid, 'Date': date, 'Team': t2_info['team']['displayName'], 'Opponent': t1_info['team']['displayName'], 'Location': loc2, **t2_factors})
            
            success_count += 1
            
            # SAFETY SAVE
            if len(new_rows) >= 100:
                df = pd.DataFrame(new_rows)
                if os.path.exists(OUTPUT_FILE): df.to_csv(OUTPUT_FILE, mode='a', header=False, index=False)
                else: df.to_csv(OUTPUT_FILE, mode='w', header=True, index=False)
                print(f"   [SAVED] {len(new_rows)} rows safely to disk.")
                new_rows = []
                
        except Exception as e:
            continue
            
        time.sleep(REQUEST_DELAY)

    # FINAL SAVE
    if len(new_rows) > 0:
        df = pd.DataFrame(new_rows)
        if os.path.exists(OUTPUT_FILE): df.to_csv(OUTPUT_FILE, mode='a', header=False, index=False)
        else: df.to_csv(OUTPUT_FILE, mode='w', header=True, index=False)
        print("   [FINAL SAVE] Process Complete.")

if __name__ == "__main__":
    main()
