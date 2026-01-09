"""
06_espn_box_scraper_v8_diagnostic.py
=========================================
THE BIBLE - PHASE 2: BOX SCORE ENGINE (X-RAY DIAGNOSTIC)
=========================================
PURPOSE:
  - This script is designed to FAIL LOUDLY.
  - It will print the exact JSON keys for the first game that fails.
  - Use this to identify why 'Field Goals' are not being detected.
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
REQUEST_DELAY = 0.2

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

def parse_stats_debug(statistics, game_id):
    """
    Tries to parse stats, but logs ALL available keys if FGA is missing.
    """
    stats = {}
    found_labels = [] # Keep track of what we see
    
    for item in statistics:
        label = item['name']
        val = item['displayValue']
        found_labels.append(f"{label}: {val}")
        
        if '-' in val: 
            parts = val.split('-')
            try:
                made, att = int(parts[0]), int(parts[1])
                if label in ['fieldGoals', 'fieldGoalsMade']: 
                    stats['FGM'], stats['FGA'] = made, att
                elif label in ['threePointFieldGoals', 'threePointFieldGoalsMade']:
                    stats['3PM'], stats['3PA'] = made, att
                elif label in ['freeThrows', 'freeThrowsMade']:
                    stats['FTM'], stats['FTA'] = made, att
            except:
                continue
        else:
            try:
                num = int(val)
                if label in ['rebounds', 'totalRebounds']: stats['REB'] = num
                elif label in ['offensiveRebounds']: stats['OR'] = num
                elif label in ['turnovers']: stats['TO'] = num
            except:
                continue
                
    # DEBUG CHECK
    if 'FGA' not in stats:
        print(f"\n🔴 DEBUG: REJECTED GAME {game_id}")
        print(f"   REASON: Could not find 'fieldGoals' in these labels:")
        print(f"   FOUND LABELS: {found_labels[:10]} ...") # Print first 10 stats found
        return None
        
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
    print("\n--- RUNNING VERSION 8 (X-RAY DIAGNOSTIC) ---\n")
    
    logger.info("1. Fetching Teams...")
    teams = get_d1_teams()
    logger.info(f"   Found {len(teams)} teams.")
    
    logger.info("2. Scanning Schedules (Only Completed Games)...")
    all_game_ids = set()
    
    # SCAN ONLY FIRST 20 TEAMS TO SAVE TIME FOR DIAGNOSTIC
    scan_limit = 0
    for name, tid in teams.items():
        if scan_limit > 20: break 
        gids = get_completed_games(tid)
        all_game_ids.update(gids)
        scan_limit += 1
        
    logger.info(f"   Found {len(all_game_ids)} games from sample scan.")
    games_to_scrape = list(all_game_ids)
    
    print("\n--- STARTING DIAGNOSTIC SCRAPE ---")
    print("If this fails, copy the 'FOUND LABELS' list and send it to me.\n")
    
    new_rows = []
    success_count = 0
    
    for i, gid in enumerate(games_to_scrape):
        if i % 10 == 0: print(f"   Processing {i}/{len(games_to_scrape)}...")
            
        data = fetch_box_score(gid)
        if not data or 'boxscore' not in data or 'teams' not in data['boxscore']: continue
        
        try:
            t1_info = data['boxscore']['teams'][0]
            t2_info = data['boxscore']['teams'][1]
            
            # THIS IS WHERE WE CHECK FOR THE ERROR
            t1_stats = parse_stats_debug(t1_info['statistics'], gid)
            t2_stats = parse_stats_debug(t2_info['statistics'], gid)
            
            if not t1_stats or not t2_stats:
                # We found the error! Stop script so user can read it.
                print("\n🛑 STOPPING SCRIPT EARLY FOR DIAGNOSIS.")
                return 

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
            print(f"   ✅ SUCCESS: Processed Game {gid}")
            
        except Exception as e:
            print(f"Error: {e}")
            continue
            
        time.sleep(REQUEST_DELAY)

if __name__ == "__main__":
    main()
