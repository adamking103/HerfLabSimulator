"""
06_espn_box_scraper_v7.py
=========================================
THE BIBLE - PHASE 2: BOX SCORE ENGINE (LOUD DEBUG VERSION)
=========================================
Changes:
  - REMOVED "silent" error handling.
  - Will print API Status Codes (200, 403, 404, etc.)
  - Will print which specific Data Keys are missing.
  - Slower speed (0.2s) to prevent being blocked.
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
REQUEST_DELAY = 0.2  # SLOWED DOWN to prevent blocking

# Logger Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_d1_teams():
    """Fetches list of all D1 teams."""
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?limit=400"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        teams = resp.json()['sports'][0]['leagues'][0]['teams']
        return {t['team']['displayName']: t['team']['id'] for t in teams}
    except Exception as e:
        logger.error(f"Error fetching teams: {e}")
        return {}

def get_completed_games(team_id):
    """Fetches ONLY games that are marked 'STATUS_FINAL'."""
    url = f"http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{team_id}/schedule?season={SEASON_YEAR}"
    valid_ids = set()
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        data = resp.json()
        if 'events' in data:
            for event in data['events']:
                try:
                    status = event['competitions'][0]['status']['type']['name']
                    if status == 'STATUS_FINAL':
                        valid_ids.add(event['id'])
                except:
                    continue
    except:
        pass
    return valid_ids

def fetch_box_score_debug(game_id):
    """Fetches the game summary with FULL DEBUGGING."""
    url = f"http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={game_id}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        # Return the code AND the data so we can see what's wrong
        if resp.status_code == 200:
            return resp.status_code, resp.json()
        else:
            return resp.status_code, None
    except Exception as e:
        return f"ERROR: {str(e)}", None

def parse_stats(statistics):
    """Extracts stats flexibly."""
    stats = {}
    for item in statistics:
        label = item['name']
        val = item['displayValue']
        
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
                elif label in ['defensiveRebounds']: stats['DR'] = num
                elif label in ['turnovers']: stats['TO'] = num
                elif label in ['fouls']: stats['PF'] = num
            except:
                continue
    return stats

def calculate_factors(t_stats, o_stats):
    """Calculates the Four Factors."""
    if 'FGA' not in t_stats or t_stats['FGA'] == 0:
        return None
    
    poss = t_stats.get('FGA',0) - t_stats.get('OR',0) + t_stats.get('TO',0) + (0.475 * t_stats.get('FTA',0))
    if poss <= 0: poss = 1
    
    efg = (t_stats.get('FGM',0) + 0.5 * t_stats.get('3PM',0)) / t_stats.get('FGA',1)
    to_rate = t_stats.get('TO',0) / poss
    
    or_chances = t_stats.get('OR',0) + o_stats.get('DR',0)
    or_rate = 0
    if or_chances > 0:
        or_rate = t_stats.get('OR',0) / or_chances
        
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
    print("\n--- RUNNING VERSION 7 (LOUD DEBUG MODE) ---\n")
    logger.info("1. Fetching Teams...")
    teams = get_d1_teams()
    logger.info(f"   Found {len(teams)} teams.")
    
    logger.info("2. Scanning Schedules...")
    all_game_ids = set()
    
    for i, (name, tid) in enumerate(teams.items()):
        if i % 20 == 0: print(f"   Scanning {i}/{len(teams)}: {name}...")
        gids = get_completed_games(tid)
        all_game_ids.update(gids)
        time.sleep(0.01)
        
    logger.info(f"   Total Completed Games Found: {len(all_game_ids)}")
    
    games_to_scrape = list(all_game_ids)
    
    logger.info(f"3. Starting Scrape on {len(games_to_scrape)} games...")
    
    new_rows = []
    success_count = 0
    fail_print_count = 0 # Limit debug prints
    
    for i, gid in enumerate(games_to_scrape):
        if i % 10 == 0:
            print(f"   Processing {i}/{len(games_to_scrape)} | Success: {success_count}")
            
        # --- DEBUG FETCH ---
        status, data = fetch_box_score_debug(gid)
        
        # 1. CHECK STATUS CODE
        if status != 200:
            if fail_print_count < 5:
                print(f"   [DEBUG] Game {gid} FAILED. Status Code: {status}")
                fail_print_count += 1
            continue
            
        # 2. CHECK DATA KEYS
        if not data or 'boxscore' not in data:
            if fail_print_count < 5:
                print(f"   [DEBUG] Game {gid} FAILED. Missing 'boxscore' key. Keys found: {list(data.keys()) if data else 'None'}")
                fail_print_count += 1
            continue
            
        if 'teams' not in data['boxscore']:
            if fail_print_count < 5:
                print(f"   [DEBUG] Game {gid} FAILED. Missing 'teams' in boxscore.")
                fail_print_count += 1
            continue
        
        try:
            t1_info = data['boxscore']['teams'][0]
            t2_info = data['boxscore']['teams'][1]
            
            t1_stats = parse_stats(t1_info['statistics'])
            t2_stats = parse_stats(t2_info['statistics'])
            
            if not t1_stats or not t2_stats:
                if fail_print_count < 5:
                    print(f"   [DEBUG] Game {gid} FAILED. Stats empty after parsing.")
                    fail_print_count += 1
                continue
            
            t1_factors = calculate_factors(t1_stats, t2_stats)
            t2_factors = calculate_factors(t2_stats, t1_stats)
            
            if not t1_factors or not t2_factors: continue
            
            # Metadata
            date = data['header']['competitions'][0]['date'][:10]
            neutral = data['header']['competitions'][0]['neutralSite']
            competitors = data['header']['competitions'][0]['competitors']
            home_id = next((c['id'] for c in competitors if c['homeAway'] == 'home'), None)
            
            if t1_info['team']['id'] == home_id:
                loc1, loc2 = "Home", "Away"
            else:
                loc1, loc2 = "Away", "Home"
                
            if neutral: loc1, loc2 = "Neutral", "Neutral"
            
            # Add to list
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
            if fail_print_count < 5:
                print(f"   [DEBUG] Game {gid} EXCEPTION: {e}")
                fail_print_count += 1
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
