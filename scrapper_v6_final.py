"""
06_espn_box_scraper_v6.py
=========================================
THE BIBLE - PHASE 2: BOX SCORE ENGINE (VERSION 6)
=========================================
FIXES:
  - Combines v5's perfect schedule finder (3,000+ games)
  - Restores v3's flexible stat parsing (fixes "Success: 0" bug)
  - Adds error printing for the first failure so we know why.
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

def fetch_box_score(game_id):
    """Fetches the game summary."""
    url = f"http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={game_id}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return None

def parse_stats(statistics):
    """
    Extracts stats flexibly (Handles 'fieldGoals' AND 'fieldGoalsMade').
    """
    stats = {}
    for item in statistics:
        label = item['name']
        val = item['displayValue']
        
        # Check for 'Made-Att' format (e.g., "25-60")
        if '-' in val: 
            parts = val.split('-')
            try:
                made, att = int(parts[0]), int(parts[1])
                
                # CHECK MULTIPLE NAMES FOR SAFETY
                if label in ['fieldGoals', 'fieldGoalsMade']: 
                    stats['FGM'], stats['FGA'] = made, att
                elif label in ['threePointFieldGoals', 'threePointFieldGoalsMade']:
                    stats['3PM'], stats['3PA'] = made, att
                elif label in ['freeThrows', 'freeThrowsMade']:
                    stats['FTM'], stats['FTA'] = made, att
            except:
                continue
        else: # Direct numbers
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
    # Strict check: If we missed FGA, we can't do anything.
    if 'FGA' not in t_stats or t_stats['FGA'] == 0:
        return None
    
    # 1. Possessions
    poss = t_stats.get('FGA',0) - t_stats.get('OR',0) + t_stats.get('TO',0) + (0.475 * t_stats.get('FTA',0))
    if poss <= 0: poss = 1
    
    # 2. eFG%
    efg = (t_stats.get('FGM',0) + 0.5 * t_stats.get('3PM',0)) / t_stats.get('FGA',1)
    
    # 3. TO%
    to_rate = t_stats.get('TO',0) / poss
    
    # 4. OR%
    or_chances = t_stats.get('OR',0) + o_stats.get('DR',0)
    or_rate = 0
    if or_chances > 0:
        or_rate = t_stats.get('OR',0) / or_chances
        
    # 5. FTR
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
    print("\n--- RUNNING VERSION 6 (FINAL FIX) ---\n")
    logger.info("1. Fetching Teams...")
    teams = get_d1_teams()
    logger.info(f"   Found {len(teams)} teams.")
    
    logger.info("2. Scanning Schedules (Only Completed Games)...")
    all_game_ids = set()
    
    # Scan teams
    for i, (name, tid) in enumerate(teams.items()):
        if i % 20 == 0:
            print(f"   Scanning {i}/{len(teams)}: {name}...")
        gids = get_completed_games(tid)
        all_game_ids.update(gids)
        time.sleep(0.01)
        
    logger.info(f"   Total Completed Games Found: {len(all_game_ids)}")
    
    # Check against existing file
    games_to_scrape = list(all_game_ids)
    if os.path.exists(OUTPUT_FILE):
        try:
            df_exist = pd.read_csv(OUTPUT_FILE)
            existing_ids = set(df_exist['GameID'].astype(str))
            games_to_scrape = list(all_game_ids - existing_ids)
            logger.info(f"   Skipping {len(existing_ids)} already scraped.")
        except:
            pass
        
    logger.info(f"3. Starting Scrape on {len(games_to_scrape)} games...")
    
    new_rows = []
    success_count = 0
    first_fail_printed = False
    
    for i, gid in enumerate(games_to_scrape):
        if i % 10 == 0:
            print(f"   Processing {i}/{len(games_to_scrape)} | Success: {success_count}")
            
        data = fetch_box_score(gid)
        
        # Basic validation
        if not data or 'boxscore' not in data: continue
        if 'teams' not in data['boxscore']: continue
        
        try:
            t1_info = data['boxscore']['teams'][0]
            t2_info = data['boxscore']['teams'][1]
            
            t1_stats = parse_stats(t1_info['statistics'])
            t2_stats = parse_stats(t2_info['statistics'])
            
            # DEBUG: If parsing failed, print WHY for the first one
            if (not t1_stats or not t2_stats) and not first_fail_printed:
                print(f"   ⚠️ DEBUG FAIL Game {gid}: Stats Empty. Found Keys: {[x['name'] for x in t1_info['statistics']]}")
                first_fail_printed = True
                continue

            if not t1_stats or not t2_stats: continue
            
            t1_factors = calculate_factors(t1_stats, t2_stats)
            t2_factors = calculate_factors(t2_stats, t1_stats)
            
            if not t1_factors or not t2_factors: continue
            
            # Metadata
            date = data['header']['competitions'][0]['date'][:10]
            neutral = data['header']['competitions'][0]['neutralSite']
            
            # Determine Home/Away
            competitors = data['header']['competitions'][0]['competitors']
            home_id = next((c['id'] for c in competitors if c['homeAway'] == 'home'), None)
            
            if t1_info['team']['id'] == home_id:
                loc1, loc2 = "Home", "Away"
            else:
                loc1, loc2 = "Away", "Home"
                
            if neutral:
                loc1, loc2 = "Neutral", "Neutral"
                
            # Add to list
            new_rows.append({
                'GameID': gid, 'Date': date, 
                'Team': t1_info['team']['displayName'], 
                'Opponent': t2_info['team']['displayName'],
                'Location': loc1, **t1_factors
            })
            new_rows.append({
                'GameID': gid, 'Date': date, 
                'Team': t2_info['team']['displayName'], 
                'Opponent': t1_info['team']['displayName'],
                'Location': loc2, **t2_factors
            })
            
            success_count += 1
            
            # SAFETY SAVE (Every 50 games)
            if len(new_rows) >= 100:
                df = pd.DataFrame(new_rows)
                if os.path.exists(OUTPUT_FILE):
                    df.to_csv(OUTPUT_FILE, mode='a', header=False, index=False)
                else:
                    df.to_csv(OUTPUT_FILE, mode='w', header=True, index=False)
                print(f"   [SAVED] {len(new_rows)} rows safely to disk.")
                new_rows = []
                
        except Exception as e:
            continue
            
    # FINAL SAVE
    if len(new_rows) > 0:
        df = pd.DataFrame(new_rows)
        if os.path.exists(OUTPUT_FILE):
            df.to_csv(OUTPUT_FILE, mode='a', header=False, index=False)
        else:
            df.to_csv(OUTPUT_FILE, mode='w', header=True, index=False)
        print("   [FINAL SAVE] Process Complete.")

if __name__ == "__main__":
    main()
