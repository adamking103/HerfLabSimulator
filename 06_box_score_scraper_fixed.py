"""
06_espn_box_scraper_ROBUST.py
=========================================
THE BIBLE - PHASE 2: BOX SCORE ENGINE (ROBUST VERSION)
=========================================
FIXES:
  - Uses ESPN Scoreboard API instead of HTML regex scraping
  - Only processes completed games (has final score)
  - Better error handling and diagnostics
  - Shows you which teams/games are being processed
"""

import requests
import pandas as pd
import time
import os
import logging
from datetime import datetime, timedelta

# ==============================================================================
# CONFIGURATION
# ==============================================================================
SEASON_YEAR = 2026
OUTPUT_FILE = "master_box_scores_2026.csv"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
REQUEST_DELAY = 0.15

# Logger Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# ==============================================================================
# IMPROVED FUNCTIONS
# ==============================================================================

def get_d1_teams():
    """Fetches dictionary of {TeamName: ESPN_ID} for all D1 schools."""
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?limit=400"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        teams = resp.json()['sports'][0]['leagues'][0]['teams']
        team_dict = {t['team']['displayName']: t['team']['id'] for t in teams}
        logger.info(f"✅ Loaded {len(team_dict)} D1 teams")
        return team_dict
    except Exception as e:
        logger.error(f"❌ Failed to fetch team list: {e}")
        return {}

def get_completed_games_for_team(team_id, season=SEASON_YEAR):
    """
    Uses ESPN's official API to get COMPLETED games for a team.
    This is much more reliable than regex scraping HTML.
    """
    # ESPN Team Schedule API
    url = f"http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{team_id}/schedule?season={season}"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return set()
        
        data = resp.json()
        
        if 'events' not in data:
            return set()
        
        completed_game_ids = set()
        
        for event in data['events']:
            # Check if game is completed
            if 'competitions' not in event:
                continue
            
            comp = event['competitions'][0]
            
            # Must have a status indicating completion
            if 'status' in comp and 'type' in comp['status']:
                status_type = comp['status']['type']['name']
                
                # Only include completed games
                if status_type in ['STATUS_FINAL', 'Final']:
                    game_id = event['id']
                    completed_game_ids.add(game_id)
        
        return completed_game_ids
        
    except Exception as e:
        logger.warning(f"⚠️ Could not fetch schedule for team {team_id}: {e}")
        return set()

def fetch_game_summary(game_id):
    """Hits the ESPN Summary API to get box score stats."""
    url = f"http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={game_id}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.debug(f"Failed to fetch game {game_id}: {e}")
    return None

def parse_stat_group(statistics):
    """Parses the 'statistics' list from ESPN API."""
    stats = {}
    for item in statistics:
        label = item['name']
        val = item['displayValue']
        
        # --- Handle "Made-Att" format (e.g. "25-60") ---
        if '-' in val:
            parts = val.split('-')
            try:
                made = int(parts[0])
                att = int(parts[1])
                
                if label in ['fieldGoals', 'fieldGoalsMade']: 
                    stats['FGM'], stats['FGA'] = made, att
                elif label in ['threePointFieldGoals', 'threePointFieldGoalsMade']:
                    stats['3PM'], stats['3PA'] = made, att
                elif label in ['freeThrows', 'freeThrowsMade']:
                    stats['FTM'], stats['FTA'] = made, att
            except:
                continue
        else:
            # --- Handle direct numbers ---
            try:
                num = int(val)
                if label in ['totalRebounds', 'rebounds']: stats['REB'] = num
                elif label in ['offensiveRebounds']: stats['OR'] = num
                elif label in ['defensiveRebounds']: stats['DR'] = num
                elif label in ['turnovers']: stats['TO'] = num
                elif label in ['fouls']: stats['PF'] = num
            except:
                continue
    return stats

def calculate_four_factors(team_stats, opp_stats):
    """Calculates eFG%, TO%, OR%, FTR."""
    # Validation
    if 'FGA' not in team_stats or team_stats['FGA'] == 0:
        return None

    fga = team_stats.get('FGA', 0)
    or_val = team_stats.get('OR', 0)
    to_val = team_stats.get('TO', 0)
    fta = team_stats.get('FTA', 0)
    
    poss = fga - or_val + to_val + (0.475 * fta)
    if poss <= 0: poss = 1.0
    
    fgm = team_stats.get('FGM', 0)
    pm3 = team_stats.get('3PM', 0)
    efg = (fgm + 0.5 * pm3) / fga
    
    to_rate = to_val / poss
    
    opp_dr = opp_stats.get('DR', 0)
    or_chances = or_val + opp_dr
    or_rate = or_val / or_chances if or_chances > 0 else 0.0
    
    ft_rate = fta / fga
    
    return {
        'Possessions': round(poss, 1),
        'eFG%': round(efg * 100, 1),
        'TO%': round(to_rate * 100, 1),
        'OR%': round(or_rate * 100, 1),
        'FTR': round(ft_rate * 100, 1),
        'Raw_OR': or_val,
        'Raw_TO': to_val,
        'Raw_FT': fta
    }

# ==============================================================================
# DIAGNOSTIC FUNCTION
# ==============================================================================

def test_single_game(game_id):
    """
    Test function to see what's happening with a specific game.
    Use this to debug why games are failing.
    """
    logger.info(f"\n{'='*70}")
    logger.info(f"🔍 TESTING GAME ID: {game_id}")
    logger.info(f"{'='*70}")
    
    data = fetch_game_summary(game_id)
    
    if not data:
        logger.error("❌ No data returned from API")
        return
    
    # Check what we got back
    logger.info(f"✅ Data retrieved successfully")
    
    # Check for boxscore
    if 'boxscore' not in data:
        logger.error("❌ No 'boxscore' key in response")
        logger.info(f"Available keys: {list(data.keys())}")
        return
    
    if 'teams' not in data['boxscore']:
        logger.error("❌ No 'teams' in boxscore")
        logger.info(f"Boxscore keys: {list(data['boxscore'].keys())}")
        return
    
    # Try to parse
    try:
        team1_info = data['boxscore']['teams'][0]
        team2_info = data['boxscore']['teams'][1]
        
        t1_name = team1_info['team']['displayName']
        t2_name = team2_info['team']['displayName']
        
        logger.info(f"🏀 {t1_name} vs {t2_name}")
        
        # Check for statistics
        if 'statistics' not in team1_info:
            logger.error("❌ No statistics for team 1")
            return
        
        t1_stats = parse_stat_group(team1_info['statistics'])
        t2_stats = parse_stat_group(team2_info['statistics'])
        
        logger.info(f"📊 Team 1 stats: {t1_stats}")
        logger.info(f"📊 Team 2 stats: {t2_stats}")
        
        # Try four factors
        t1_factors = calculate_four_factors(t1_stats, t2_stats)
        t2_factors = calculate_four_factors(t2_stats, t1_stats)
        
        if t1_factors and t2_factors:
            logger.info("✅ Four factors calculated successfully!")
            logger.info(f"   {t1_name}: {t1_factors}")
            logger.info(f"   {t2_name}: {t2_factors}")
        else:
            logger.error("❌ Four factors calculation failed")
            
    except Exception as e:
        logger.error(f"❌ Error parsing game: {e}")
        import traceback
        traceback.print_exc()

# ==============================================================================
# MAIN PIPELINE
# ==============================================================================

def main():
    logger.info("\n" + "="*70)
    logger.info("🏀 ESPN BOX SCORE SCRAPER - ROBUST VERSION")
    logger.info("="*70)
    
    # 1. Load Existing Data
    processed_ids = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            existing = pd.read_csv(OUTPUT_FILE)
            if 'GameID' in existing.columns:
                processed_ids = set(existing['GameID'].astype(str))
                logger.info(f"📚 Loaded {len(processed_ids)} existing games from CSV")
        except Exception as e:
            logger.warning(f"⚠️ Could not read existing file: {e}")

    # 2. Get All Teams
    teams = get_d1_teams()
    if not teams:
        logger.error("❌ Failed to load teams. Exiting.")
        return
    
    # 3. Find Completed Games
    all_completed_games = set()
    logger.info("\n🔍 Scanning for COMPLETED games...")
    
    for i, (name, tid) in enumerate(teams.items()):
        if i % 20 == 0:
            logger.info(f"   Scanning team {i+1}/{len(teams)}: {name}")
        
        completed_games = get_completed_games_for_team(tid)
        all_completed_games.update(completed_games)
        time.sleep(REQUEST_DELAY)
    
    logger.info(f"\n✅ Found {len(all_completed_games)} total completed games")
    
    # Remove already processed
    games_to_scrape = list(all_completed_games - processed_ids)
    logger.info(f"📋 New games to scrape: {len(games_to_scrape)}")
    
    if not games_to_scrape:
        logger.info("✅ Database is up to date!")
        return
    
    # 4. Scrape New Games
    new_rows = []
    successful_games = 0
    failed_games = 0
    
    logger.info(f"\n{'='*70}")
    logger.info(f"🚀 SCRAPING {len(games_to_scrape)} NEW GAMES")
    logger.info(f"{'='*70}\n")
    
    for i, gid in enumerate(games_to_scrape):
        # Progress update
        if i % 10 == 0:
            pct = (i / len(games_to_scrape)) * 100
            logger.info(f"⛏️  Progress: {i}/{len(games_to_scrape)} ({pct:.1f}%) | Success: {successful_games} | Failed: {failed_games}")
        
        # Fetch game data
        data = fetch_game_summary(gid)
        
        if not data or 'boxscore' not in data or 'teams' not in data['boxscore']:
            failed_games += 1
            continue
        
        try:
            # Parse teams
            team1_info = data['boxscore']['teams'][0]
            team2_info = data['boxscore']['teams'][1]
            
            t1_name = team1_info['team']['displayName']
            t2_name = team2_info['team']['displayName']
            
            # Parse stats
            t1_stats = parse_stat_group(team1_info['statistics'])
            t2_stats = parse_stat_group(team2_info['statistics'])
            
            if not t1_stats or not t2_stats:
                failed_games += 1
                continue
            
            # Calculate four factors
            t1_factors = calculate_four_factors(t1_stats, t2_stats)
            t2_factors = calculate_four_factors(t2_stats, t1_stats)
            
            if not t1_factors or not t2_factors:
                failed_games += 1
                continue
            
            # Get game info
            date_str = data['header']['competitions'][0]['date'][:10]
            neutral_site = data['header']['competitions'][0]['neutralSite']
            
            # Determine location
            comps = data['header']['competitions'][0]['competitors']
            home_id = next((c['id'] for c in comps if c['homeAway'] == 'home'), None)
            
            t1_loc = "Home" if team1_info['team']['id'] == home_id else "Away"
            t2_loc = "Away" if t1_loc == "Home" else "Home"
            
            if neutral_site:
                t1_loc = "Neutral"
                t2_loc = "Neutral"
            
            # Add rows
            new_rows.append({
                'GameID': gid, 'Date': date_str, 
                'Team': t1_name, 'Opponent': t2_name,
                'Location': t1_loc, **t1_factors
            })
            new_rows.append({
                'GameID': gid, 'Date': date_str,
                'Team': t2_name, 'Opponent': t1_name,
                'Location': t2_loc, **t2_factors
            })
            
            successful_games += 1
            
            # Show some successful games so you know it's working
            if successful_games <= 5 or successful_games % 50 == 0:
                logger.info(f"   ✅ Game {successful_games}: {t1_name} vs {t2_name}")
            
            # Safety save every 100 games
            if len(new_rows) >= 200:
                df_safe = pd.DataFrame(new_rows)
                mode = 'a' if os.path.exists(OUTPUT_FILE) else 'w'
                header = not os.path.exists(OUTPUT_FILE)
                df_safe.to_csv(OUTPUT_FILE, mode=mode, header=header, index=False)
                logger.info(f"💾 Safety save: {len(new_rows)} rows")
                new_rows = []
        
        except Exception as e:
            failed_games += 1
            if failed_games <= 10:
                logger.warning(f"⚠️ Error on game {gid}: {e}")
            continue
        
        time.sleep(REQUEST_DELAY)
    
    # Final save
    if new_rows:
        df = pd.DataFrame(new_rows)
        mode = 'a' if os.path.exists(OUTPUT_FILE) else 'w'
        header = not os.path.exists(OUTPUT_FILE)
        df.to_csv(OUTPUT_FILE, mode=mode, header=header, index=False)
        logger.info(f"💾 Final save: {len(new_rows)} rows")
    
    # Summary
    logger.info("\n" + "="*70)
    logger.info("🏁 SCRAPING COMPLETE")
    logger.info("="*70)
    logger.info(f"✅ Successful games: {successful_games}")
    logger.info(f"❌ Failed games: {failed_games}")
    logger.info(f"📊 Success rate: {(successful_games/(successful_games+failed_games)*100):.1f}%")
    logger.info(f"📁 Output file: {OUTPUT_FILE}")
    logger.info("="*70 + "\n")

if __name__ == "__main__":
    # Uncomment this to test a specific game:
    # test_single_game("401729314")  # Replace with actual game ID
    
    main()
