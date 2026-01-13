"""
04_quadrant_mapper.py
=====================
THE BIBLE - Step 4 (Internal API Fix)
1. Uses ESPN API to create a "Short Name" -> "Long Name" translator.
2. Applies YOUR existing rankings to the schedule.
3. Fixes the 'Q1 Games = 0' bug without scraping KenPom.
"""

import pandas as pd
import requests
import logging

# --- CONFIGURATION ---
INPUT_BOX_SCORES = "master_box_scores_2026.csv"       # Your Games
INPUT_RANKINGS   = "team_quadrant_analysis_2026.csv"  # Your Rankings (We extract them from here)
OUTPUT_FILE      = "team_quadrant_analysis_fixed.csv"

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

# --- 1. BUILD THE ROSETTA STONE (API) ---
def get_team_translator():
    """
    Fetches all D1 teams from ESPN API to map Short Names to Long Names.
    Returns: dict { 'Duke': 'Duke Blue Devils', 'Omaha': 'Omaha Mavericks', ... }
    """
    logger.info("📡 Hitting ESPN API to build Name Translator...")
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?limit=1000"
    
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        
        translator = {}
        
        for item in data['sports'][0]['leagues'][0]['teams']:
            team = item['team']
            full_name = team['displayName']     # "Duke Blue Devils"
            short_name = team.get('shortDisplayName', team.get('name', '')) # "Duke"
            abbr = team.get('abbreviation', '') # "DUKE"
            
            # Map ALL variants to the Full Name
            translator[short_name] = full_name
            translator[abbr] = full_name
            translator[full_name] = full_name # Self-match
            
            # Also try stripping "State" or common variations if needed
            # For now, the API shortName is usually what's in the schedule
            
        logger.info(f"✅ Learned {len(translator)} name variations.")
        return translator
        
    except Exception as e:
        logger.error(f"❌ API Error: {e}")
        return {}

# --- 2. QUADRANT LOGIC ---
def get_quadrant(rank, location):
    if pd.isna(rank): return 'Q4'
    
    # Standard NCAA Definitions
    loc = str(location).upper()
    if loc.startswith('H'): # Home
        if rank <= 30: return 'Q1'
        if rank <= 75: return 'Q2'
        if rank <= 160: return 'Q3'
        return 'Q4'
    elif loc.startswith('A'): # Away
        if rank <= 75: return 'Q1'
        if rank <= 135: return 'Q2'
        if rank <= 240: return 'Q3'
        return 'Q4'
    else: # Neutral
        if rank <= 50: return 'Q1'
        if rank <= 100: return 'Q2'
        if rank <= 200: return 'Q3'
        return 'Q4'

# --- 3. MAIN SCRIPT ---
def main():
    # A. Load Data
    try:
        games = pd.read_csv(INPUT_BOX_SCORES)
        # Load the Analysis file just to get the Ranks you already have
        rank_source = pd.read_csv(INPUT_RANKINGS) 
        
        # Create Rank Map: {'Duke Blue Devils': 5, ...}
        # We assume 'Team' column in analysis file is the Full Name
        rank_map = dict(zip(rank_source['Team'], rank_source['AdjRank']))
        
        logger.info(f"📋 Loaded {len(games)} games and {len(rank_map)} ranked teams.")
        
    except FileNotFoundError as e:
        logger.error(f"❌ Missing File: {e}")
        return

    # B. Get Translator
    translator = get_team_translator()
    
    # C. Apply Fix
    logger.info("⚙️  Fixing Opponent Names & Assigning Ranks...")
    
    def resolve_rank(short_name):
        # 1. Try Direct Match (e.g. "Duke Blue Devils" in schedule)
        if short_name in rank_map: 
            return rank_map[short_name]
            
        # 2. Try Translator (e.g. "Duke" -> "Duke Blue Devils")
        if short_name in translator:
            long_name = translator[short_name]
            if long_name in rank_map:
                return rank_map[long_name]
                
        # 3. Try Fuzzy / "StartsWith" (e.g. "Omaha" starts "Omaha Mavericks")
        # This is the "Silver Bullet" for stubborn names
        for long_name, rank in rank_map.items():
            if str(long_name).startswith(short_name):
                return rank
                
        return 999 # Q4

    # Apply Ranking
    games['OppRank'] = games['Opponent'].apply(resolve_rank)
    
    # Apply Quadrants
    games['Quadrant'] = games.apply(lambda x: get_quadrant(x['OppRank'], x['Location']), axis=1)
    
    # D. Aggregate & Save (Re-run the Analysis Logic)
    logger.info("📊 Re-calculating Quadrant Records...")
    
    results = []
    teams = games['Team'].unique()
    
    for team in teams:
        t_logs = games[games['Team'] == team]
        
        # Get Team's Own Rank
        my_rank = rank_map.get(team, 999)
        
        wins = len(t_logs[t_logs['TeamScore'] > t_logs['OpponentScore']])
        
        row = {
            'Team': team,
            'AdjRank': my_rank,
            'TotalGames': len(t_logs),
            'TotalWins': wins,
        }
        
        for q in ['Q1', 'Q2', 'Q3', 'Q4']:
            q_logs = t_logs[t_logs['Quadrant'] == q]
            count = len(q_logs)
            q_wins = len(q_logs[q_logs['TeamScore'] > q_logs['OpponentScore']])
            
            row[f'{q}_Record'] = f"{q_wins}-{count-q_wins}"
            row[f'{q}_Games'] = count
            row[f'{q}_WinPct'] = round(q_wins/count, 2) if count > 0 else 0.0
            
            # Simple Eff Calculation
            if count > 0:
                 # Net Eff = (TeamPts - OppPts) / Poss * 100
                 diff = q_logs['TeamScore'].sum() - q_logs['OpponentScore'].sum()
                 poss = q_logs['Possessions'].sum()
                 row[f'{q}_NetEff'] = round((diff / poss) * 100, 1)
            else:
                 row[f'{q}_NetEff'] = 0.0

        results.append(row)

    final_df = pd.DataFrame(results).sort_values('AdjRank')
    final_df.to_csv(OUTPUT_FILE, index=False)
    
    logger.info(f"✅ FIXED! Saved to {OUTPUT_FILE}")
    
    # Validation
    q1_count = final_df['Q1_Games'].sum()
    logger.info(f"🔍 Validation: Found {q1_count} total Q1 games played (Should be > 0).")
    if q1_count > 0:
        logger.info("🎉 The name mapping worked!")
    else:
        logger.warning("⚠️ Still seeing 0 Q1 games. We might need to check specific team names.")

if __name__ == "__main__":
    main()
