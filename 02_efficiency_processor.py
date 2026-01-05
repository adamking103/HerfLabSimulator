"""
02_efficiency_processor.py
==========================
THE BIBLE - Data Pipeline Step 2 (FINAL FIXED VERSION)
Fixes:
1. Mascot Stripping (matches "Alabama Crimson Tide" -> "Alabama")
2. Tempo Priority (uses "Tempo" instead of "AdjTempo" for accurate possession counts)
"""

import pandas as pd
import numpy as np
import logging
import re
import os
import sys

# ============================================================================
# CONFIGURATION
# ============================================================================
INPUT_GAME_LOGS = "master_game_logs_2026.csv"
KENPOM_DATA_FILE = "kenpom_2026.csv"
OUTPUT_FILE = "team_raw_efficiency_profiles_2026.csv"
DEFAULT_TEMPO = 68.0

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# DATA: MASCOTS
# ============================================================================
MASCOTS = [
    'crimson tide', 'razorbacks', 'tigers', 'gators', 'bulldogs', 'wildcats',
    'rebels', 'commodores', 'gamecocks', 'volunteers', 'aggies', 'longhorns',
    'buckeyes', 'wolverines', 'spartans', 'nittany lions', 'hawkeyes',
    'golden gophers', 'badgers', 'boilermakers', 'hoosiers', 'fighting illini',
    'cornhuskers', 'scarlet knights', 'terrapins', 'bruins', 'trojans',
    'ducks', 'huskies', 'cougars', 'jayhawks', 'red raiders', 'horned frogs',
    'bears', 'cyclones', 'sooners', 'cowboys', 'mountaineers', 'bearcats',
    'knights', 'sun devils', 'buffaloes', 'utes', 'cardinals', 'blue devils',
    'tar heels', 'wolfpack', 'demon deacons', 'cavaliers', 'hokies',
    'yellow jackets', 'seminoles', 'hurricanes', 'orange', 'panthers',
    'fighting irish', 'eagles', 'blue jays', 'hoyas', 'red storm', 'pirates',
    'friars', 'musketeers', 'golden eagles', 'tritons', 'billikens', 'gaels',
    'toreros', 'dons', 'waves', 'broncos', 'aztecs', 'running rebels',
    'falcons', 'owls', 'mean green', 'miners', 'roadrunners'
]

# ============================================================================
# NORMALIZATION LOGIC
# ============================================================================
def normalize_name(name):
    """Standardizes team names by stripping mascots and punctuation."""
    if not isinstance(name, str): return ""
    name = name.lower().strip()
    
    # Strip Mascots
    for mascot in MASCOTS:
        pattern = r'\s+' + re.escape(mascot) + r'\s*$'
        name = re.sub(pattern, '', name)
        
    # Standardize
    name = name.replace('st.', 'st').replace('&', 'and').replace("'", '').replace('-', '')
    name = re.sub(r'[^a-z0-9]', '', name)
    return name.strip()

def create_name_matcher(teams: list) -> dict:
    matcher = {}
    for team in teams:
        norm = normalize_name(team)
        matcher[norm] = team
    return matcher

# ============================================================================
# KENPOM LOADING
# ============================================================================
def load_kenpom_tempo(filepath: str) -> dict:
    if not os.path.exists(filepath):
        logger.error(f"❌ ERROR: KenPom file '{filepath}' not found!")
        return {}
        
    try:
        df = pd.read_csv(filepath)
        cols = df.columns.tolist()
        
        # FIXED: Look for 'Tempo' (Raw) FIRST, then 'AdjTempo'
        # This ensures we get the faster raw pace (e.g. 75.8) instead of adjusted (73.9)
        tempo_col = next((c for c in ['Tempo', 'AdjTempo', 'tempo'] if c in cols), None)
        team_col = next((c for c in ['TeamName', 'Team', 'team', 'School'] if c in cols), None)
        
        if tempo_col and team_col:
            tempo_dict = dict(zip(df[team_col], df[tempo_col]))
            
            # DEBUG Confirmation
            if "Alabama" in tempo_dict:
                logger.info(f"✅ Loaded KenPom. Alabama Tempo = {tempo_dict['Alabama']} (Should be > 75.0)")
                
            return tempo_dict
        else:
            logger.error(f"❌ Could not find Team/Tempo columns in {filepath}")
            return {}
    except Exception as e:
        logger.error(f"❌ Error loading KenPom: {e}")
        return {}

# ============================================================================
# MAIN PROCESSING
# ============================================================================
def process_efficiency():
    logger.info("=" * 60)
    logger.info("THE BIBLE - Step 2: Efficiency Processor")
    logger.info("=" * 60)
    
    games_df = pd.read_csv(INPUT_GAME_LOGS)
    tempo_data = load_kenpom_tempo(KENPOM_DATA_FILE)
    use_tempo = len(tempo_data) > 0
    
    # Create Matcher
    tempo_matcher = create_name_matcher(list(tempo_data.keys()))
    
    team_profiles = []
    all_teams = games_df['Team'].unique()
    
    logger.info(f"Processing {len(all_teams)} teams...")
    
    for team in all_teams:
        # Determine Team Tempo
        team_tempo = DEFAULT_TEMPO
        if use_tempo:
            norm_name = normalize_name(team)
            if norm_name in tempo_matcher:
                matched_name = tempo_matcher[norm_name]
                team_tempo = tempo_data[matched_name]
        
        # Process Games
        team_games = games_df[games_df['Team'] == team].copy()
        possessions_list = []
        
        for _, game in team_games.iterrows():
            opp_tempo = DEFAULT_TEMPO
            if use_tempo:
                opp_norm = normalize_name(game['Opponent'])
                if opp_norm in tempo_matcher:
                    opp_tempo = tempo_data[tempo_matcher[opp_norm]]
            
            # Possessions Formula
            if use_tempo:
                poss = (team_tempo + opp_tempo) / 2
            else:
                poss = (game['TeamScore'] + game['OpponentScore']) / 1.84
                poss = max(50, min(90, poss))
                
            possessions_list.append(poss)
        
        team_games['Possessions'] = possessions_list
        team_games['OffEff'] = (team_games['TeamScore'] / team_games['Possessions']) * 100
        team_games['DefEff'] = (team_games['OpponentScore'] / team_games['Possessions']) * 100
        team_games['NetEff'] = team_games['OffEff'] - team_games['DefEff']
        
        profile = {
            'Team': team,
            'Games': len(team_games),
            'Record': f"{(team_games['Result']=='W').sum()}-{((team_games['Result']=='L').sum())}",
            'WinPct': (team_games['Result']=='W').mean(),
            'RawOffEff': team_games['OffEff'].mean(),
            'RawDefEff': team_games['DefEff'].mean(),
            'RawNetEff': team_games['NetEff'].mean(),
            'AvgTempo': team_games['Possessions'].mean(),
            'OffEffStd': team_games['OffEff'].std(),
            'DefEffStd': team_games['DefEff'].std(),
            'AvgPointsFor': team_games['TeamScore'].mean(),
            'AvgPointsAgainst': team_games['OpponentScore'].mean(),
            'AvgMargin': team_games['Margin'].mean(),
        }
        team_profiles.append(profile)
    
    profiles_df = pd.DataFrame(team_profiles)
    profiles_df['RawRank'] = profiles_df['RawNetEff'].rank(ascending=False, method='min')
    profiles_df.sort_values('RawRank').to_csv(OUTPUT_FILE, index=False)
    
    logger.info(f"✅ Success! Processed {len(profiles_df)} teams.")
    logger.info(f"Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    process_efficiency()
