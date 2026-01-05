"""
03_sos_adjustment_processor.py
==============================
THE BIBLE - Data Pipeline Step 3 (DEBUG VERSION)
"""

import pandas as pd
import numpy as np
import re
import sys
import os
from difflib import SequenceMatcher
from typing import Dict, Optional

# ============================================================================
# CONFIGURATION
# ============================================================================
INPUT_GAME_LOGS = "master_game_logs_2026.csv"
INPUT_RAW_PROFILES = "team_raw_efficiency_profiles_2026.csv"
OUTPUT_FILE = "team_adjusted_efficiency_profiles_2026.csv"

MAX_ITERATIONS = 100
CONVERGENCE_THRESHOLD = 0.001

# ============================================================================
# DATA: MASCOTS & ALIASES
# ============================================================================
MASCOTS = [
    'crimson tide', 'razorbacks', 'tigers', 'gators', 'bulldogs', 'wildcats',
    'rebels', 'commodores', 'gamecocks', 'volunteers', 'vols', 'aggies', 
    'longhorns', 'buckeyes', 'wolverines', 'spartans', 'nittany lions', 'hawkeyes',
    'golden gophers', 'gophers', 'badgers', 'boilermakers', 'hoosiers', 
    'fighting illini', 'illini', 'cornhuskers', 'huskers', 'scarlet knights',
    'terrapins', 'terps', 'bruins', 'trojans', 'ducks', 'huskies', 'cougars',
    'jayhawks', 'red raiders', 'horned frogs', 'bears', 'cyclones', 'sooners',
    'cowboys', 'cowgirls', 'mountaineers', 'bearcats', 'knights', 'sun devils',
    'buffaloes', 'buffs', 'utes', 'cardinals', 'red storm', 'blue devils', 
    'tar heels', 'wolfpack', 'wolf pack', 'demon deacons', 'cavaliers', 'wahoos', 
    'hokies', 'yellow jackets', 'ramblin wreck', 'seminoles', 'noles', 'hurricanes', 
    'canes', 'orange', 'orangemen', 'panthers', 'fighting irish', 'irish', 'eagles', 
    'cardinal', 'blue jays', 'bluejays', 'hoyas', 'pirates', 'friars', 'musketeers',
    'golden eagles', 'marquette golden eagles', 'providence friars', 'green wave', 
    'owls', 'mustangs', 'bulls', 'blazers', 'mean green', 'roadrunners', 
    'thundering herd', 'aztecs', 'falcons', 'rams', 'lobos', 'running rebels', 
    'broncos', 'rainbow warriors', 'warriors', 'gaels', 'toreros', 'dons', 'waves', 
    'lions', 'pilots', 'zags', 'gonzaga bulldogs', 'big green', 'crimson', 'quakers', 
    'big red', 'leopards', 'mountain hawks', 'raiders', 'crusaders', 'bison', 
    'black knights', 'cadets', 'midshipmen', 'mids', 'tribe', 'monarchs', 'dukes', 
    'patriots', 'pride', 'phoenix', 'seahawks', 'dragons', 'billikens', 'colonials', 
    'explorers', 'hawks', 'flyers', 'bonnies', 'spiders', 'rhodies', 'redbirds', 
    'braves', 'shockers', 'sycamores', 'leathernecks', 'salukis', 'catamounts', 
    'great danes', 'retrievers', 'river hawks', 'seawolves', 'terriers', 'mastodons',
    'flames', 'penguins', 'norse', 'vikings', 'jaguars', 'roos', 'coyotes', 
    'jackrabbits', 'jaspers', 'peacocks', 'red foxes', 'purple eagles', 'stags', 
    'griffs', 'golden griffins', 'saints', 'mocs', 'paladins', 'keydets', 
    'buccaneers', 'bucs', 'rattlers', 'lumberjacks', 'vandals', 'grizzlies', 'griz', 
    'bobcats', 'bengals', 'thunderbirds', 'matadors', 'gauchos', 'highlanders', 
    'anteaters', 'titans', 'beach', 'governors', 'govs', 'colonels', 'racers', 
    'redhawks', 'skyhawks', 'demons', 'privateers', 'tritons', 'banana slugs', 
    'fighting camels', 'ichabods', 'penmen', 'blue hose', 'chanticleers', 'firebirds'
]

TEAM_ALIASES = {
    'uc san diego': 'ucsd', 'uc davis': 'ucdavis', 'uc irvine': 'ucirvine',
    'uc riverside': 'ucriverside', 'uc santa barbara': 'ucsb',
    'cal state fullerton': 'csfullerton', 'cal state northridge': 'csnorthridge',
    'cal state bakersfield': 'csbakersfield', 'long beach state': 'longbeachstate',
    'san diego state': 'sandiegostate', 'san jose state': 'sanjosestate',
    'fresno state': 'fresnostate', 'usc': 'southerncal', 'ucla': 'ucla',
    'unlv': 'unlv', 'utep': 'utep', 'utsa': 'utsa', 'unc': 'northcarolina',
    'uconn': 'connecticut', 'smu': 'smu', 'tcu': 'tcu', 'lsu': 'lsu', 'vcu': 'vcu',
    'fiu': 'fiu', 'fau': 'fau', 'byu': 'byu', 'ucf': 'ucf', 'penn state': 'pennstate',
    'penn st': 'pennstate', 'ohio state': 'ohiostate', 'ohio st': 'ohiostate',
    'michigan state': 'michiganstate', 'michigan st': 'michiganstate',
    'florida state': 'floridastate', 'florida st': 'floridastate',
    'nc state': 'ncstate', 'north carolina state': 'ncstate',
    'iowa state': 'iowastate', 'kansas state': 'kansasstate',
    'oklahoma state': 'oklahomastate', 'oregon state': 'oregonstate',
    'washington state': 'washingtonstate', 'arizona state': 'arizonastate',
    'colorado state': 'coloradostate', "saint mary's": 'stmarys',
    "st. mary's": 'stmarys', "st mary's": 'stmarys', "saint john's": 'stjohns',
    "st. john's": 'stjohns', "st john's": 'stjohns', "saint joseph's": 'saintjosephs',
    "st. joseph's": 'saintjosephs', "saint louis": 'saintlouis',
    "st. louis": 'saintlouis', "saint peter's": 'stpeters', "st. peter's": 'stpeters',
    "saint bonaventure": 'stbonaventure', "st. bonaventure": 'stbonaventure',
    'north carolina': 'northcarolina', 'south carolina': 'southcarolina',
    'north texas': 'northtexas', 'south florida': 'southflorida',
    'east carolina': 'eastcarolina', 'west virginia': 'westvirginia',
    'northern iowa': 'northerniowa', 'southern illinois': 'southernillinois',
    'western kentucky': 'westernkentucky', 'eastern kentucky': 'easternkentucky',
    'middle tennessee': 'middletennessee', 'ole miss': 'mississippi',
    'pitt': 'pittsburgh', "hawai'i": 'hawaii', 'miami (fl)': 'miami',
    'miami (oh)': 'miamioh', 'miami florida': 'miami', 'miami ohio': 'miamioh',
    'louisiana': 'louisiana', 'louisiana-lafayette': 'louisiana',
    'ul lafayette': 'louisiana', 'louisiana-monroe': 'ulmonroe',
    'ul monroe': 'ulmonroe', 'texas a&m': 'texasam', 'texas a & m': 'texasam',
}

# ============================================================================
# CORE FUNCTIONS
# ============================================================================

def normalize_team_name(name: str) -> str:
    if not isinstance(name, str): return ""
    name = name.lower().strip()
    
    # 2. NEW FIX: Strip leading digits (e.g., "12Gonzaga" -> "Gonzaga")
    name = re.sub(r'^\d+', '', name)

    if name in TEAM_ALIASES: return TEAM_ALIASES[name]
    
    for mascot in MASCOTS:
        pattern = r'\s+' + re.escape(mascot) + r'\s*$'
        name = re.sub(pattern, '', name)
        
    name = name.replace('st.', 'st').replace('&', 'and').replace("'", '').replace('-', '')
    name = re.sub(r'[^a-z0-9]', '', name).strip()
    
    if name in TEAM_ALIASES: return TEAM_ALIASES[name]
    return name

def run_iterative_adjustment(logs_df, profiles_df):
    print("  -> Creating Normalized Keys...")
    profiles_df = profiles_df.copy()
    profiles_df['NormKey'] = profiles_df['Team'].apply(normalize_team_name)
    
    logs_df = logs_df.copy()
    logs_df['TeamKey'] = logs_df['Team'].apply(normalize_team_name)
    logs_df['OppKey'] = logs_df['Opponent'].apply(normalize_team_name)
    
    # DEBUG: Check matches
    unique_opps = set(logs_df['OppKey'])
    unique_teams = set(profiles_df['NormKey'])
    missing = unique_opps - unique_teams
    print(f"  -> Match Rate: {len(unique_opps) - len(missing)}/{len(unique_opps)} opponents matched.")
    if "gonzaga" in unique_teams:
        print("     (OK) 'gonzaga' is in Team Profiles")
    else:
        print("     (ERROR) 'gonzaga' NOT found in Team Profiles!")

    league_avg_off = profiles_df['RawOffEff'].mean()
    league_avg_def = profiles_df['RawDefEff'].mean()
    
    profiles_df['AdjOffEff'] = profiles_df['RawOffEff'].copy()
    profiles_df['AdjDefEff'] = profiles_df['RawDefEff'].copy()
    
    print(f"  -> Starting Iterations (Max {MAX_ITERATIONS})...")
    
    for iteration in range(MAX_ITERATIONS):
        prev_off = profiles_df['AdjOffEff'].copy()
        prev_def = profiles_df['AdjDefEff'].copy()
        
        off_map = dict(zip(profiles_df['NormKey'], profiles_df['AdjOffEff']))
        def_map = dict(zip(profiles_df['NormKey'], profiles_df['AdjDefEff']))
        
        new_adj_off = []
        new_adj_def = []
        sos_values = []

        for _, row in profiles_df.iterrows():
            team_key = row['NormKey']
            team_games = logs_df[logs_df['TeamKey'] == team_key]
            
            if team_games.empty:
                new_adj_off.append(row['AdjOffEff'])
                new_adj_def.append(row['AdjDefEff'])
                sos_values.append(0)
                continue
            
            opp_adj_defs = team_games['OppKey'].apply(lambda x: def_map.get(x, league_avg_def))
            opp_adj_offs = team_games['OppKey'].apply(lambda x: off_map.get(x, league_avg_off))
            
            sos = opp_adj_offs.mean() - opp_adj_defs.mean()
            adj_off = row['RawOffEff'] + (opp_adj_defs.mean() - league_avg_def)
            adj_def = row['RawDefEff'] + (opp_adj_offs.mean() - league_avg_off)
            
            new_adj_off.append(adj_off)
            new_adj_def.append(adj_def)
            sos_values.append(sos)
        
        profiles_df['AdjOffEff'] = new_adj_off
        profiles_df['AdjDefEff'] = new_adj_def
        profiles_df['SOS'] = sos_values
        
        change = max((profiles_df['AdjOffEff'] - prev_off).abs().max(),
                     (profiles_df['AdjDefEff'] - prev_def).abs().max())
        
        if change < CONVERGENCE_THRESHOLD:
            print(f"  -> Converged after {iteration+1} iterations.")
            break
            
    profiles_df['AdjNetEff'] = profiles_df['AdjOffEff'] - profiles_df['AdjDefEff']
    profiles_df['AdjRank'] = profiles_df['AdjNetEff'].rank(ascending=False, method='min')
    profiles_df['NetAdjustment'] = profiles_df['AdjNetEff'] - profiles_df['RawNetEff']
    
    return profiles_df.drop(columns=['NormKey'])

# ============================================================================
# EXECUTION
# ============================================================================
def main():
    print("="*60)
    print("THE BIBLE - Step 3: SOS Adjustment (DEBUG MODE)")
    print("="*60)
    
    if not os.path.exists(INPUT_GAME_LOGS):
        print(f"ERROR: {INPUT_GAME_LOGS} not found!")
        return
    if not os.path.exists(INPUT_RAW_PROFILES):
        print(f"ERROR: {INPUT_RAW_PROFILES} not found!")
        print("Run Step 2 first!")
        return

    print("Loading data...")
    logs = pd.read_csv(INPUT_GAME_LOGS)
    profiles = pd.read_csv(INPUT_RAW_PROFILES)
    print(f"Loaded {len(logs)} games and {len(profiles)} profiles.")

    adjusted = run_iterative_adjustment(logs, profiles)
    
    print("Saving output...")
    adjusted.sort_values('AdjRank').to_csv(OUTPUT_FILE, index=False)
    print(f"SUCCESS! Saved to {OUTPUT_FILE}")
    
    # Check Alabama
    print("\n--- CHECKING ALABAMA ---")
    bama = adjusted[adjusted['Team'].str.contains("Alabama", case=False)]
    if not bama.empty:
        print(bama[['Team', 'AdjRank', 'AdjNetEff', 'SOS']].to_string(index=False))
    else:
        print("Alabama not found in output!")

if __name__ == "__main__":
    main()
