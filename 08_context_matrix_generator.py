"""
08_context_matrix_generator_offline.py
========================================================
THE BIBLE - PHASE 3c: SELF-CONTAINED MATRIX GENERATOR
========================================================
PURPOSE:
  - Generates Context Files WITHOUT relying on external APIs.
  - Uses your 'Master Box Scores' to calculate internal strength ratings.
  - Solves the 'Name Mismatch' error by keeping everything internal.
"""

import pandas as pd
import numpy as np
import os

# CONFIGURATION
BOX_SCORE_FILE = "master_box_scores_2026.csv"
OUTPUT_HOME = "team_home_performance_by_quadrant_2026.csv"
OUTPUT_ROAD = "team_road_performance_by_quadrant_2026.csv"

def load_data():
    if not os.path.exists(BOX_SCORE_FILE):
        print(f"❌ Error: {BOX_SCORE_FILE} not found.")
        return None
    return pd.read_csv(BOX_SCORE_FILE)

def calculate_internal_ranks(df):
    """
    Generates a 'Strength Rank' for every team based on the box score data.
    We use a simple 'Forensic Score' average (eFG, TO, OR, FTR weighted).
    """
    print("⚖️  Calculating Internal Team Strength Ratings...")
    
    # Calculate a "Performance Score" for every single game row
    # (eFG*2) - (TO*1.5) + (OR*0.5) + (FTR*0.3)
    # This roughly approximates efficiency.
    df['ForensicScore'] = (df['eFG%'] * 2.0) - (df['TO%'] * 1.5) + (df['OR%'] * 0.5) + (df['FTR'] * 0.3)
    
    # Group by Team to get average strength
    team_strength = df.groupby('Team')['ForensicScore'].mean().sort_values(ascending=False)
    
    # Rank them 1 to 363
    rank_map = team_strength.rank(ascending=False, method='min').to_dict()
    
    print(f"✅ Generated ranks for {len(rank_map)} teams internally.")
    return rank_map

def get_quadrant(rank, location):
    """Assigns Q1-Q4 based on the internal rank."""
    if location == 'Home':
        if rank <= 30: return 'Q1'
        if rank <= 75: return 'Q2'
        if rank <= 160: return 'Q3'
        return 'Q4'
    elif location == 'Neutral':
        if rank <= 50: return 'Q1'
        if rank <= 100: return 'Q2'
        if rank <= 200: return 'Q3'
        return 'Q4'
    else: # Away
        if rank <= 75: return 'Q1'
        if rank <= 135: return 'Q2'
        if rank <= 240: return 'Q3'
        return 'Q4'

def build_matrices():
    df = load_data()
    if df is None: return

    # 1. Get Ranks (Internally)
    rank_map = calculate_internal_ranks(df)
    
    # 2. Build Profiles
    team_data = {}
    
    print("🧠 Categorizing games into Contextual Quadrants...")
    
    for idx, row in df.iterrows():
        team = row['Team']
        opp = row['Opponent']
        loc = row['Location']
        
        # Get Opponent Rank (Using internal map, so names MATCH PERFECTLY)
        opp_rank = rank_map.get(opp, 363) # Default to 363 if new team
        
        # Determine Quadrant
        quad = get_quadrant(opp_rank, loc)
        
        # Calculate Net Score (Performance vs Baseline of ~97)
        # 97 is roughly the league average Forensic Score
        net_eff = row['ForensicScore'] - 97.0
        
        if team not in team_data: team_data[team] = {'Home': {}, 'Away': {}, 'Neutral': {}}
        if quad not in team_data[team][loc]: team_data[team][loc][quad] = []
        
        team_data[team][loc][quad].append(net_eff)

    # 3. Aggregation & Export
    home_rows = []
    road_rows = []
    
    for team, locs in team_data.items():
        # Home Profile
        h_row = {'Team': team}
        all_h = []
        for q in ['Q1', 'Q2', 'Q3', 'Q4']:
            scores = locs['Home'].get(q, [])
            h_row[f'{q}_Games'] = len(scores)
            h_row[f'{q}_NetEff'] = round(np.mean(scores), 2) if scores else 0
            all_h.extend(scores)
        h_row['AdjNetEff'] = round(np.mean(all_h), 2) if all_h else 0
        home_rows.append(h_row)
        
        # Road Profile (Away + Neutral)
        r_row = {'Team': team}
        combined_road = locs['Away'].copy()
        for q, s in locs['Neutral'].items():
            if q not in combined_road: combined_road[q] = []
            combined_road[q].extend(s)
            
        all_r = []
        for q in ['Q1', 'Q2', 'Q3', 'Q4']:
            scores = combined_road.get(q, [])
            r_row[f'{q}_Games'] = len(scores)
            r_row[f'{q}_NetEff'] = round(np.mean(scores), 2) if scores else 0
            all_r.extend(scores)
        r_row['AdjNetEff'] = round(np.mean(all_r), 2) if all_r else 0
        road_rows.append(r_row)

    pd.DataFrame(home_rows).to_csv(OUTPUT_HOME, index=False)
    pd.DataFrame(road_rows).to_csv(OUTPUT_ROAD, index=False)
    
    print(f"\n✅ SUCCESS!")
    print(f"   📂 Generated: {OUTPUT_HOME}")
    print(f"   📂 Generated: {OUTPUT_ROAD}")
    print("   🚀 Simulator is ready to use these files.")

if __name__ == "__main__":
    build_matrices()
