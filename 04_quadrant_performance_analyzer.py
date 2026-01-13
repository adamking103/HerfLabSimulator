"""
04_quadrant_performance_analyzer.py
===================================
THE BIBLE - Step 4: Quad Analysis (Powered by Robust Scraper)
"""

import pandas as pd
import numpy as np
import logging

# --- CONFIGURATION ---
INPUT_GAME_LOGS = "master_box_scores_2026.csv" 
INPUT_ADJ_PROFILES = "team_adjusted_efficiency_profiles_2026.csv"
OUTPUT_FILE = "team_quadrant_analysis_2026.csv"

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

def get_quadrant(rank):
    if pd.isna(rank): return 'Q4'
    if rank <= 50: return 'Q1'
    if rank <= 100: return 'Q2'
    if rank <= 200: return 'Q3'
    return 'Q4'

def weighted_avg(df, metric, weight_col='Possessions'):
    """Calculates weighted average of a percentage."""
    if df.empty or weight_col not in df.columns: return 0.0
    try:
        return (df[metric] * df[weight_col]).sum() / df[weight_col].sum()
    except: return 0.0

def main():
    logger.info("📊 Starting V10 Quadrant Analysis (Robust Data Source)...")
    
    if not pd.io.common.file_exists(INPUT_GAME_LOGS):
        logger.error(f"❌ {INPUT_GAME_LOGS} not found! Run Step 06 first.")
        return

    # Load Data
    logs = pd.read_csv(INPUT_GAME_LOGS)
    profiles = pd.read_csv(INPUT_ADJ_PROFILES)
    
    # --- AUTO-FIX COLUMN NAMES (The Fix) ---
    # We check for common names like 'Pts' or 'Score' and rename them to 'TeamScore'
    column_mappings = {
        'TeamScore': ['Pts', 'Points', 'TmScore', 'Score', 'TmPts'],
        'OpponentScore': ['OppPts', 'OppPoints', 'OppScore', 'Opp', 'OpponentPoints']
    }

    for standard_name, alternatives in column_mappings.items():
        if standard_name not in logs.columns:
            found = False
            for alt in alternatives:
                if alt in logs.columns:
                    logger.info(f"🔧 Auto-Repair: Renaming column '{alt}' to '{standard_name}'")
                    logs.rename(columns={alt: standard_name}, inplace=True)
                    found = True
                    break
            
            if not found and standard_name == 'TeamScore':
                # If we still can't find it, print available columns and stop safely
                logger.error(f"❌ ERROR: Could not find score columns.")
                logger.error(f"Your file has these columns: {logs.columns.tolist()}")
                return

    # Map Ranks (Using AdjRank from profiles)
    rank_map = dict(zip(profiles['Team'], profiles['AdjRank']))
    
    # Pre-calculate Opponent info
    logs['OppRank'] = logs['Opponent'].map(rank_map)
    logs['Quadrant'] = logs['OppRank'].apply(get_quadrant)
    
    results = []
    
    for team in profiles['Team'].unique():
        t_logs = logs[logs['Team'] == team]
        if t_logs.empty: continue
        
        prof = profiles[profiles['Team'] == team].iloc[0]
        
        # Calculate Wins safely now that columns are fixed
        wins = len(t_logs[t_logs['TeamScore'] > t_logs['OpponentScore']])

        row = {
            'Team': team,
            'AdjRank': prof['AdjRank'],
            'AdjNetEff': prof['AdjNetEff'],
            'TotalGames': len(t_logs),
            'TotalWins': wins
        }
        
        for q in ['Q1', 'Q2', 'Q3', 'Q4']:
            q_logs = t_logs[t_logs['Quadrant'] == q]
            count = len(q_logs)
            q_wins = len(q_logs[q_logs['TeamScore'] > q_logs['OpponentScore']])
            
            # Confidence
            if count >= 5: conf = 'HIGH'
            elif count >= 3: conf = 'MEDIUM'
            elif count >= 1: conf = 'LOW'
            else: conf = 'INSUFFICIENT'
            
            # Efficiency (NetEff column might be missing, calc on fly)
            net_rating = 0.0
            if count > 0:
                diff = (q_logs['TeamScore'] - q_logs['OpponentScore']).sum()
                poss = q_logs['Possessions'].sum()
                net_rating = (diff / poss) * 100 if poss > 0 else 0
            
            # 4 FACTORS AGGREGATION (Weighted)
            row[f'{q}_Record'] = f"{q_wins}-{count-q_wins}"
            row[f'{q}_Games'] = count
            row[f'{q}_WinPct'] = q_wins/count if count > 0 else 0
            row[f'{q}_NetEff'] = net_rating
            row[f'{q}_Confidence'] = conf
            
            row[f'{q}_eFG_Pct'] = weighted_avg(q_logs, 'eFG%')
            row[f'{q}_TO_Pct'] = weighted_avg(q_logs, 'TO%')
            row[f'{q}_OR_Pct'] = weighted_avg(q_logs, 'OR%')
            row[f'{q}_FT_Rate'] = weighted_avg(q_logs, 'FTR')

        results.append(row)
        
    # Save
    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    logger.info(f"✅ Success! Saved analysis to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
