"""
09_hca_decomposer.py
========================================================
THE BIBLE - PHASE 4: CONTEXTUAL HCA DECOMPOSER
========================================================
PURPOSE:
  - Calculates "True" Home Court Advantage (HCA) by Quadrant.
  - Decomposes HCA into 4 drivers: Crowd (Defense), Refs, Comfort (Shooting), Hustle (Rebounding).
  - Uses Bayesian Shrinkage to fix small sample sizes.
"""

import pandas as pd
import numpy as np

# CONFIGURATION
INPUT_FILE = "master_box_scores_2026.csv"
OUTPUT_FILE = "team_contextual_hca_2026.csv"
K_PRIOR = 5  # Bayesian weight (games needed for full credibility)

def load_and_prep_data():
    print("📊 Loading Master Box Scores...")
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: {INPUT_FILE} not found.")
        return None
        
    df = pd.read_csv(INPUT_FILE)
    
    # 1. Calculate Forensic Performance Score (Efficiency Proxy)
    # Formula: (eFG*2) - (TO*1.5) + (OR*0.5) + (FTR*0.3)
    # This correlates highly with Net Rating.
    df['ForensicScore'] = (df['eFG%'] * 2.0) - (df['TO%'] * 1.5) + (df['OR%'] * 0.5) + (df['FTR'] * 0.3)
    
    return df

def generate_internal_ranks(df):
    """
    Generates a 'Strength Rank' based on average Forensic Score.
    This makes the script self-contained (no API needed).
    """
    print("⚖️  Calculating Team Strength Profiles...")
    team_strength = df.groupby('Team')['ForensicScore'].mean().sort_values(ascending=False)
    team_ranks = team_strength.rank(ascending=False, method='min')
    
    # Map ranks to dataframe
    rank_map = team_ranks.to_dict()
    df['OpponentRank'] = df['Opponent'].map(rank_map).fillna(300) # Default to 300 if unknown
    
    return df

def assign_quadrant(rank):
    if rank <= 50: return 'Q1'
    elif rank <= 100: return 'Q2'
    elif rank <= 200: return 'Q3'
    return 'Q4'

def get_opponent_stats(df):
    """
    Self-joins the dataframe to get Opponent Stats (Defensive metrics).
    """
    print("🔄 Decomposing Matchups (Self-Join)...")
    # Drop duplicates to prevent join explosion
    df_dedup = df.drop_duplicates(subset=['GameID', 'Team'])
    
    # Merge on GameID
    df_merged = pd.merge(df_dedup, df_dedup, on='GameID', suffixes=('', '_Opp'))
    
    # Filter out self-matches
    df_merged = df_merged[df_merged['Team'] != df_merged['Team_Opp']]
    
    # Calculate Net Score (Team - Opponent)
    df_merged['Net_Forensic'] = df_merged['ForensicScore'] - df_merged['ForensicScore_Opp']
    
    return df_merged

def calculate_hca_deltas(df):
    print("🧠 Computing Bayesian HCA Deltas...")
    
    # 1. Assign Quadrants
    df['OpponentQuad'] = df['OpponentRank'].apply(assign_quadrant)
    
    # 2. Filter for Home/Away only
    df_loc = df[df['Location'].isin(['Home', 'Away'])].copy()
    
    # 3. Aggregate Metrics by Team/Location/Quad
    metrics = {
        'Net_Forensic': 'mean', # The Scoreboard
        'eFG%': 'mean',         # Comfort
        'FTR': 'mean',          # Refs
        'OR%': 'mean',          # Hustle
        'TO%_Opp': 'mean',      # Crowd (Defense)
        'GameID': 'count'
    }
    
    grouped = df_loc.groupby(['Team', 'Location', 'OpponentQuad']).agg(metrics).reset_index()
    
    # 4. Pivot to compare Home vs Away
    pivoted = grouped.pivot(index=['Team', 'OpponentQuad'], columns='Location', values=list(metrics.keys()))
    pivoted.columns = [f"{col[1]}_{col[0]}" for col in pivoted.columns]
    pivoted = pivoted.reset_index()
    
    # 5. Calculate Raw Deltas (Home - Away)
    # Positive = Better at Home
    pivoted['HCA_Raw'] = pivoted['Home_Net_Forensic'] - pivoted['Away_Net_Forensic']
    
    # Decomposition Deltas
    pivoted['Delta_Comfort'] = pivoted['Home_eFG%'] - pivoted['Away_eFG%']
    pivoted['Delta_Refs'] = pivoted['Home_FTR'] - pivoted['Away_FTR']
    pivoted['Delta_Hustle'] = pivoted['Home_OR%'] - pivoted['Away_OR%']
    pivoted['Delta_Crowd'] = pivoted['Home_TO%_Opp'] - pivoted['Away_TO%_Opp'] # Opponent TOs
    
    # 6. Bayesian Shrinkage
    # Calculate Team's Overall HCA (across all quads)
    global_agg = df_loc.groupby(['Team', 'Location'])['Net_Forensic'].mean().unstack()
    global_agg['Overall_HCA'] = global_agg['Home'] - global_agg['Away']
    league_avg_hca = global_agg['Overall_HCA'].mean()
    
    # Merge Overall HCA
    pivoted = pd.merge(pivoted, global_agg[['Overall_HCA']], on='Team', how='left')
    pivoted['Overall_HCA'] = pivoted['Overall_HCA'].fillna(league_avg_hca)
    
    # Bayesian Formula
    # We use Min(Home, Away) games as the "Credibility" factor.
    # If you have 10 home games but 0 away games, we can't measure the Delta, so credibility is 0.
    pivoted['Min_Games'] = pivoted[['Home_GameID', 'Away_GameID']].min(axis=1).fillna(0)
    pivoted['Credibility'] = pivoted['Min_Games'] / (pivoted['Min_Games'] + K_PRIOR)
    
    # Handle Missing Raw HCA (if no games played)
    pivoted['HCA_Raw'] = pivoted['HCA_Raw'].fillna(pivoted['Overall_HCA'])
    
    pivoted['HCA_Adjusted'] = (pivoted['Credibility'] * pivoted['HCA_Raw']) + \
                              ((1 - pivoted['Credibility']) * pivoted['Overall_HCA'])
                              
    return pivoted

def classify_drivers(df):
    """
    Identifies the 'Why'.
    We weight the factors by their coefficients in the Forensic Score.
    """
    df['Impact_Comfort'] = df['Delta_Comfort'] * 2.0
    df['Impact_Refs'] = df['Delta_Refs'] * 0.3
    df['Impact_Hustle'] = df['Delta_Hustle'] * 0.5
    df['Impact_Crowd'] = df['Delta_Crowd'] * 1.5
    
    drivers = ['Impact_Comfort', 'Impact_Refs', 'Impact_Hustle', 'Impact_Crowd']
    labels = {
        'Impact_Comfort': 'Shooting (Comfort)',
        'Impact_Refs': 'Whistle (Refs)',
        'Impact_Hustle': 'Rebounding (Hustle)',
        'Impact_Crowd': 'Turnovers (Crowd)'
    }
    
    # Find max driver
    df['Primary_Driver'] = df[drivers].idxmax(axis=1).map(labels)
    return df

def main():
    import os
    print("\n--- RUNNING PHASE 4: HCA DECOMPOSER ---")
    
    # Pipeline
    df = load_and_prep_data()
    if df is None: return
    
    df = generate_internal_ranks(df)
    df_expanded = get_opponent_stats(df)
    final_df = calculate_hca_deltas(df_expanded)
    final_df = classify_drivers(final_df)
    
    # Cleanup Output
    cols = ['Team', 'OpponentQuad', 'Overall_HCA', 'HCA_Adjusted', 'Primary_Driver', 
            'Home_GameID', 'Away_GameID', 'Delta_Comfort', 'Delta_Refs', 'Delta_Crowd']
    
    output = final_df[cols].round(2)
    output.to_csv(OUTPUT_FILE, index=False)
    
    print(f"\n✅ SUCCESS. Analysis saved to {OUTPUT_FILE}")
    print("\nSAMPLE INSIGHTS:")
    print(output.head(5).to_string())

if __name__ == "__main__":
    main()
