"""
kenpom_quadrant_v3_FIXED.py
==============================================================
Enhanced True Quadrant Power Rating (TQPR) system.
FIX: Automatically detects OneDrive Desktop paths.
"""

# ==============================================================================
# CONFIGURATION
# ==============================================================================
KP_API_KEY = "18ee6ae93a94ade96fc899611578ef42f57ad96e09845cb585ee3b2aab1444fa"
INPUT_FILE_PATH = 'team_quadrant_analysis_fixed.csv' 

import pandas as pd
import numpy as np
import requests
import re
import warnings
import os
warnings.filterwarnings('ignore')

# ==============================================================================
# SMART PATH FINDER (The Fix)
# ==============================================================================
def get_desktop_path():
    """Smartly finds the real Desktop, even if inside OneDrive."""
    user_home = os.path.expanduser("~")
    
    # Check OneDrive first (Most common on modern Windows)
    onedrive_desktop = os.path.join(user_home, "OneDrive", "Desktop")
    if os.path.exists(onedrive_desktop):
        return onedrive_desktop
        
    # Check Standard Desktop
    standard_desktop = os.path.join(user_home, "Desktop")
    if os.path.exists(standard_desktop):
        return standard_desktop
        
    # Fallback to current folder if neither exists
    return os.getcwd()

# Define Output Paths using the smart finder
desktop = get_desktop_path()
OUTPUT_RANKINGS = os.path.join(desktop, 'tqpr_full_rankings.csv')
OUTPUT_REPORT = os.path.join(desktop, 'tqpr_top50_report.csv')

# ==============================================================================
# TEAM NAME STANDARDIZATION
# ==============================================================================

def standardize_team_name(name: str) -> str:
    """Standardize team names for matching across data sources"""
    if pd.isna(name): return ""
    name = str(name).strip()
    name = re.sub(r'^\d+\s*', '', name) # Remove rank numbers
    name = name.rstrip('*') # Remove asterisks
    
    # Common mascot suffixes to remove
    mascots = [
        'Cyclones', 'Billikens', 'Bulldogs', 'Wolverines', 'Panthers',
        'Wildcats', 'Cougars', 'Cardinals', 'Hawkeyes', 'Cavaliers',
        'Blue Devils', 'Fighting Illini', 'Boilermakers', 'Mountaineers',
        'Wolfpack', 'Hoosiers', 'Huskies', 'Tigers', 'Redbirds', 
        'Lobos', 'Bulls', 'Horned Frogs', 'Tar Heels', 'Cornhuskers',
        'Gaels', 'Bruins', 'Volunteers', 'Spartans', 'Cowboys', 
        'Sooners', 'Shockers', 'Longhorns', 'Razorbacks', 'Buckeyes', 
        'Buffaloes', 'Hokies', 'Rams', 'Gators', 'Seminoles', 'Commodores',
        'Rebels', 'Musketeers', 'Bearcats', 'Golden Eagles', 'Aztecs'
    ]
    for mascot in mascots:
        pattern = rf'\s+{mascot}$'
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    
    # Specific mappings
    mappings = {
        'North Carolina': 'North Carolina', 'UNC': 'North Carolina', 
        'N.C. State': 'NC State', 'Connecticut': 'UConn',
        'Miami (FL)': 'Miami', 'Miami FL': 'Miami', 'Miami (OH)': 'Miami OH',
        'Central Florida': 'UCF', 'Southern California': 'USC',
        'Southern Methodist': 'SMU', 'Nevada Las Vegas': 'UNLV',
        'Louisiana State': 'LSU', 'Virginia Commonwealth': 'VCU',
        'Texas Christian': 'TCU', 'Brigham Young': 'BYU',
        'Mississippi': 'Ole Miss', 'Pittsburgh': 'Pitt',
        'Saint Louis': 'St. Louis', "Saint Mary's": "St. Mary's",
        "St. John's": "St. John's", 'Stephen F. Austin': 'SFA',
        'Florida International': 'FIU', 'Texas-Arlington': 'UT Arlington',
        'Texas-San Antonio': 'UTSA', 'Texas-El Paso': 'UTEP',
    }
    name = name.strip()
    return mappings.get(name, name)

# ==============================================================================
# KENPOM API CLIENT (BIBLE V10 IMPLEMENTATION)
# ==============================================================================

class KenPomAPI:
    """Fetches data using the proven Bible V10 auth method"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        # Use the exact endpoint structure from your working script
        self.base_url = "https://kenpom.com/api.php"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "User-Agent": "TQPR-Analyzer/2.0"
        }

    def get_ratings(self) -> pd.DataFrame:
        """Fetch ratings using the Bearer Token method"""
        print(f"  → Contacting KenPom API (Bearer Auth)...")
        
        # We request the 'ratings' endpoint exactly like the Bible script
        params = {'endpoint': 'ratings', 'y': 2026}
        
        try:
            response = requests.get(self.base_url, headers=self.headers, params=params, timeout=15)
            
            if response.status_code == 200:
                print(f"  ✓ Connection successful (Status 200)")
                data = response.json()
                df = pd.DataFrame(data)
                return self._clean_api_data(df)
            else:
                print(f"  ✗ API Error: Status {response.status_code}")
                # Print first 100 chars of response to diagnose
                print(f"    Response: {response.text[:100]}")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"  ✗ Connection failed: {e}")
            return pd.DataFrame()

    def _clean_api_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map Bible V10 column names to TQPR standards"""
        
        col_map = {
            'TeamName': 'Team',
            'AdjO': 'AdjO', 
            'AdjD': 'AdjD', 
            'AdjT': 'AdjTempo',
            'Luck': 'Luck',
            'SOSAdjEM': 'SOS_AdjEM'
        }
        
        df = df.rename(columns=col_map)
        
        # Calculate Rank if missing (Bible script does this manually)
        if 'KP_Rank' not in df.columns:
            # Sort by AdjEM just in case, though API usually sends sorted
            if 'AdjEM' in df.columns:
                df['AdjEM'] = pd.to_numeric(df['AdjEM'], errors='coerce')
                df = df.sort_values('AdjEM', ascending=False).reset_index(drop=True)
                df['KP_Rank'] = df.index + 1
            else:
                # Fallback: just use current order
                df['KP_Rank'] = df.index + 1

        # Ensure numeric conversion
        numeric_cols = ['KP_Rank', 'AdjEM', 'AdjO', 'AdjD', 'AdjTempo', 'Luck', 'SOS_AdjEM']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if 'Team' in df.columns:
            df['Team_Std'] = df['Team'].apply(standardize_team_name)
            
        return df

# ==============================================================================
# QUADRANT ANALYZER
# ==============================================================================

class QuadrantAnalyzer:
    def __init__(self, df: pd.DataFrame):
        self.df = self._prepare_data(df)
    
    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['Team_Std'] = df['Team'].apply(standardize_team_name)
        
        # Parse records
        for q in ['Q1', 'Q2', 'Q3', 'Q4']:
            if f'{q}_Record' in df.columns:
                df[f'{q}_Wins'] = df[f'{q}_Record'].str.extract(r'^(\d+)')[0].astype(float).fillna(0)
                df[f'{q}_Losses'] = df[f'{q}_Record'].str.extract(r'-(\d+)')[0].astype(float).fillna(0)
        
        # Deduplicate
        if df['Team_Std'].duplicated().any():
            numeric_cols = [c for c in df.select_dtypes(include=np.number).columns if c != 'AdjRank']
            agg_dict = {c: 'sum' for c in numeric_cols}
            agg_dict['Team'] = 'first'
            eff_cols = [c for c in df.columns if 'NetEff' in c]
            agg_dict.update({c: 'mean' for c in eff_cols})
            
            df = df.groupby('Team_Std', as_index=False).agg(agg_dict)
            
            # Recalc percentages
            for q in ['Q1', 'Q2', 'Q3', 'Q4']:
                df[f'{q}_WinPct'] = df[f'{q}_Wins'] / df[f'{q}_Games'].replace(0, 1)
                
        return df
    
    def calculate_scores(self) -> pd.DataFrame:
        df = self.df.copy()
        
        # 1. Win Rate Score
        weights = {'Q1': 4.0, 'Q2': 2.5, 'Q3': 1.5, 'Q4': 0.5}
        win_score = sum(df[f'{q}_WinPct'] * w * df[f'{q}_Games'] for q, w in weights.items())
        total_weight = sum(w * df[f'{q}_Games'] for q, w in weights.items())
        df['WinRate_Score'] = win_score / total_weight.replace(0, 1)
        
        # 2. Net Efficiency Score
        net_weights = {'Q1': 1.5, 'Q2': 1.2, 'Q3': 0.8, 'Q4': 0.4}
        net_score = sum(df[f'{q}_NetEff'] * w * np.minimum(df[f'{q}_Games'], 5) for q, w in net_weights.items())
        total_net_weight = sum(w * np.minimum(df[f'{q}_Games'], 5) for q, w in net_weights.items())
        df['NetEff_Score'] = net_score / total_net_weight.replace(0, 1)
        
        # 3. Composite
        total_games = df['TotalGames'].replace(0, 1)
        df['Quadrant_Score'] = (
            df['WinRate_Score'] * 45 +
            df['NetEff_Score'] * 0.4 +
            ((df['Q1_Games'] + df['Q2_Games']) / total_games * 20) + 
            (df['Q1_Wins'] * 8) + 
            (df['Q4_Losses'] * -6)
        )
        return df

    def identify_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        # Paper Tiger Logic
        df['Paper_Tiger_Score'] = (
            (df['Q4_Games'] / df['TotalGames'].replace(0, 1) * 35) + 
            (np.maximum(0, 3 - (df['Q1_Games'] + df['Q2_Games'])) * 8) - 
            (df['Q1_Wins'] * 12)
        )
        pt_range = df['Paper_Tiger_Score'].max() - df['Paper_Tiger_Score'].min()
        if pt_range > 0:
            df['Paper_Tiger_Score'] = (df['Paper_Tiger_Score'] - df['Paper_Tiger_Score'].min()) / pt_range * 100
        df['Paper_Tiger_Flag'] = df['Paper_Tiger_Score'] > 60
        
        # Kentucky Problem Logic
        q_wins = df['Q1_Wins'] + df['Q2_Wins']
        q_games = df['Q1_Games'] + df['Q2_Games']
        c_wins = df['Q3_Wins'] + df['Q4_Wins']
        c_games = df['Q3_Games'] + df['Q4_Games']
        
        df['Quality_WinPct'] = q_wins / q_games.replace(0, 1)
        df['Cupcake_WinPct'] = c_wins / c_games.replace(0, 1)
        
        df['Kentucky_Score'] = np.where(
            q_games >= 2,
            (df['Cupcake_WinPct'] - df['Quality_WinPct']) * 50,
            0
        )
        df['Kentucky_Flag'] = (df['Kentucky_Score'] > 30) & (q_games >= 2)
        
        return df

# ==============================================================================
# TRUE QUADRANT POWER RATING (TQPR)
# ==============================================================================

class TQPR:
    def __init__(self, quadrant_df: pd.DataFrame, kenpom_df: pd.DataFrame = None):
        self.quad_analyzer = QuadrantAnalyzer(quadrant_df)
        self.kenpom_df = kenpom_df
    
    def calculate(self) -> pd.DataFrame:
        df = self.quad_analyzer.calculate_scores()
        df = self.quad_analyzer.identify_outliers(df)
        
        if self.kenpom_df is not None and not self.kenpom_df.empty:
            # Merge logic
            df = pd.merge(df, self.kenpom_df, on='Team_Std', how='left', suffixes=('', '_KP'))
            df['AdjEM'] = df['AdjEM'].fillna(0)
            df['Luck'] = df['Luck'].fillna(0)
            
            # Normalization
            adjEM_norm = ((df['AdjEM'] + 25) * 1.8).clip(0, 100)
            quad_norm = df['Quadrant_Score'].clip(0, 100)
            
            # Final TQPR Formula
            df['TQPR'] = (
                adjEM_norm * 0.35 +
                quad_norm * 0.35 +
                (df['TotalWins']/df['TotalGames'].replace(0,1) * 100 * 0.15) +
                (-df['Luck'] * 4 * 0.08)
            )
        else:
            df['TQPR'] = (df['Quadrant_Score'].clip(0, 100) * 0.7 + 
                          (df['TotalWins']/df['TotalGames'].replace(0,1) * 100 * 0.3))
            
        df['TQPR_Rank'] = df['TQPR'].rank(ascending=False, method='min').astype(int)
        return df.sort_values('TQPR', ascending=False)

# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print("=" * 60)
    print("   TRUE QUADRANT POWER RATING (TQPR) SYSTEM")
    print("   Target: Auto-Detected Desktop")
    print("=" * 60)
    
    # 1. Load Local Data
    if os.path.exists(INPUT_FILE_PATH):
        print(f"\nLoading quadrant data: {INPUT_FILE_PATH}")
        quad_df = pd.read_csv(INPUT_FILE_PATH)
    else:
        print(f"\nERROR: Could not find '{INPUT_FILE_PATH}'")
        print("Please ensure your CSV file is in the same directory.")
        return

    # 2. Fetch KenPom Data
    kp = KenPomAPI(KP_API_KEY)
    kenpom_df = kp.get_ratings()
    
    if not kenpom_df.empty:
        print(f"  ✓ Successfully fetched KenPom ratings for {len(kenpom_df)} teams")
    else:
        print("  ! API fetch returned no data. Running Quadrant-only mode.")

    # 3. Run Model
    print("\nCalculating TQPR...")
    tqpr = TQPR(quad_df, kenpom_df)
    full_df = tqpr.calculate()
    
    # 4. Display Results
    cols = ['TQPR_Rank', 'Team', 'TQPR', 'Quadrant_Score', 'AdjEM', 'Luck']
    avail_cols = [c for c in cols if c in full_df.columns]
    
    print("\nTOP 25 TQPR RANKINGS:")
    print(full_df[avail_cols].head(25).to_string(index=False))
    
    # 5. Save Results
    print(f"\nAttempting to save to: {OUTPUT_RANKINGS}")
    full_df.to_csv(OUTPUT_RANKINGS, index=False)
    print(f"✓ Success! File saved.")

if __name__ == '__main__':
    main()
