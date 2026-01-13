"""
================================================================================
THE BIBLE V10.1 - TQPR ENHANCED EDITION
================================================================================

Integrates True Quadrant Power Rating (TQPR) analysis into the production model.

NEW FEATURES:
1. Hierarchical Bayesian adjustments using TQPR
2. Paper Tiger detection and penalty
3. Kentucky Problem identification
4. Schedule-aware luck regression
5. TQPR-informed variance estimation
6. Enhanced signal generation with battle-tested flags

THEORETICAL FOUNDATION:
The model now conditions predictions on REALIZED performance vs opponent quality,
not just efficiency ratings derived from full-season aggregate data.

================================================================================
"""

import requests
import pandas as pd
import numpy as np
import os
import sys
from io import StringIO
from datetime import datetime
from scipy import stats
from typing import Tuple, Dict, List, Optional
import warnings

warnings.filterwarnings('ignore')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ==============================================================================
# CONFIGURATION
# ==============================================================================

KP_API_KEY = "18ee6ae93a94ade96fc899611578ef42f57ad96e09845cb585ee3b2aab1444fa"
BASE_HCA_POINTS = 2.6

# Data paths
STYLE_DB_PATH = os.path.join(BASE_DIR, "cbb_style_2025_complete.csv")
QUADRANT_DATA_PATH = os.path.join(BASE_DIR, "team_quadrant_analysis_2026.csv")
ADJUSTED_EFF_PATH = os.path.join(BASE_DIR, "team_adjusted_efficiency_profiles_2026.csv")
TQPR_DATA_PATH = os.path.join(BASE_DIR, "tqpr_full_rankings.csv")

# Location data
HOME_PERF_FILE = os.path.join(BASE_DIR, "team_home_performance_VALIDATED_2026.csv")
ROAD_PERF_FILE = os.path.join(BASE_DIR, "team_road_performance_VALIDATED_2026.csv")
LOCATION_SCALING = 0.35

CONFIDENCE_WEIGHTS = {'HIGH': 1.0, 'MEDIUM': 0.7, 'LOW': 0.3, 'INSUFFICIENT': 0.0}

# ==============================================================================
# SIMULATION PARAMETERS
# ==============================================================================

BASE_VARIANCE_TOTAL = 9.5
BASE_VARIANCE_SPREAD = 11.5
SIM_RUNS = 5000
LUCK_REGRESSION_FACTOR = 0.30
TURNOVER_POINT_VALUE = 1.2
OREB_POSSESSION_RATE = 0.25
SECOND_CHANCE_PPP = 1.05
BLOWOUT_TALENT_THRESHOLD = 25.0
BLOWOUT_MULTIPLIER = 1.15

# Bayesian parameters
BAYESIAN_PRIOR_WEIGHT = 4.0
QUADRANT_CREDIBILITY_THRESHOLD = 3
MAX_QUADRANT_ADJUSTMENT = 6.0
EB_VARIANCE_ALPHA = 0.15

# Thresholds
LOW_MAJOR_RANK_THRESHOLD = 200
LOW_MAJOR_VARIANCE_MULT = 1.4
TOTAL_EDGE_THRESHOLD = 3.0
SPREAD_EDGE_THRESHOLD = 2.0
HIGH_CONFIDENCE_TOTAL = 5.0
HIGH_CONFIDENCE_SPREAD = 4.0

# ==============================================================================
# TQPR PARAMETERS (NEW IN V10.1)
# ==============================================================================

TQPR_WEIGHT_DIVERGENCE = 0.25
TQPR_WEIGHT_PAPER_TIGER = 0.30
TQPR_WEIGHT_KENTUCKY = 0.25
TQPR_WEIGHT_REALIZED_QUAD = 0.20
TQPR_MAX_ADJUSTMENT = 5.0

# ==============================================================================
# NAME TRANSLATION
# ==============================================================================

KENPOM_TRANSLATION = {
    # ACC
    "Boston College Eagles": "Boston College", "California Golden Bears": "California",
    "Clemson Tigers": "Clemson", "Duke Blue Devils": "Duke", "Florida State Seminoles": "Florida St.",
    "Georgia Tech Yellow Jackets": "Georgia Tech", "Louisville Cardinals": "Louisville",
    "Miami Hurricanes": "Miami FL", "NC State Wolfpack": "N.C. State",
    "North Carolina Tar Heels": "North Carolina", "Notre Dame Fighting Irish": "Notre Dame",
    "Pittsburgh Panthers": "Pittsburgh", "SMU Mustangs": "SMU", "Stanford Cardinal": "Stanford",
    "Syracuse Orange": "Syracuse", "Virginia Cavaliers": "Virginia",
    "Virginia Tech Hokies": "Virginia Tech", "Wake Forest Demon Deacons": "Wake Forest",
    # BIG 12
    "Arizona Wildcats": "Arizona", "Arizona State Sun Devils": "Arizona St.",
    "Baylor Bears": "Baylor", "BYU Cougars": "BYU", "UCF Golden Knights": "UCF",
    "Cincinnati Bearcats": "Cincinnati", "Colorado Buffaloes": "Colorado",
    "Houston Cougars": "Houston", "Iowa State Cyclones": "Iowa St.", "Kansas Jayhawks": "Kansas",
    "Kansas State Wildcats": "Kansas St.", "Oklahoma State Cowboys": "Oklahoma St.",
    "TCU Horned Frogs": "TCU", "Texas Tech Red Raiders": "Texas Tech", "Utah Utes": "Utah",
    "West Virginia Mountaineers": "West Virginia",
    # BIG EAST
    "Butler Bulldogs": "Butler", "Connecticut Huskies": "Connecticut", "UConn Huskies": "Connecticut",
    "Creighton Bluejays": "Creighton", "DePaul Blue Demons": "DePaul", "Georgetown Hoyas": "Georgetown",
    "Marquette Golden Eagles": "Marquette", "Providence Friars": "Providence",
    "Seton Hall Pirates": "Seton Hall", "St. John's Red Storm": "St. John's",
    "Villanova Wildcats": "Villanova", "Xavier Musketeers": "Xavier",
    # BIG TEN
    "Illinois Fighting Illini": "Illinois", "Indiana Hoosiers": "Indiana", "Iowa Hawkeyes": "Iowa",
    "Maryland Terrapins": "Maryland", "Michigan Wolverines": "Michigan",
    "Michigan State Spartans": "Michigan St.", "Minnesota Golden Gophers": "Minnesota",
    "Nebraska Cornhuskers": "Nebraska", "Northwestern Wildcats": "Northwestern",
    "Ohio State Buckeyes": "Ohio St.", "Oregon Ducks": "Oregon", "Penn State Nittany Lions": "Penn St.",
    "Purdue Boilermakers": "Purdue", "Rutgers Scarlet Knights": "Rutgers", "UCLA Bruins": "UCLA",
    "USC Trojans": "USC", "Washington Huskies": "Washington", "Wisconsin Badgers": "Wisconsin",
    # SEC
    "Alabama Crimson Tide": "Alabama", "Arkansas Razorbacks": "Arkansas", "Auburn Tigers": "Auburn",
    "Florida Gators": "Florida", "Georgia Bulldogs": "Georgia", "Kentucky Wildcats": "Kentucky",
    "LSU Tigers": "LSU", "Mississippi Rebels": "Ole Miss", "Ole Miss Rebels": "Ole Miss",
    "Mississippi State Bulldogs": "Mississippi St.", "Missouri Tigers": "Missouri",
    "Oklahoma Sooners": "Oklahoma", "South Carolina Gamecocks": "South Carolina",
    "Tennessee Volunteers": "Tennessee", "Texas Longhorns": "Texas", "Texas A&M Aggies": "Texas A&M",
    "Vanderbilt Commodores": "Vanderbilt",
    # Others
    "Gonzaga Bulldogs": "Gonzaga", "Saint Mary's Gaels": "Saint Mary's",
    "VCU Rams": "VCU", "Dayton Flyers": "Dayton", "Saint Louis Billikens": "Saint Louis",
    "Memphis Tigers": "Memphis", "Miami (FL)": "Miami FL", "Miami (OH)": "Miami OH",
    "UConn": "Connecticut", "Pitt": "Pittsburgh",
}

def standardize_name(name):
    if not isinstance(name, str): return str(name)
    return KENPOM_TRANSLATION.get(name.strip(), name.strip())


# ==============================================================================
# TQPR MODULE (NEW IN V10.1)
# ==============================================================================

class TQPRModule:
    """
    True Quadrant Power Rating integration module.
    Provides hierarchical Bayesian adjustments based on realized quadrant performance.
    """
    
    def __init__(self, tqpr_path: str):
        self.enabled = False
        self.df = None
        
        if not os.path.exists(tqpr_path):
            print(f"   ⚠️ TQPR data not found at {tqpr_path}")
            return
        
        try:
            self.df = pd.read_csv(tqpr_path)
            self._preprocess()
            self.enabled = True
            print(f"   ✅ TQPR module loaded: {len(self.df)} teams")
        except Exception as e:
            print(f"   ⚠️ TQPR load error: {e}")
    
    def _preprocess(self):
        """Compute derived fields"""
        numeric = ['TQPR', 'TQPR_Rank', 'Quadrant_Score', 'Paper_Tiger_Score',
                   'Kentucky_Score', 'AdjEM', 'Luck', 'Q1_Games', 'Q2_Games',
                   'Q1_WinPct', 'Q2_WinPct', 'Q1_NetEff', 'Q2_NetEff', 
                   'Q3_NetEff', 'Q4_NetEff']
        
        for col in numeric:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
        
        self.df['Quality_Games'] = self.df.get('Q1_Games', 0).fillna(0) + self.df.get('Q2_Games', 0).fillna(0)
        self.df['KP_Rank'] = self.df['AdjEM'].rank(ascending=False, method='min')
        self.df['Rank_Divergence'] = self.df['KP_Rank'] - self.df['TQPR_Rank']
    
    def get_team(self, name: str) -> Optional[Dict]:
        """Get TQPR data for team"""
        if not self.enabled:
            return None
        
        row = self.df[self.df['Team'] == name]
        if row.empty:
            row = self.df[self.df['Team'].str.contains(name, case=False, na=False)]
        if row.empty or len(row) > 1:
            return None
        
        return row.iloc[0].to_dict()
    
    def compute_adjustment(self, team: str, opp_rank: int) -> Tuple[float, Dict]:
        """
        Compute TQPR-based adjustment for team facing opponent of given rank.
        
        Returns (adjustment, breakdown_dict)
        """
        data = self.get_team(team)
        if data is None:
            return 0.0, {'error': 'Team not found'}
        
        # Determine opponent quadrant
        if opp_rank <= 50: opp_quad = 'Q1'
        elif opp_rank <= 100: opp_quad = 'Q2'
        elif opp_rank <= 200: opp_quad = 'Q3'
        else: opp_quad = 'Q4'
        
        breakdown = {'opp_quad': opp_quad}
        
        # === Component 1: Rank Divergence ===
        rank_div = data.get('Rank_Divergence', 0)
        quality_games = data.get('Quality_Games', 0)
        quality_weight = min(quality_games / 5.0, 1.0)
        div_adj = np.clip(rank_div / 50.0, -2.0, 2.0) * quality_weight
        breakdown['divergence'] = div_adj
        
        # === Component 2: Paper Tiger Penalty ===
        pt_score = data.get('Paper_Tiger_Score', 50)
        if opp_quad in ['Q1', 'Q2'] and pt_score > 70:
            pt_penalty = -((pt_score - 70) / 30) * 2.5
        else:
            pt_penalty = 0.0
        breakdown['paper_tiger'] = pt_penalty
        
        # === Component 3: Kentucky Problem ===
        ky_score = data.get('Kentucky_Score', 0)
        if opp_quad in ['Q1', 'Q2'] and ky_score > 25:
            ky_penalty = -(ky_score / 50) * 2.0
        else:
            ky_penalty = 0.0
        breakdown['kentucky'] = ky_penalty
        
        # === Component 4: Realized Quadrant Performance ===
        quad_games = data.get(f'{opp_quad}_Games', 0)
        quad_neteff = data.get(f'{opp_quad}_NetEff', 0)
        
        if quad_games >= 2 and not pd.isna(quad_neteff):
            shrinkage = quad_games / (quad_games + BAYESIAN_PRIOR_WEIGHT)
            quad_adj = shrinkage * (quad_neteff / 10.0) * 1.5
            quad_adj = np.clip(quad_adj, -2.0, 2.0)
        else:
            quad_adj = 0.0
        breakdown['quadrant_perf'] = quad_adj
        
        # === Composite ===
        total = (
            div_adj * TQPR_WEIGHT_DIVERGENCE +
            pt_penalty * TQPR_WEIGHT_PAPER_TIGER +
            ky_penalty * TQPR_WEIGHT_KENTUCKY +
            quad_adj * TQPR_WEIGHT_REALIZED_QUAD
        )
        total = np.clip(total, -TQPR_MAX_ADJUSTMENT, TQPR_MAX_ADJUSTMENT)
        breakdown['total'] = total
        
        return total, breakdown
    
    def compute_luck_regression(self, team: str, raw_luck: float) -> float:
        """
        Compute luck regression with schedule-aware strength.
        Battle-tested teams get lighter regression (luck is more real).
        """
        data = self.get_team(team)
        
        if data is None:
            return raw_luck * LUCK_REGRESSION_FACTOR
        
        quality_games = data.get('Quality_Games', 0)
        pt_score = data.get('Paper_Tiger_Score', 50)
        
        if quality_games >= 5 and pt_score < 50:
            factor = LUCK_REGRESSION_FACTOR * 0.7  # Lighter regression
        elif quality_games < 3 or pt_score > 75:
            factor = LUCK_REGRESSION_FACTOR * 1.3  # Heavier regression
        else:
            factor = LUCK_REGRESSION_FACTOR
        
        return raw_luck * factor
    
    def estimate_variance_mult(self, team: str, rank: int) -> float:
        """Get variance multiplier based on schedule reliability"""
        data = self.get_team(team)
        
        if data is None:
            return 1.2  # Maximum uncertainty
        
        mult = 1.0
        quality_games = data.get('Quality_Games', 0)
        pt_score = data.get('Paper_Tiger_Score', 50)
        
        if quality_games < 3:
            mult *= 1.25
        elif quality_games < 5:
            mult *= 1.1
        
        if pt_score > 75:
            mult *= 1.15
        
        if rank > 150 and pt_score > 60:
            mult *= 1.2
        
        return mult
    
    def get_flags(self, team: str, role: str = 'T') -> List[str]:
        """Generate signal flags for team"""
        flags = []
        data = self.get_team(team)
        
        if data is None:
            return flags
        
        pt_score = data.get('Paper_Tiger_Score', 0)
        ky_score = data.get('Kentucky_Score', 0)
        luck = data.get('Luck', 0)
        quality_games = data.get('Quality_Games', 0)
        
        if pt_score > 70:
            flags.append(f"🐯{role}:PaperTiger({pt_score:.0f})")
        
        if ky_score > 30:
            flags.append(f"🔻{role}:KYProb({ky_score:.0f})")
        
        if abs(luck) > 0.08:
            direction = "Lucky" if luck > 0 else "Unlucky"
            flags.append(f"🍀{role}:{direction}({luck:+.3f})")
        
        if quality_games >= 6:
            flags.append(f"⚔️{role}:Battle-tested({quality_games})")
        elif quality_games < 2:
            flags.append(f"⚠️{role}:Untested")
        
        return flags


# ==============================================================================
# DATA LOADING
# ==============================================================================

def get_kenpom_data(endpoint, year=2026):
    url = f"https://kenpom.com/api.php?endpoint={endpoint}&y={year}"
    headers = {"Authorization": f"Bearer {KP_API_KEY}", "User-Agent": "TheBibleModel/10.1"}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        if endpoint == "misc-stats":
            return pd.read_csv(StringIO(response.text))
        return pd.DataFrame(response.json())
    except Exception as e:
        print(f"⚠️ API Error ({endpoint}): {e}")
        return None

def load_quadrant_data():
    if not os.path.exists(QUADRANT_DATA_PATH):
        return None
    df = pd.read_csv(QUADRANT_DATA_PATH)
    df['Team'] = df['Team'].apply(standardize_name)
    return df

def load_efficiency_profiles():
    if not os.path.exists(ADJUSTED_EFF_PATH):
        return None
    df = pd.read_csv(ADJUSTED_EFF_PATH)
    df['Team'] = df['Team'].apply(standardize_name)
    return df

def load_validated_location_data():
    try:
        if not os.path.exists(HOME_PERF_FILE) or not os.path.exists(ROAD_PERF_FILE):
            print("   ℹ️ Validated location data not found. Using standard HCA.")
            return None, None
        h_df = pd.read_csv(HOME_PERF_FILE)
        r_df = pd.read_csv(ROAD_PERF_FILE)
        h_df['Team'] = h_df['Team'].apply(standardize_name)
        r_df['Team'] = r_df['Team'].apply(standardize_name)
        print(f"   ✅ Validated location data: {len(h_df)} home, {len(r_df)} road profiles")
        return h_df, r_df
    except Exception as e:
        print(f"   ⚠️ Could not load location data: {e}")
        return None, None

def build_team_database():
    print("🏗️ Building Enhanced Team Database (V10.1 TQPR)...")
    
    ratings = get_kenpom_data("ratings")
    factors = get_kenpom_data("four-factors")
    if ratings is None or factors is None:
        return None, None, None, None, None, None, None
    
    if 'Rank' not in ratings.columns:
        ratings['Rank'] = ratings.index + 1
    ratings = ratings.rename(columns={'AdjO': 'AdjOE', 'AdjD': 'AdjDE', 'AdjT': 'AdjTempo', 'SOS_AdjEM': 'SOS'})
    ratings['TeamName'] = ratings['TeamName'].apply(standardize_name)
    factors['TeamName'] = factors['TeamName'].apply(standardize_name)
    
    stats = ratings[['TeamName', 'Rank', 'AdjEM', 'AdjOE', 'AdjDE', 'AdjTempo', 'Luck', 'SOS']].rename(columns={
        'AdjOE': 'Off_Eff', 'AdjDE': 'Def_Eff', 'AdjTempo': 'Tempo'
    })
    stats = stats.merge(factors[['TeamName', 'OR_Pct', 'DOR_Pct', 'TO_Pct', 'DTO_Pct', 'FT_Rate', 'DFT_Rate']], 
                       on='TeamName', how='left')
    
    style = pd.DataFrame()
    if os.path.exists(STYLE_DB_PATH):
        try:
            style = pd.read_csv(STYLE_DB_PATH)
            style['Team'] = style['Team'].apply(standardize_name)
        except:
            pass
    
    quad = load_quadrant_data()
    eff = load_efficiency_profiles()
    h_perf, r_perf = load_validated_location_data()
    
    # NEW: Load TQPR module
    tqpr = TQPRModule(TQPR_DATA_PATH)
    
    print(f"   ✓ Core stats: {len(stats)} teams")
    return stats, style, quad, eff, h_perf, r_perf, tqpr


# ==============================================================================
# ADJUSTMENT LOGIC
# ==============================================================================

def calculate_validated_location_adjustment(home_team, visitor_team, visitor_rank, home_df, road_df):
    if home_df is None or road_df is None:
        return 0.0, 0.0, "", "NO_DATA"
    
    if visitor_rank <= 30: v_quad = 'Q1'
    elif visitor_rank <= 75: v_quad = 'Q2'
    elif visitor_rank <= 160: v_quad = 'Q3'
    else: v_quad = 'Q4'
    
    h_data = home_df[home_df['Team'] == home_team]
    if h_data.empty:
        h_adj = 0.0; h_conf = 'NO_DATA'; h_reason = f"{home_team}: No home data"
    else:
        h_data = h_data.iloc[0]
        q_shrunk = f'{v_quad}_NetEff_Shrunk'
        q_conf = f'{v_quad}_Confidence'
        if pd.notna(h_data.get(q_shrunk)) and h_data.get(q_conf) not in ['LOW', 'INSUFFICIENT', None]:
            h_adj = h_data[q_shrunk] * LOCATION_SCALING * CONFIDENCE_WEIGHTS.get(h_data[q_conf], 0.3)
            h_adj = np.clip(h_adj, -4.0, 4.0)
            h_conf = h_data[q_conf]
            h_reason = f"{home_team} Home vs {v_quad}: {h_data[q_shrunk]:+.1f}"
        else:
            h_adj = h_data['Overall_NetEff'] * LOCATION_SCALING * 0.5
            h_adj = np.clip(h_adj, -2.5, 2.5)
            h_conf = 'LOW'
            h_reason = f"{home_team} Home (Overall): {h_data['Overall_NetEff']:+.1f}"
    
    v_data = road_df[road_df['Team'] == visitor_team]
    if v_data.empty:
        v_adj = 0.0; v_conf = 'NO_DATA'; v_reason = f"{visitor_team}: No road data"
    else:
        v_data = v_data.iloc[0]
        games = v_data['Total_Games']
        weight = 1.0 if games >= 7 else (0.7 if games >= 5 else 0.5)
        v_conf = 'HIGH' if games >= 7 else ('MEDIUM' if games >= 5 else 'LOW')
        v_adj = v_data['Overall_NetEff'] * LOCATION_SCALING * weight
        v_adj = np.clip(v_adj, -3.0, 3.0)
        v_reason = f"{visitor_team} Road: {v_data['Overall_NetEff']:+.1f}"
    
    reasoning = f"Loc: {h_reason} | {v_reason}"
    conf_map = {'NO_DATA': 0, 'LOW': 1, 'MEDIUM': 2, 'HIGH': 3}
    final_conf = [k for k, v in conf_map.items() if v == min(conf_map.get(h_conf, 0), conf_map.get(v_conf, 0))][0]
    
    return h_adj, v_adj, reasoning, final_conf

def compute_bayesian_quadrant_adjustment(team, opp_rank, quad_data):
    if quad_data is None or quad_data.empty:
        return 0.0, "", ""
    row = quad_data[quad_data['Team'] == team]
    if row.empty:
        return 0.0, "", ""
    row = row.iloc[0]
    
    if opp_rank <= 50: quad = 'Q1'
    elif opp_rank <= 100: quad = 'Q2'
    elif opp_rank <= 200: quad = 'Q3'
    else: quad = 'Q4'
    
    try:
        games = row[f'{quad}_Games']
        net_eff = row[f'{quad}_NetEff']
        base_eff = row.get('AdjNetEff', 0)
        consistency = row.get('ConsistencyScore', 15.0)
    except:
        return 0.0, "", ""
    
    if pd.isna(net_eff) or games < QUADRANT_CREDIBILITY_THRESHOLD:
        return 0.0, "", ""
    
    shrinkage = games / (games + BAYESIAN_PRIOR_WEIGHT)
    delta = net_eff - base_eff
    if net_eff > 0 and delta < 0:
        delta = 0.0
    adj = delta * shrinkage
    
    if consistency < 12.0:
        adj *= 1.2
    elif consistency > 20.0:
        adj *= 0.7
    
    adj = np.clip(adj, -MAX_QUADRANT_ADJUSTMENT, MAX_QUADRANT_ADJUSTMENT)
    flag = f"BAYES: {adj:+.1f} vs {quad}" if abs(adj) > 1.5 else ""
    
    return adj, flag, ""

def calculate_four_factors_edge(v, h):
    turnover = ((h.get('DTO_Pct', 18) - v.get('TO_Pct', 18)) - 
                (v.get('DTO_Pct', 18) - h.get('TO_Pct', 18))) * TURNOVER_POINT_VALUE
    reb = ((h.get('OR_Pct', 28) - v.get('DOR_Pct', 28)) - 
           (v.get('OR_Pct', 28) - h.get('DOR_Pct', 28))) * OREB_POSSESSION_RATE * SECOND_CHANCE_PPP
    return turnover + reb

def estimate_variance(v_name, h_name, v_rank, h_rank, gap, eff, base, tqpr):
    var = base
    
    if v_rank > LOW_MAJOR_RANK_THRESHOLD or h_rank > LOW_MAJOR_RANK_THRESHOLD:
        var *= LOW_MAJOR_VARIANCE_MULT
    
    if eff is not None:
        v_d = eff[eff['Team'] == v_name]
        h_d = eff[eff['Team'] == h_name]
        if not v_d.empty and not h_d.empty:
            emp = np.mean([v_d.iloc[0].get('OffEffStd', 15), h_d.iloc[0].get('DefEffStd', 15)])
            var = (1 - EB_VARIANCE_ALPHA) * var + EB_VARIANCE_ALPHA * emp
    
    # NEW: TQPR variance adjustment
    if tqpr.enabled:
        v_mult = tqpr.estimate_variance_mult(v_name, v_rank)
        h_mult = tqpr.estimate_variance_mult(h_name, h_rank)
        var *= (v_mult + h_mult) / 2.0
    
    if abs(gap) > BLOWOUT_TALENT_THRESHOLD:
        var *= 0.85
    
    return var


# ==============================================================================
# CORE SIMULATION ENGINE
# ==============================================================================

def run_simulation(v_name, h_name, stats, style, quad, eff, h_perf, r_perf, tqpr, spread=None, total=None):
    v = stats[stats['TeamName'] == v_name]
    h = stats[stats['TeamName'] == h_name]
    if v.empty or h.empty:
        return {"error": "Team not found", "Visitor": v_name, "Home": h_name}
    
    v = v.iloc[0].to_dict()
    h = h.iloc[0].to_dict()
    
    # === 1. Base Efficiency with TQPR-aware luck regression ===
    if tqpr.enabled:
        v_luck_adj = tqpr.compute_luck_regression(v_name, v.get('Luck', 0))
        h_luck_adj = tqpr.compute_luck_regression(h_name, h.get('Luck', 0))
    else:
        v_luck_adj = v.get('Luck', 0) * LUCK_REGRESSION_FACTOR
        h_luck_adj = h.get('Luck', 0) * LUCK_REGRESSION_FACTOR
    
    v_off = v['Off_Eff'] - v_luck_adj
    h_off = h['Off_Eff'] - h_luck_adj
    v_def = v['Def_Eff']
    h_def = h['Def_Eff']
    
    # === 2. TQPR Hierarchical Adjustment (NEW) ===
    tqpr_flags = []
    if tqpr.enabled:
        v_tqpr_adj, v_tqpr_bd = tqpr.compute_adjustment(v_name, h['Rank'])
        h_tqpr_adj, h_tqpr_bd = tqpr.compute_adjustment(h_name, v['Rank'])
        
        # Apply TQPR adjustments (split between offense and defense)
        v_off += v_tqpr_adj / 2
        v_def -= v_tqpr_adj / 2
        h_off += h_tqpr_adj / 2
        h_def -= h_tqpr_adj / 2
        
        # Collect flags
        tqpr_flags.extend(tqpr.get_flags(v_name, 'V'))
        tqpr_flags.extend(tqpr.get_flags(h_name, 'H'))
    else:
        v_tqpr_adj = h_tqpr_adj = 0.0
    
    # === 3. Bayesian Quadrant Adjustment (existing) ===
    v_adj, v_flag, _ = compute_bayesian_quadrant_adjustment(v_name, h['Rank'], quad)
    h_adj, h_flag, _ = compute_bayesian_quadrant_adjustment(h_name, v['Rank'], quad)
    v_off += v_adj / 2
    v_def -= v_adj / 2
    h_off += h_adj / 2
    h_def -= h_adj / 2
    
    # === 4. Location Adjustment ===
    h_loc, v_loc, loc_reason, loc_conf = calculate_validated_location_adjustment(
        h_name, v_name, v['Rank'], h_perf, r_perf
    )
    h_off += h_loc
    v_off += v_loc
    
    # === 5. Tempo & Scoring ===
    tempo = (v['Tempo'] * h['Tempo']) / 68.5
    v_ppp = (v_off * h_def) / 106.0
    h_ppp = (h_off * v_def) / 106.0
    h_ppp += calculate_four_factors_edge(v, h) / tempo
    
    if loc_conf == 'NO_DATA':
        h_ppp += BASE_HCA_POINTS / tempo
    
    # === 6. Result ===
    v_score = (v_ppp * tempo) / 100.0
    h_score = (h_ppp * tempo) / 100.0
    margin = h_score - v_score
    proj_total = v_score + h_score
    
    # === 7. Blowout Adjustment ===
    gap = abs(v['AdjEM'] - h['AdjEM'])
    if gap > BLOWOUT_TALENT_THRESHOLD:
        margin *= BLOWOUT_MULTIPLIER
        if h_score > v_score:
            h_score = v_score + margin
        else:
            v_score = h_score - margin
        proj_total = v_score + h_score
    
    # === 8. Monte Carlo with TQPR-informed variance ===
    np.random.seed(42)
    s_var = estimate_variance(v_name, h_name, v['Rank'], h['Rank'], gap, eff, BASE_VARIANCE_SPREAD, tqpr)
    sims = np.random.normal(margin, s_var, SIM_RUNS)
    win_prob = np.mean(sims > 0) * 100
    
    # === 9. Betting Signals ===
    signals = []
    if spread is not None:
        market_margin = -spread
        edge = abs(margin - market_margin)
        if edge >= SPREAD_EDGE_THRESHOLD:
            if margin > market_margin:
                side = h_name
                bet_line = spread
            else:
                side = v_name
                bet_line = -spread
            conf = "HIGH" if edge >= HIGH_CONFIDENCE_SPREAD else "MEDIUM"
            signals.append(f"SPREAD: {side} {bet_line:+.1f} (Edge: {edge:.1f}, {conf})")
    
    if total is not None:
        edge = abs(proj_total - total)
        if edge >= TOTAL_EDGE_THRESHOLD:
            side = "OVER" if proj_total > total else "UNDER"
            conf = "HIGH" if edge >= HIGH_CONFIDENCE_TOTAL else "MEDIUM"
            signals.append(f"TOTAL: {side} {total} (Edge: {edge:.1f}, {conf})")
    
    # === 10. Build Flags ===
    flags = []
    if v_flag:
        flags.append(f"V: {v_flag}")
    if h_flag:
        flags.append(f"H: {h_flag}")
    if loc_conf != 'NO_DATA':
        flags.append(f"PhD_Loc: {h_loc - v_loc:+.1f} Net")
    if abs(v_tqpr_adj) > 0.5:
        flags.append(f"V_TQPR: {v_tqpr_adj:+.1f}")
    if abs(h_tqpr_adj) > 0.5:
        flags.append(f"H_TQPR: {h_tqpr_adj:+.1f}")
    
    return {
        'Visitor': v_name,
        'Home': h_name,
        'V_Score': round(v_score, 1),
        'H_Score': round(h_score, 1),
        'Predicted_Spread': round(margin, 1),
        'Predicted_Total': round(proj_total, 1),
        'Home_Win_Prob': round(win_prob, 1),
        'Signals': "; ".join(signals),
        'Analysis_Flags': " | ".join(flags),
        'TQPR_Flags': " | ".join(tqpr_flags),
        'PhD_Reasoning': loc_reason if loc_conf != 'NO_DATA' else "Standard HCA Used",
        'Spread_Variance': round(s_var, 2),
    }


# ==============================================================================
# INTERFACE
# ==============================================================================

def run_single_game(stats, style, quad, eff, h_perf, r_perf, tqpr):
    v = input("Visitor: ")
    h = input("Home: ")
    s = input("Spread (opt, Home Line e.g. -5.5): ")
    t = input("Total (opt): ")
    
    res = run_simulation(
        v, h, stats, style, quad, eff, h_perf, r_perf, tqpr,
        float(s) if s else None,
        float(t) if t else None
    )
    
    if "error" in res:
        print(f"❌ Error: {res['error']}")
        return
    
    disp_line = -res['Predicted_Spread']
    
    print(f"\n{'='*80}")
    print(f"📊 THE BIBLE V10.1 PREDICTION (TQPR Enhanced)")
    print(f"{'='*80}")
    print(f"   Score: {res['Visitor']} {res['V_Score']} - {res['Home']} {res['H_Score']}")
    print(f"   Line:  {res['Home']} {disp_line:+.1f}")
    print(f"   Total: {res['Predicted_Total']:.1f}")
    print(f"   Win%:  {res['Home_Win_Prob']}%")
    print(f"   Variance: {res['Spread_Variance']}")
    
    print(f"\n🧠 INTELLIGENCE:")
    print(f"   Analysis: {res['Analysis_Flags']}")
    if res['TQPR_Flags']:
        print(f"   TQPR: {res['TQPR_Flags']}")
    print(f"   Location: {res['PhD_Reasoning']}")
    
    if res['Signals']:
        print(f"\n💰 SIGNALS: {res['Signals']}")
    
    print(f"{'='*80}\n")


if __name__ == "__main__":
    # Load all data including TQPR
    data = build_team_database()
    
    if data[0] is not None:
        stats, style, quad, eff, h_perf, r_perf, tqpr = data
        
        while True:
            print("\nTHE BIBLE V10.1 (TQPR Enhanced)")
            print("1. Predict Single Game")
            print("2. Exit")
            choice = input("Select: ")
            
            if choice == "1":
                run_single_game(stats, style, quad, eff, h_perf, r_perf, tqpr)
            elif choice == "2":
                break
