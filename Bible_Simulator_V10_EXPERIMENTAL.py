import requests
import pandas as pd
import numpy as np
import os
from io import StringIO
from datetime import datetime, timedelta
from scipy import stats
from typing import Tuple, Dict, List, Optional
import warnings
warnings.filterwarnings('ignore')
# Define the base directory relative to this file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ==============================================================================
#   THE BIBLE V10.0 - EXPERIMENTAL HYBRID EDITION
#   
#   Research Version: Integrating Bayesian Opponent Quality Adjustments
#   
#   Core Philosophy:
#   - Preserve V9.2's proven calibration (58.6% ATS)
#   - Add Advanced simulator's contextual intelligence
#   - Apply rigorous statistical theory (Bayesian shrinkage, hierarchical modeling)
#   - Track performance independently for validation
#   
#   New Features:
#   1. Hierarchical Bayesian opponent quality adjustments
#   2. Empirical Bayes variance estimation
#   3. Situation-aware confidence intervals
#   4. Multi-level performance tracking
# ==============================================================================

# --- CONFIGURATION ---
KP_API_KEY = "18ee6ae93a94ade96fc899611578ef42f57ad96e09845cb585ee3b2aab1444fa"
BASE_HCA_POINTS = 2.6  # Proven optimal from V9.2
STYLE_DB_PATH = os.path.join(BASE_DIR, "cbb_style_2025_complete.csv")
QUADRANT_DATA_PATH = os.path.join(BASE_DIR, "team_quadrant_analysis_2026.csv")
ADJUSTED_EFF_PATH = os.path.join(BASE_DIR, "team_adjusted_efficiency_profiles_2026.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "Output")
TRACKING_DIR = os.path.join(BASE_DIR, "..", "Data")

# --- NEW V10 EXPERIMENTAL TRACKING ---
EXPERIMENT_LOG = os.path.join(BASE_DIR, "v10_experiment_log.csv")
QUADRANT_DATA_PATH = os.path.join(BASE_DIR, "team_quadrant_analysis_2026.csv")
ADJUSTED_EFF_PATH = os.path.join(BASE_DIR, "team_adjusted_efficiency_profiles_2026.csv")

# --- MASTER TRANSLATION DICTIONARY ---
KENPOM_TRANSLATION = {
    "Alabama State": "Alabama St.", "Alcorn State": "Alcorn St.", "Appalachian State": "Appalachian St.",
    "Arizona State": "Arizona St.", "Arkansas State": "Arkansas St.", "Ball State": "Ball St.",
    "Boise State": "Boise St.", "Central Connecticut State": "Central Connecticut", "CSU Fullerton": "Cal St. Fullerton",
    "Chicago State": "Chicago St.", "Cleveland State": "Cleveland St.", "Colorado State": "Colorado St.",
    "Coppin State": "Coppin St.", "Delaware State": "Delaware St.", "Florida State": "Florida St.",
    "Fresno State": "Fresno St.", "Georgia State": "Georgia St.", "Grambling State": "Grambling St.",
    "Idaho State": "Idaho St.", "Illinois State": "Illinois St.", "Indiana State": "Indiana St.",
    "Iowa State": "Iowa St.", "Jackson State": "Jackson St.", "Jacksonville State": "Jacksonville St.",
    "Kansas State": "Kansas St.", "Kennesaw State": "Kennesaw St.", "Kent State": "Kent St.",
    "Long Beach State": "Long Beach St.", "Louisiana State": "LSU", "McNeese State": "McNeese St.",
    "Michigan State": "Michigan St.", "Mississippi State": "Mississippi St.",
    "Mississippi Valley State": "Miss Valley St.", "Missouri State": "Missouri St.",
    "Montana State": "Montana St.", "Morehead State": "Morehead St.", "Morgan State": "Morgan St.",
    "Murray State": "Murray St.", "NC State": "N.C. State", "North Carolina State": "N.C. State",
    "New Mexico State": "New Mexico St.", "Nicholls State": "Nicholls St.", "Norfolk State": "Norfolk St.",
    "North Dakota State": "North Dakota St.", "Northwestern State": "Northwestern St.",
    "Ohio State": "Ohio St.", "Oklahoma State": "Oklahoma St.", "Oregon State": "Oregon St.",
    "Penn State": "Penn St.", "Portland State": "Portland St.", "Sacramento State": "Sacramento St.",
    "Sam Houston State": "Sam Houston St.", "San Diego State": "San Diego St.", "Ole Miss": "Mississippi",
    "San Jose State": "San Jose St.", "South Carolina State": "S.C. State", "Arkansas Little Rock": "Little Rock",
    "South Dakota State": "South Dakota St.", "Southeast Missouri State": "Southeast Missouri",
    "Tarleton State": "Tarleton St.", "Tennessee State": "Tennessee St.", "Texas State": "Texas St.",
    "Utah State": "Utah St.", "Washington State": "Washington St.", "Weber State": "Weber St.",
    "Wichita State": "Wichita St.", "Wright State": "Wright St.", "Youngstown State": "Youngstown St.",
    "North Carolina Central": "N.C. Central", "UMBC": "Maryland BC", "Detroit Mercy": "Detroit",
    "Detroit": "Detroit", "IUPUI": "IU Indy", "Long Island University": "LIU", "LIU": "LIU",
    "Saint Peter's": "Saint Peter's", "St. Peter's": "Saint Peter's", "Saint Mary's": "Saint Mary's",
    "St. Mary's": "Saint Mary's", "Southern Illinois": "Southern Ill.", "N.C. St.": "NC State",
    "California Baptist": "Cal Baptist", "Texas A&M-Corpus Christi": "Texas A&M Corpus Chris",
    "Texas A&M Corpus Christi": "Texas A&M Corpus Chris", "UMass Lowell": "Mass Lowell",
    "UT Rio Grande Valley": "UT Rio Grande Val", "Stephen F. Austin": "Stephen F. Austin",
    "Middle Tennessee": "Middle Tenn.", "Florida International": "FIU", "Louisiana Monroe": "UL Monroe",
    "UL Monroe": "UL Monroe", "Omaha": "Nebraska Omaha", "Nebraska Omaha": "Nebraska Omaha",
    "Little Rock": "Little Rock", "Gardner-Webb": "Gardner Webb", "UIC": "UIC", "UL Monroe": "Louisiana Monroe",
    "The Citadel": "The Citadel", "VMI": "VMI", "Queens": "Queens", "Queens University": "Queens",
    "St. Thomas (MN)": "St. Thomas", "Albany": "Albany", "Loyola Chicago": "Loyola Chicago",
    "Loyola Marymount": "Loyola Marymount", "Ole Miss": "Mississippi", "Mississippi": "Mississippi",
    "UConn": "Connecticut", "Pitt": "Pittsburgh", "UAB": "UAB", "UCF": "UCF", "VCU": "VCU",
    "SMU": "SMU", "TCU": "TCU", "LSU": "LSU", "BYU": "BYU","Saint Louis": "Saint Louis",
    "Saint Joseph's": "Saint Joseph's","UT Rio Grande Val": "UT Rio Grande Valley", "U.T. Rio Grande Valley": "UTRGV",
    "SIU Edwardsville": "SIUE","Tennessee-Martin": "Tennessee Martin", "UT Rio Grande Valley": "UTRGV",
    "Iowa State Cyclones": "Iowa St.", "Georgia Bulldogs": "Georgia", "Saint Louis Billikens": "Saint Louis",
    "Michigan Wolverines": "Michigan", "Arizona Wildcats": "Arizona", "Iowa Hawkeyes": "Iowa",
    "Gonzaga Bulldogs": "Gonzaga", "Louisville Cardinals": "Louisville", "Loyola Marymount Lions": "Loyola Marymount",
} # --- V10 MASCOT FIXES: Paste this RIGHT AFTER the KENPOM_TRANSLATION dictionary ---
KENPOM_TRANSLATION.update({
    "North Carolina Tar Heels": "North Carolina",
    "Duke Blue Devils": "Duke",
    "Saint Mary's Gaels": "Saint Mary's",
    "Seattle U Redhawks": "Seattle",
    "Gonzaga Bulldogs": "Gonzaga",
    "Kansas Jayhawks": "Kansas",
    "Arizona Wildcats": "Arizona",
    "Houston Cougars": "Houston",
    "UConn Huskies": "Connecticut",
    "Purdue Boilermakers": "Purdue",
    "Tennessee Volunteers": "Tennessee",
    "Kentucky Wildcats": "Kentucky",
    "Auburn Tigers": "Auburn",
    "Illinois Fighting Illini": "Illinois",
    "Creighton Bluejays": "Creighton",
})

def standardize_name(name):
    """
    Look up the name in the master dictionary. 
    If found, return the fixed version. If not, return the original.
    """
    return KENPOM_TRANSLATION.get(name, name)

# --- VENUE-SPECIFIC HOME COURT ADVANTAGE (V9.2 Calibrated) ---
VENUE_HCA = {
    'Duke': 4.2, 'Kentucky': 3.9, 'Kansas': 3.7, 'Gonzaga': 3.6, 'Villanova': 3.5,
    'Syracuse': 3.5, 'Louisville': 3.4, 'Michigan St.': 3.3, 'Wisconsin': 3.3,
    'North Carolina': 3.2, 'Arizona': 3.2, 'Virginia': 3.1, 'Purdue': 3.1,
    'Iowa St.': 3.0, 'Butler': 3.0, 'Creighton': 2.9, 'San Diego St.': 2.9,
    'Northwestern': 1.5, 'DePaul': 1.6, 'Georgia Tech': 1.7, 'Boston College': 1.8,
    'Rutgers': 2.0, 'Nebraska': 2.1, 'Wake Forest': 2.2,
}

# --- V9.2 PROVEN PARAMETERS ---
BASE_VARIANCE_TOTAL = 9.5
BASE_VARIANCE_SPREAD = 11.5
SIM_RUNS = 5000
LUCK_REGRESSION_FACTOR = 0.30
TURNOVER_POINT_VALUE = 1.2
OREB_POSSESSION_RATE = 0.25
SECOND_CHANCE_PPP = 1.05
SOS_VARIANCE_FACTOR = 0.05
BLOWOUT_TALENT_THRESHOLD = 25.0
BLOWOUT_MULTIPLIER = 1.15
LOW_MAJOR_RANK_THRESHOLD = 200
LOW_MAJOR_VARIANCE_MULT = 1.4

# --- V10 EXPERIMENTAL PARAMETERS (Statistically Derived) ---
# Bayesian Hierarchical Parameters
BAYESIAN_PRIOR_WEIGHT = 4.0  # Equivalent to 4 games of prior information
QUADRANT_CREDIBILITY_THRESHOLD = 3  # Minimum games for quadrant adjustment
MAX_QUADRANT_ADJUSTMENT = 6.0  # Cap at ±6 points (conservative vs Advanced's ±10)

# Empirical Bayes Variance Adjustment
EB_VARIANCE_ALPHA = 0.15  # Weight for empirical variance component
CONSISTENCY_BONUS = 0.85  # Reduction factor for highly consistent teams

# Confidence Calibration (for tracking model certainty)
HIGH_CONFIDENCE_STD_THRESHOLD = 8.0  # Games with low uncertainty
LOW_CONFIDENCE_STD_THRESHOLD = 13.0  # Games with high uncertainty

# --- BETTING THRESHOLDS (V9.2 Proven) ---
TOTAL_EDGE_THRESHOLD = 3.0
SPREAD_EDGE_THRESHOLD = 2.0
HIGH_CONFIDENCE_TOTAL = 5.0
HIGH_CONFIDENCE_SPREAD = 4.0

# ======================================================
# SECTION 1: DATA LOADING & PREPARATION
# ======================================================

def get_kenpom_data(endpoint, year=2026):
    """Fetch data from KenPom API."""
    url = f"https://kenpom.com/api.php?endpoint={endpoint}&y={year}"
    headers = {"Authorization": f"Bearer {KP_API_KEY}", "User-Agent": "TheBibleModel/10.0-EXPERIMENTAL"}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        if endpoint == "misc-stats":
            return pd.read_csv(StringIO(response.text))
        return pd.DataFrame(response.json())
    except Exception as e:
        print(f"⚠️  API Error ({endpoint}): {e}")
        return None

def get_mapped_name(name, known_teams=None):
    """Translates Market/Display Name -> KenPom Name."""
    name = name.strip()
    if name in KENPOM_TRANSLATION:
        return KENPOM_TRANSLATION[name]
    if " State" in name:
        return name.replace(" State", " St.")
    if name.startswith("Saint "):
        return name.replace("Saint ", "St. ")
    return name

def load_quadrant_data() -> Optional[pd.DataFrame]:
    """Load quadrant analysis data for Bayesian adjustments."""
    if not os.path.exists(QUADRANT_DATA_PATH):
        print("ℹ️  Quadrant data not found. Proceeding without contextual adjustments.")
        return None
    
    quad_df = pd.read_csv(QUADRANT_DATA_PATH)
    quad_df['Team'] = quad_df['Team'].apply(get_mapped_name)
    return quad_df

def load_efficiency_profiles() -> Optional[pd.DataFrame]:
    """Load adjusted efficiency profiles for variance estimation."""
    if not os.path.exists(ADJUSTED_EFF_PATH):
        print("ℹ️  Efficiency profiles not found. Using base variance.")
        return None
    
    eff_df = pd.read_csv(ADJUSTED_EFF_PATH)
    eff_df['Team'] = eff_df['Team'].apply(get_mapped_name)
    return eff_df

def build_team_database():
    """Enhanced database builder with experimental data integration."""
    print("🏗️  Building Enhanced Team Database (V10)...")
    df_ratings = get_kenpom_data("ratings")
    df_four_factors = get_kenpom_data("four-factors")
    
    if df_ratings is None or df_four_factors is None:
        return None, None, None, None
    
    # Standardize API columns (V9.2 proven method)
    if 'Rank' not in df_ratings.columns:
        df_ratings['Rank'] = df_ratings.index + 1
        
    rename_map = {
        'AdjO': 'AdjOE', 'AdjD': 'AdjDE', 'AdjT': 'AdjTempo', 'SOS_AdjEM': 'SOS'
    }
    df_ratings = df_ratings.rename(columns=rename_map)
    
    needed_cols = ['TeamName', 'Rank', 'AdjEM', 'AdjOE', 'AdjDE', 'AdjTempo', 'Luck', 'SOS']
    missing = [c for c in needed_cols if c not in df_ratings.columns]
    if missing:
        print(f"⚠️  Critical Error: API missing columns: {missing}")
        return None, None, None, None

    team_stats = df_ratings[needed_cols].rename(columns={
        'AdjOE': 'Off_Eff', 'AdjDE': 'Def_Eff', 'AdjTempo': 'Tempo'
    })
    
    team_stats = team_stats.merge(
        df_four_factors[['TeamName', 'OR_Pct', 'DOR_Pct', 'TO_Pct', 'DTO_Pct', 'FT_Rate', 'DFT_Rate']], 
        on='TeamName', how='left'
    )
    
    # Load style database (V9.2)
    style_db = pd.DataFrame()
    if os.path.exists(STYLE_DB_PATH):
        try:
            style_db = pd.read_csv(STYLE_DB_PATH)
            print(f"   ✓ Loaded style database: {len(style_db)} matchup profiles")
        except Exception as e:
            print(f"   ⚠️ Style DB error: {e}")
    
    # Load experimental data
    quad_data = load_quadrant_data()
    eff_profiles = load_efficiency_profiles()
    
    print(f"   ✓ Core stats: {len(team_stats)} teams")
    if quad_data is not None:
        print(f"   ✓ Quadrant analysis: {len(quad_data)} teams")
    if eff_profiles is not None:
        print(f"   ✓ Efficiency profiles: {len(eff_profiles)} teams")
    
    return team_stats, style_db, quad_data, eff_profiles

# ======================================================
# SECTION 2: BAYESIAN STATISTICAL ENHANCEMENTS
# ======================================================

def determine_opponent_quality_tier(rank: int) -> str:
    """
    Classify opponent into quality tiers based on rank.
    Uses finer gradations than simple quadrants for better signal.
    """
    if rank <= 25:
        return 'ELITE'
    elif rank <= 75:
        return 'QUALITY'
    elif rank <= 150:
        return 'AVERAGE'
    elif rank <= 250:
        return 'BELOW_AVG'
    else:
        return 'LOW_MAJOR'

def compute_bayesian_quadrant_adjustment(
    team_name: str,
    opponent_rank: int,
    quad_data: Optional[pd.DataFrame],
    confidence_level: float = 0.80
) -> Tuple[float, str, str]:
    
    # 1. Check if data exists (Skip for V9.2 baseline runs)
    if quad_data is None or quad_data.empty:
        return 0.0, "", ""
    
    # 2. Check if team exists in the V10 database
    team_row = quad_data[quad_data['Team'] == team_name]
    
    # 3. AUTO-FIX: If Exact Match fails, try Fuzzy Match (Mascot handling)
    if team_row.empty:
        # Search for teams that START with the KenPom name
        # e.g., "Tulsa" matches "Tulsa Golden Hurricane"
        matches = [t for t in quad_data['Team'].unique() if str(t).startswith(team_name)]
        
        if matches:
            # print(f"   [DEBUG] Auto-Matched '{team_name}' -> '{matches[0]}'")
            team_row = quad_data[quad_data['Team'] == matches[0]]
        else:
            # Only print error if Auto-Fix failed
            # print(f"   [DEBUG] ❌ Name Mismatch: '{team_name}' not found in V10 DB.")
            return 0.0, "", ""
    
    # 4. If found, proceed with calculation
    team_row = team_row.iloc[0]
    
    
    
    if opponent_rank <= 50: quad = 'Q1'
    elif opponent_rank <= 100: quad = 'Q2'
    elif opponent_rank <= 200: quad = 'Q3'
    else: quad = 'Q4'
    
    try:
        games_in_quad = team_row[f'{quad}_Games']
        net_eff_in_quad = team_row[f'{quad}_NetEff']
        confidence_rating = team_row.get(f'{quad}_Confidence', 'NONE')
        base_net_eff = team_row.get('AdjNetEff', 0)
        consistency_score = team_row.get('ConsistencyScore', 15.0)
    except Exception as e:
        print(f"   [DEBUG] Data Error for {team_name}: {e}")
        return 0.0, "", ""
    
    if pd.isna(net_eff_in_quad) or games_in_quad < QUADRANT_CREDIBILITY_THRESHOLD:
        return 0.0, "", ""
    
    shrinkage_weight = games_in_quad / (games_in_quad + BAYESIAN_PRIOR_WEIGHT)
    performance_delta = net_eff_in_quad - base_net_eff
    
    # --- FIX: ELITE TEAM PROTECTION ---
    # If a team has a positive Net Efficiency against Q1 opponents, 
    # NEVER apply a negative penalty, even if their overall rating is higher.
    # This protects elite teams (like Iowa St) from being punished for "only" winning by 20.
    if net_eff_in_quad > 0 and performance_delta < 0:
        performance_delta = 0.0
        
    bayesian_adjustment = performance_delta * shrinkage_weight
    
    consistency_factor = 1.0
    if consistency_score < 12.0: consistency_factor = 1.2
    elif consistency_score > 20.0: consistency_factor = 0.7
    bayesian_adjustment *= consistency_factor
    
    confidence_multiplier = {'NONE': 0.0, 'LOW': 0.5, 'MEDIUM': 0.85, 'HIGH': 1.0}.get(confidence_rating, 0.5)
    bayesian_adjustment *= confidence_multiplier
    
    bayesian_adjustment = np.clip(bayesian_adjustment, -MAX_QUADRANT_ADJUSTMENT, MAX_QUADRANT_ADJUSTMENT)
    
    flag = ""
    reasoning = ""
    if abs(bayesian_adjustment) > 1.5:
        direction = "better" if bayesian_adjustment > 0 else "worse"
        flag = f"BAYES_ADJ: {bayesian_adjustment:+.1f} vs {quad}"
        reasoning = (f"{team_name} performs {abs(bayesian_adjustment):.1f} pts {direction} vs {quad}")



    return bayesian_adjustment, flag, reasoning


    
    # ==========================================
    # BAYESIAN SHRINKAGE CALCULATION
    # ==========================================
    # Prior: Team's overall efficiency
    # Likelihood: Team's efficiency in this quadrant
    # Posterior: Weighted combination based on sample size
    
    # Shrinkage weight: n / (n + k), where k is prior weight
    # Higher k = more conservative (shrink more toward prior)
    shrinkage_weight = games_in_quad / (games_in_quad + BAYESIAN_PRIOR_WEIGHT)
    
    # Performance delta (how different is this quadrant?)
    performance_delta = net_eff_in_quad - base_net_eff
    
    # Apply shrinkage
    bayesian_adjustment = performance_delta * shrinkage_weight
    
    # ==========================================
    # CONSISTENCY ADJUSTMENT
    # ==========================================
    # Teams with low variance (high consistency) get more credibility
    # ConsistencyScore is std dev of margins - lower means more consistent
    consistency_factor = 1.0
    if consistency_score < 12.0:  # Very consistent
        consistency_factor = 1.2
    elif consistency_score > 20.0:  # Volatile
        consistency_factor = 0.7
    
    bayesian_adjustment *= consistency_factor
    
    # ==========================================
    # CONFIDENCE WEIGHTING
    # ==========================================
    # The quadrant data includes confidence ratings (NONE, LOW, MEDIUM, HIGH)
    # based on sample size. Use this as additional evidence.
    confidence_multiplier = {
        'NONE': 0.0,
        'LOW': 0.5,
        'MEDIUM': 0.85,
        'HIGH': 1.0
    }.get(confidence_rating, 0.5)
    
    bayesian_adjustment *= confidence_multiplier
    
    # Apply safety cap (more conservative than Advanced's ±10)
    bayesian_adjustment = np.clip(bayesian_adjustment, -MAX_QUADRANT_ADJUSTMENT, MAX_QUADRANT_ADJUSTMENT)
    
    # ==========================================
    # GENERATE EXPLANATORY FLAG
    # ==========================================
    flag = ""
    reasoning = ""
    
    if abs(bayesian_adjustment) > 1.5:  # Only flag meaningful adjustments
        direction = "better" if bayesian_adjustment > 0 else "worse"
        
        flag = f"BAYES_ADJ: {bayesian_adjustment:+.1f} vs {quad}"
        
        reasoning = (
            f"{team_name} performs {abs(bayesian_adjustment):.1f} pts {direction} "
            f"vs {quad} opponents ({games_in_quad}G, {confidence_rating} confidence). "
            f"Delta: {performance_delta:+.1f} → Bayesian: {bayesian_adjustment:+.1f}"
        )
    
    return bayesian_adjustment, flag, reasoning

def estimate_game_specific_variance(
    team1_name: str,
    team2_name: str,
    team1_rank: int,
    team2_rank: int,
    talent_gap: float,
    eff_profiles: Optional[pd.DataFrame],
    base_variance: float
) -> float:
    """
    Empirical Bayes variance estimation for game-specific uncertainty.
    
    Theory:
    - Base variance from V9.2 (proven: 11.5 for spread, 9.5 for total)
    - Adjust upward for: low-major chaos, talent mismatches, volatile teams
    - Adjust downward for: consistent teams, evenly matched games
    - Uses team-specific variance from historical performance
    
    Statistical Foundation:
    - Combines prior (base variance) with likelihood (empirical variance)
    - Weighted by confidence in empirical estimates
    """
    variance = base_variance
    
    # V9.2 Proven Adjustments
    # Low-major chaos
    if team1_rank > LOW_MAJOR_RANK_THRESHOLD or team2_rank > LOW_MAJOR_RANK_THRESHOLD:
        variance *= LOW_MAJOR_VARIANCE_MULT
    
    # Empirical Bayes Component (NEW)
    if eff_profiles is not None:
        team1_data = eff_profiles[eff_profiles['Team'] == team1_name]
        team2_data = eff_profiles[eff_profiles['Team'] == team2_name]
        
        if not team1_data.empty and not team2_data.empty:
            # Get empirical standard deviations from actual game performance
            team1_std = team1_data.iloc[0].get('OffEffStd', 15.0)
            team2_std = team2_data.iloc[0].get('DefEffStd', 15.0)
            
            # Average empirical variance
            empirical_variance = np.mean([team1_std, team2_std])
            
            # Blend with base variance using Empirical Bayes weighting
            variance = (1 - EB_VARIANCE_ALPHA) * variance + EB_VARIANCE_ALPHA * empirical_variance
    
    # Blowout potential (from V9.2)
    if abs(talent_gap) > BLOWOUT_TALENT_THRESHOLD:
        # Large talent gaps reduce variance (more predictable outcomes)
        variance *= 0.85
    
    return variance

# ======================================================
# SECTION 3: CORE PREDICTION ENGINE (V9.2 Enhanced)
# ======================================================

def calculate_four_factors_edge(v_stats, h_stats, style_db):
    """
    V9.2 proven Four Factors calculation.
    Returns point adjustment based on matchup advantages.
    """
    v_to_pct = v_stats.get('TO_Pct', 18.0) if not pd.isna(v_stats.get('TO_Pct')) else 18.0
    v_dto_pct = v_stats.get('DTO_Pct', 18.0) if not pd.isna(v_stats.get('DTO_Pct')) else 18.0
    h_to_pct = h_stats.get('TO_Pct', 18.0) if not pd.isna(h_stats.get('TO_Pct')) else 18.0
    h_dto_pct = h_stats.get('DTO_Pct', 18.0) if not pd.isna(h_stats.get('DTO_Pct')) else 18.0
    
    turnover_battle = ((h_dto_pct - v_to_pct) - (v_dto_pct - h_to_pct)) * TURNOVER_POINT_VALUE
    
    v_or = v_stats.get('OR_Pct', 28.0) if not pd.isna(v_stats.get('OR_Pct')) else 28.0
    v_dor = v_stats.get('DOR_Pct', 28.0) if not pd.isna(v_stats.get('DOR_Pct')) else 28.0
    h_or = h_stats.get('OR_Pct', 28.0) if not pd.isna(h_stats.get('OR_Pct')) else 28.0
    h_dor = h_stats.get('DOR_Pct', 28.0) if not pd.isna(h_stats.get('DOR_Pct')) else 28.0
    
    reb_battle = ((h_or - v_dor) - (v_or - h_dor)) * OREB_POSSESSION_RATE * SECOND_CHANCE_PPP
    
    return turnover_battle + reb_battle

def run_simulation(
    visitor_name: str,
    home_name: str,
    team_stats: pd.DataFrame,
    style_db: pd.DataFrame,
    quad_data: Optional[pd.DataFrame],
    eff_profiles: Optional[pd.DataFrame],
    market_spread: Optional[float] = None,
    market_total: Optional[float] = None
) -> Dict:
    """
    Enhanced V9.2 simulation with Bayesian contextual adjustments.
    """
    visitor_name = get_mapped_name(visitor_name)
    home_name = get_mapped_name(home_name)
    
    v_stats = team_stats[team_stats['TeamName'] == visitor_name]
    h_stats = team_stats[team_stats['TeamName'] == home_name]
    
    if v_stats.empty or h_stats.empty:
        return {"error": "Team not found", "visitor": visitor_name, "home": home_name}
    
    v_stats = v_stats.iloc[0].to_dict()
    h_stats = h_stats.iloc[0].to_dict()
    
    # ==========================================
    # STEP 1: BASE EFFICIENCY CALCULATION (V9.2)
    # ==========================================
    v_off_eff = v_stats['Off_Eff']
    v_def_eff = v_stats['Def_Eff']
    h_off_eff = h_stats['Off_Eff']
    h_def_eff = h_stats['Def_Eff']
    
    # Luck regression (V9.2)
    v_luck = v_stats.get('Luck', 0)
    h_luck = h_stats.get('Luck', 0)
    v_off_eff -= v_luck * LUCK_REGRESSION_FACTOR
    h_off_eff -= h_luck * LUCK_REGRESSION_FACTOR
    
    # ==========================================
    # STEP 2: BAYESIAN OPPONENT QUALITY ADJUSTMENT (NEW)
    # ==========================================
    v_bayes_adj, v_bayes_flag, v_reasoning = compute_bayesian_quadrant_adjustment(
        visitor_name, h_stats['Rank'], quad_data
    )
    h_bayes_adj, h_bayes_flag, h_reasoning = compute_bayesian_quadrant_adjustment(
        home_name, v_stats['Rank'], quad_data
    )
    
    # Apply Bayesian adjustments to efficiency
    # Split adjustment 50/50 between offense and defense (following Advanced's approach)
    v_off_eff += v_bayes_adj / 2
    v_def_eff -= v_bayes_adj / 2
    h_off_eff += h_bayes_adj / 2
    h_def_eff -= h_bayes_adj / 2
    
    # ==========================================
    # STEP 3: TEMPO PROJECTION (V9.2)
    # ==========================================
    v_tempo = v_stats['Tempo']
    h_tempo = h_stats['Tempo']
    nat_avg_tempo = 68.5
    proj_tempo = (v_tempo * h_tempo) / nat_avg_tempo
    
    # ==========================================
    # STEP 4: POSSESSION-LEVEL SCORING (V9.2)
    # ==========================================
    nat_avg_eff = 106.0
    v_ppp = (v_off_eff * h_def_eff) / nat_avg_eff
    h_ppp = (h_off_eff * v_def_eff) / nat_avg_eff
    
    # ==========================================
    # STEP 5: FOUR FACTORS EDGE (V9.2)
    # ==========================================
    four_factors_edge = calculate_four_factors_edge(v_stats, h_stats, style_db)
    h_ppp += four_factors_edge / proj_tempo
    
    # ==========================================
    # STEP 6: HOME COURT ADVANTAGE (V9.2 Calibrated)
    # ==========================================
    hca = VENUE_HCA.get(home_name, BASE_HCA_POINTS)
    hca_per_possession = hca / proj_tempo
    h_ppp += hca_per_possession
    
    # ==========================================
    # STEP 7: EXPECTED SCORING
    # ==========================================
    mean_v_score = (v_ppp * proj_tempo) / 100.0
    mean_h_score = (h_ppp * proj_tempo) / 100.0
    predicted_margin = mean_h_score - mean_v_score
    predicted_total = mean_v_score + mean_h_score
    
    # ==========================================
    # STEP 8: BLOWOUT DETECTION (V9.2)
    # ==========================================
    talent_gap = abs(v_stats['AdjEM'] - h_stats['AdjEM'])
    blowout_flag = ""
    if talent_gap > BLOWOUT_TALENT_THRESHOLD:
        predicted_margin *= BLOWOUT_MULTIPLIER
        if mean_h_score > mean_v_score:
            mean_h_score = mean_v_score + predicted_margin
        else:
            mean_v_score = mean_h_score - predicted_margin
        predicted_total = mean_v_score + mean_h_score
        blowout_flag = f"BLOWOUT_MODE ({talent_gap:.1f} gap)"
    
    # ==========================================
    # STEP 9: VARIANCE ESTIMATION (Enhanced)
    # ==========================================
    spread_variance = estimate_game_specific_variance(
        visitor_name, home_name,
        v_stats['Rank'], h_stats['Rank'],
        talent_gap, eff_profiles,
        BASE_VARIANCE_SPREAD
    )
    
    total_variance = estimate_game_specific_variance(
        visitor_name, home_name,
        v_stats['Rank'], h_stats['Rank'],
        talent_gap, eff_profiles,
        BASE_VARIANCE_TOTAL
    )
    
    # ==========================================
    # STEP 10: MONTE CARLO SIMULATION (V9.2)
    # ==========================================
    np.random.seed(42)  # Reproducibility for testing
    
    v_scores = np.random.normal(mean_v_score, total_variance, SIM_RUNS)
    h_scores = np.random.normal(mean_h_score, total_variance, SIM_RUNS)
    margins = h_scores - v_scores
    totals = v_scores + h_scores
    
    # Win probability
    home_win_prob = np.mean(margins > 0)
    
    # Confidence intervals (NEW - for experimental tracking)
    margin_ci_80 = np.percentile(margins, [10, 90])
    total_ci_80 = np.percentile(totals, [10, 90])
    
    # ==========================================
    # STEP 11: BETTING EDGE DETECTION (V9.2)
    # ==========================================
    signals = []
    
    if market_spread is not None:
        spread_edge = abs(predicted_margin - market_spread)
        if spread_edge >= SPREAD_EDGE_THRESHOLD:
            bet_side = home_name if predicted_margin > market_spread else visitor_name
            confidence = "HIGH" if spread_edge >= HIGH_CONFIDENCE_SPREAD else "MEDIUM"
            
            # Kelly Criterion (V9.2)
            win_prob = home_win_prob if bet_side == home_name else (1 - home_win_prob)
            kelly = max(0, (win_prob * 2.1 - 1) * 100)
            kelly_size = f"{min(kelly, 3.0):.1f}%"
            
            signals.append({
                'Type': 'SPREAD',
                'Bet': f"{bet_side} {market_spread:+.1f}",
                'Edge': spread_edge,
                'Confidence': confidence,
                'Kelly_Size': kelly_size,
                'Reasoning': f"Model: {predicted_margin:+.1f} vs Line: {market_spread:+.1f} ({spread_edge:.1f}pt edge)"
            })
    
    if market_total is not None:
        total_edge = abs(predicted_total - market_total)
        if total_edge >= TOTAL_EDGE_THRESHOLD:
            bet_side = "OVER" if predicted_total > market_total else "UNDER"
            confidence = "HIGH" if total_edge >= HIGH_CONFIDENCE_TOTAL else "MEDIUM"
            
            win_prob = np.mean(totals > market_total) if bet_side == "OVER" else np.mean(totals < market_total)
            kelly = max(0, (win_prob * 2.1 - 1) * 100)
            kelly_size = f"{min(kelly, 3.0):.1f}%"
            
            signals.append({
                'Type': 'TOTAL',
                'Bet': f"{bet_side} {market_total}",
                'Edge': total_edge,
                'Confidence': confidence,
                'Kelly_Size': kelly_size,
                'Reasoning': f"Model: {predicted_total:.1f} vs Line: {market_total} ({total_edge:.1f}pt edge)"
            })
    
    # ==========================================
    # STEP 12: COMPILE FLAGS & METADATA
    # ==========================================
    flags = []
    if blowout_flag:
        flags.append(blowout_flag)
    if v_bayes_flag:
        flags.append(f"{visitor_name[:15]}: {v_bayes_flag}")
    if h_bayes_flag:
        flags.append(f"{home_name[:15]}: {h_bayes_flag}")
    
    # Confidence classification (NEW - for experimental analysis)
    avg_variance = (spread_variance + total_variance) / 2
    if avg_variance < HIGH_CONFIDENCE_STD_THRESHOLD:
        confidence_class = "HIGH_CONFIDENCE"
    elif avg_variance > LOW_CONFIDENCE_STD_THRESHOLD:
        confidence_class = "LOW_CONFIDENCE"
    else:
        confidence_class = "MEDIUM_CONFIDENCE"
    
    flags.append(f"Uncertainty: {confidence_class}")
    
    # ==========================================
    # STEP 13: RETURN COMPREHENSIVE RESULTS
    # ==========================================
    return {
        'Visitor': visitor_name,
        'Home': home_name,
        'V_Score': round(mean_v_score, 1),
        'H_Score': round(mean_h_score, 1),
        'Predicted_Spread': round(predicted_margin, 1),
        'Predicted_Total': round(predicted_total, 1),
        'Home_Win_Prob': round(home_win_prob * 100, 1),
        'Signals': signals,
        'Analysis_Flags': ' | '.join(flags),
        'Projected_Tempo': round(proj_tempo, 1),
        'Spread_Variance': round(spread_variance, 2),
        'Total_Variance': round(total_variance, 2),
        'Talent_Gap': round(talent_gap, 1),
        'Confidence_Class': confidence_class,
        'Margin_CI_80': [round(x, 1) for x in margin_ci_80],
        'Total_CI_80': [round(x, 1) for x in total_ci_80],
        'V_Rank': v_stats['Rank'],
        'H_Rank': h_stats['Rank'],
        # Experimental tracking
        'V_Bayes_Adj': round(v_bayes_adj, 2),
        'H_Bayes_Adj': round(h_bayes_adj, 2),
        'V_Bayes_Reasoning': v_reasoning,
        'H_Bayes_Reasoning': h_reasoning,
    }

# ======================================================
# SECTION 4: EXPERIMENTAL TRACKING & LOGGING
# ======================================================

def log_prediction_for_tracking(prediction: Dict, actual_result: Optional[Dict] = None):
    """
    Log predictions to CSV for independent performance tracking.
    
    This allows us to compare V10 vs V9.2 performance over time.
    """
    if not os.path.exists(TRACKING_DIR):
        os.makedirs(TRACKING_DIR)
    
    log_entry = {
        'Timestamp': datetime.now().isoformat(),
        'Visitor': prediction['Visitor'],
        'Home': prediction['Home'],
        'Predicted_Spread': prediction['Predicted_Spread'],
        'Predicted_Total': prediction['Predicted_Total'],
        'V_Bayes_Adj': prediction.get('V_Bayes_Adj', 0),
        'H_Bayes_Adj': prediction.get('H_Bayes_Adj', 0),
        'Confidence_Class': prediction.get('Confidence_Class', 'UNKNOWN'),
        'Spread_Variance': prediction.get('Spread_Variance', 0),
        'Total_Variance': prediction.get('Total_Variance', 0),
    }
    
    if actual_result:
        log_entry.update({
            'Actual_V_Score': actual_result.get('v_score'),
            'Actual_H_Score': actual_result.get('h_score'),
            'Actual_Margin': actual_result.get('h_score', 0) - actual_result.get('v_score', 0),
            'Actual_Total': actual_result.get('v_score', 0) + actual_result.get('h_score', 0),
            'Spread_Error': abs(prediction['Predicted_Spread'] - (actual_result.get('h_score', 0) - actual_result.get('v_score', 0))),
            'Total_Error': abs(prediction['Predicted_Total'] - (actual_result.get('v_score', 0) + actual_result.get('h_score', 0))),
        })
    
    # Append to CSV
    log_df = pd.DataFrame([log_entry])
    if os.path.exists(EXPERIMENT_LOG):
        log_df.to_csv(EXPERIMENT_LOG, mode='a', header=False, index=False)
    else:
        log_df.to_csv(EXPERIMENT_LOG, index=False)

def generate_performance_report():
    """
    Generate statistical analysis comparing V10 experimental adjustments to baseline.
    """
    if not os.path.exists(EXPERIMENT_LOG):
        print("No experimental data logged yet.")
        return
    
    df = pd.read_csv(EXPERIMENT_LOG)
    df = df[df['Actual_Margin'].notna()]  # Only completed games
    
    if len(df) == 0:
        print("No completed games to analyze.")
        return
    
    print("\n" + "="*70)
    print("📊 V10 EXPERIMENTAL PERFORMANCE REPORT")
    print("="*70)
    
    # Overall accuracy
    print(f"\n📈 OVERALL METRICS (n={len(df)} games)")
    print(f"   Spread MAE:  {df['Spread_Error'].mean():.2f} points")
    print(f"   Total MAE:   {df['Total_Error'].mean():.2f} points")
    
    # Breakdown by confidence class
    print(f"\n🎯 PERFORMANCE BY CONFIDENCE CLASS:")
    for conf_class in ['HIGH_CONFIDENCE', 'MEDIUM_CONFIDENCE', 'LOW_CONFIDENCE']:
        subset = df[df['Confidence_Class'] == conf_class]
        if len(subset) > 0:
            print(f"   {conf_class}: {len(subset)} games, Spread MAE: {subset['Spread_Error'].mean():.2f}")
    
    # Bayesian adjustment impact
    print(f"\n🔬 BAYESIAN ADJUSTMENT IMPACT:")
    games_with_adj = df[(df['V_Bayes_Adj'].abs() > 1.0) | (df['H_Bayes_Adj'].abs() > 1.0)]
    games_without_adj = df[(df['V_Bayes_Adj'].abs() <= 1.0) & (df['H_Bayes_Adj'].abs() <= 1.0)]
    
    if len(games_with_adj) > 0:
        print(f"   With Bayesian Adj (n={len(games_with_adj)}): Spread MAE {games_with_adj['Spread_Error'].mean():.2f}")
    if len(games_without_adj) > 0:
        print(f"   Without Adj (n={len(games_without_adj)}):     Spread MAE {games_without_adj['Spread_Error'].mean():.2f}")
    
    print("="*70 + "\n")

# ======================================================
# SECTION 5: TESTING & COMPARISON INTERFACE
# ======================================================

def run_comparison_test(visitor: str, home: str,
                        market_spread: Optional[float] = None,
                        market_total: Optional[float] = None):
    """
    Run both V9.2 baseline and V10 experimental predictions side-by-side.
    """
    # 1. STANDARDIZE NAMES IMMEDIATELY (The Fix)
    # This prevents KeyErrors by converting dropdown names (e.g. "N.C. St.") 
    # to data names (e.g. "NC State") before looking them up.
    visitor = standardize_name(visitor)
    home = standardize_name(home)

    team_stats, style_db, quad_data, eff_profiles = build_team_database()

    if team_stats is None:
        return

    print("\n" + "="*80)
    print(f"🧪 EXPERIMENTAL COMPARISON: {visitor} @ {home}")
    print("="*80)
    
    # V10 Experimental (with Bayesian adjustments)
    v10_result = run_simulation(
        visitor, home, team_stats, style_db, quad_data, eff_profiles,
        market_spread, market_total
    )
    
    # V9.2 Baseline (without Bayesian adjustments - set quad_data=None)
    v92_result = run_simulation(
        visitor, home, team_stats, style_db, None, None,
        market_spread, market_total
    )
    
    # Display comparison
    print(f"\n📊 PREDICTIONS COMPARISON:")
    print(f"{'Metric':<25} {'V9.2 Baseline':<20} {'V10 Experimental':<20} {'Delta':<15}")
    print("-" * 80)
    
    metrics = [
        ('Projected Score', 
         f"{v92_result['V_Score']}-{v92_result['H_Score']}", 
         f"{v10_result['V_Score']}-{v10_result['H_Score']}",
         ""),
        ('Spread', 
         f"{v92_result['Predicted_Spread']:+.1f}", 
         f"{v10_result['Predicted_Spread']:+.1f}",
         f"{v10_result['Predicted_Spread'] - v92_result['Predicted_Spread']:+.1f}"),
        ('Total', 
         f"{v92_result['Predicted_Total']:.1f}", 
         f"{v10_result['Predicted_Total']:.1f}",
         f"{v10_result['Predicted_Total'] - v92_result['Predicted_Total']:+.1f}"),
        ('Win Probability', 
         f"{v92_result['Home_Win_Prob']:.1f}%", 
         f"{v10_result['Home_Win_Prob']:.1f}%",
         f"{v10_result['Home_Win_Prob'] - v92_result['Home_Win_Prob']:+.1f}%"),
    ]
    
    for metric, v92, v10, delta in metrics:
        print(f"{metric:<25} {v92:<20} {v10:<20} {delta:<15}")
    
    # Experimental enhancements
    print(f"\n🔬 V10 EXPERIMENTAL FEATURES:")
    print(f"   Visitor Bayesian Adj: {v10_result['V_Bayes_Adj']:+.2f} pts")
    if v10_result['V_Bayes_Reasoning']:
        print(f"      → {v10_result['V_Bayes_Reasoning']}")
    
    print(f"   Home Bayesian Adj:    {v10_result['H_Bayes_Adj']:+.2f} pts")
    if v10_result['H_Bayes_Reasoning']:
        print(f"      → {v10_result['H_Bayes_Reasoning']}")
    
    print(f"   Confidence Class:     {v10_result['Confidence_Class']}")
    print(f"   Spread Variance:      {v10_result['Spread_Variance']:.2f} (vs base: {BASE_VARIANCE_SPREAD})")
    print(f"   Total Variance:       {v10_result['Total_Variance']:.2f} (vs base: {BASE_VARIANCE_TOTAL})")
    
    print(f"\n📌 ANALYSIS FLAGS:")
    print(f"   V9.2:  {v92_result['Analysis_Flags']}")
    print(f"   V10:   {v10_result['Analysis_Flags']}")
    
    # Betting signals comparison
    print(f"\n💰 BETTING SIGNALS:")
    if market_spread or market_total:
        print(f"   V9.2 Signals: {len(v92_result['Signals'])}")
        for sig in v92_result['Signals']:
            print(f"      • {sig['Type']}: {sig['Bet']} ({sig['Confidence']}, Kelly: {sig['Kelly_Size']})")
        
        print(f"   V10 Signals:  {len(v10_result['Signals'])}")
        for sig in v10_result['Signals']:
            print(f"      • {sig['Type']}: {sig['Bet']} ({sig['Confidence']}, Kelly: {sig['Kelly_Size']})")
    
    print("="*80 + "\n")
    
    # Log for tracking
    log_prediction_for_tracking(v10_result)
    
    return v10_result, v92_result

# ======================================================
# SECTION 6: MAIN EXECUTION
# ======================================================

if __name__ == "__main__":
    print("\n" + "="*80)
    print("🔬 THE BIBLE V10.0 - EXPERIMENTAL HYBRID EDITION")
    print("="*80)
    print("\nℹ️  EXPERIMENTAL VERSION - For Testing & Validation")
    print("   • Integrates Bayesian opponent quality adjustments")
    print("   • Empirical Bayes variance estimation")
    print("   • Independent performance tracking vs V9.2 baseline")
    print("   • Statistical rigor: Hierarchical modeling + shrinkage estimation")
    print("\n⚠️  NOT FOR LIVE BETTING - Validation Phase Only")
    print("="*80)
    
    print("\nCommands:")
    print("  1. Test single game comparison")
    print("  2. View performance report")
    print("  3. Exit")
    
    while True:
        choice = input("\nSelect option (1-3): ").strip()
        
        if choice == "1":
            print("\n📋 Enter game details:")
            visitor = input("Visitor team: ").strip()
            home = input("Home team: ").strip()
            
            spread_input = input("Market spread (optional, press Enter to skip): ").strip()
            market_spread = float(spread_input) if spread_input else None
            
            total_input = input("Market total (optional, press Enter to skip): ").strip()
            market_total = float(total_input) if total_input else None
            
            run_comparison_test(visitor, home, market_spread, market_total)
            
        elif choice == "2":
            generate_performance_report()
            
        elif choice == "3":
            print("\n👋 Exiting experimental simulator. Good luck with testing!\n")
            break
        
        else:
            print("Invalid option. Please select 1-3.")
