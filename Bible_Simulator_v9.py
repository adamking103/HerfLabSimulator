import requests
import pandas as pd
import numpy as np
import os
from io import StringIO
from datetime import datetime, timedelta

# ==============================================================================
#   THE BIBLE V9.2 - PERFORMANCE-TUNED EDITION
#   Fixes Based on 33-Game Analysis:
#   - Blowout detection improved
#   - Low-major uncertainty added
#   - HCA recalibrated
#   - Total caps implemented
# ==============================================================================

# --- CONFIGURATION ---
KP_API_KEY = "18ee6ae93a94ade96fc899611578ef42f57ad96e09845cb585ee3b2aab1444fa"
BASE_HCA_POINTS = 2.6  # ⬇️ REDUCED from 2.9 (Fix #4)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STYLE_DB_PATH = os.path.join(BASE_DIR, "..", "Data", "cbb_style_2025_complete.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "Output")
TRACKING_DIR = os.path.join(BASE_DIR, "..", "Data")

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
    "Sam Houston State": "Sam Houston St.", "San Diego State": "San Diego St.",
    "San Jose State": "San Jose St.", "South Carolina State": "S.C. State", "Arkansas Little Rock": "Little Rock",
    "South Dakota State": "South Dakota St.", "Southeast Missouri State": "Southeast Missouri",
    "Tarleton State": "Tarleton St.", "Tennessee State": "Tennessee St.", "Texas State": "Texas St.",
    "Utah State": "Utah St.", "Washington State": "Washington St.", "Weber State": "Weber St.",
    "Wichita State": "Wichita St.", "Wright State": "Wright St.", "Youngstown State": "Youngstown St.",
    "North Carolina Central": "N.C. Central", "UMBC": "Maryland BC", "Detroit Mercy": "Detroit",
    "Detroit": "Detroit", "IUPUI": "IU Indy", "Long Island University": "LIU", "LIU": "LIU",
    "Saint Peter's": "Saint Peter's", "St. Peter's": "Saint Peter's", "Saint Mary's": "Saint Mary's",
    "St. Mary's": "Saint Mary's", "Southern Illinois": "Southern Ill.", 
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
    "SMU": "SMU", "TCU": "TCU", "LSU": "LSU", "BYU": "BYU","Saint Louis": "Saint Louis","Saint Joseph's": "Saint Joseph's","UT Rio Grande Val": "UT Rio Grande Valley","SIU Edwardsville": "SIUE","Tennessee-Martin": "Tennessee Martin",
}

# --- VENUE-SPECIFIC HOME COURT ADVANTAGE (REDUCED BY 0.3-0.5) ---
VENUE_HCA = {
    'Duke': 4.2, 'Kentucky': 3.9, 'Kansas': 3.7, 'Gonzaga': 3.6, 'Villanova': 3.5,
    'Syracuse': 3.5, 'Louisville': 3.4, 'Michigan St.': 3.3, 'Wisconsin': 3.3,
    'North Carolina': 3.2, 'Arizona': 3.2, 'Virginia': 3.1, 'Purdue': 3.1,
    'Iowa St.': 3.0, 'Butler': 3.0, 'Creighton': 2.9, 'San Diego St.': 2.9,
    'Northwestern': 1.5, 'DePaul': 1.6, 'Georgia Tech': 1.7, 'Boston College': 1.8,
    'Rutgers': 2.0, 'Nebraska': 2.1, 'Wake Forest': 2.2,
}

# --- SIMULATION PARAMETERS ---
BASE_VARIANCE_TOTAL = 9.5
BASE_VARIANCE_SPREAD = 11.5
SIM_RUNS = 5000

# --- ADJUSTMENT WEIGHTS ---
LUCK_REGRESSION_FACTOR = 0.30
TURNOVER_POINT_VALUE = 1.2
OREB_POSSESSION_RATE = 0.25
SECOND_CHANCE_PPP = 1.05
SOS_VARIANCE_FACTOR = 0.05

# 🆕 V9.2 NEW PARAMETERS
BLOWOUT_TALENT_THRESHOLD = 25.0  # AdjEM gap threshold
BLOWOUT_MULTIPLIER = 1.15        # Increase predicted margin by 15%
LOW_MAJOR_RANK_THRESHOLD = 200   # Teams ranked worse than this
LOW_MAJOR_VARIANCE_MULT = 1.4    # Increase spread variance by 40%

# --- BETTING THRESHOLDS ---
TOTAL_EDGE_THRESHOLD = 3.0
SPREAD_EDGE_THRESHOLD = 2.0
HIGH_CONFIDENCE_TOTAL = 5.0
HIGH_CONFIDENCE_SPREAD = 4.0

# ======================================================
# 1. DATA FETCHING & PREPARATION
# ======================================================

def get_kenpom_data(endpoint, year=2026):
    url = f"https://kenpom.com/api.php?endpoint={endpoint}&y={year}"
    headers = {"Authorization": f"Bearer {KP_API_KEY}", "User-Agent": "TheBibleModel/9.2"}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        if endpoint == "misc-stats":
            return pd.read_csv(StringIO(response.text))
        return pd.DataFrame(response.json())
    except Exception as e:
        print(f"⚠️  API Error ({endpoint}): {e}")
        return None

def get_mapped_name(name):
    """Translates Market Name -> KenPom Name."""
    name = name.strip()
    if name in KENPOM_TRANSLATION:
        return KENPOM_TRANSLATION[name]
    if " State" in name:
        return name.replace(" State", " St.")
    if name.startswith("Saint "):
        return name.replace("Saint ", "St. ")
    return name

def build_team_database():
    print("🏗️  Building Team Database...")
    df_ratings = get_kenpom_data("ratings")
    df_four_factors = get_kenpom_data("four-factors")
    
    if df_ratings is None or df_four_factors is None:
        return None, None
    
    # --- FIX: Standardize API Columns ---
    # 1. Generate Rank (Data comes sorted by rank)
    if 'Rank' not in df_ratings.columns:
        df_ratings['Rank'] = df_ratings.index + 1
        
    # 2. Rename API keys to match Model expectations
    # API uses 'AdjO', 'AdjD', 'AdjT', 'SOS_AdjEM'. We map them here.
    rename_map = {
        'AdjO': 'AdjOE',
        'AdjD': 'AdjDE',
        'AdjT': 'AdjTempo',
        'SOS_AdjEM': 'SOS'
    }
    df_ratings = df_ratings.rename(columns=rename_map)
    
    # 3. Select the columns we need
    needed_cols = ['TeamName', 'Rank', 'AdjEM', 'AdjOE', 'AdjDE', 'AdjTempo', 'Luck', 'SOS']
    
    # Safety Check: Ensure columns exist before selecting
    missing = [c for c in needed_cols if c not in df_ratings.columns]
    if missing:
        print(f"⚠️  Critical Error: API missing columns: {missing}")
        print(f"    Found columns: {list(df_ratings.columns)}")
        return None, None

    # 4. Final Formatting
    team_stats = df_ratings[needed_cols].rename(columns={
        'AdjOE': 'Off_Eff', 'AdjDE': 'Def_Eff', 'AdjTempo': 'Tempo'
    })
    
    team_stats = team_stats.merge(
        df_four_factors[['TeamName', 'OR_Pct', 'DOR_Pct', 'TO_Pct', 'DTO_Pct', 'FT_Rate', 'DFT_Rate']], 
        on='TeamName', how='left'
    )
    
    # Load style database
    style_db = pd.DataFrame()
    if os.path.exists(STYLE_DB_PATH):
        style_db = pd.read_csv(STYLE_DB_PATH)
        print(f"✅ Style DB Loaded: {len(style_db)} teams")
    else:
        print("⚠️  Style DB not found - style adjustments disabled")
        
    return team_stats, style_db

# ======================================================
# 2. ADVANCED ADJUSTMENT FUNCTIONS
# ======================================================

def get_dynamic_hca(home_team):
    return VENUE_HCA.get(home_team, BASE_HCA_POINTS)

def get_style_adjustment(offensive_team, defensive_team, style_db):
    if style_db.empty: return 0.0, ""
    
    off_row = style_db[style_db['play_team'].str.contains(offensive_team, case=False, na=False)]
    def_row = style_db[style_db['play_team'].str.contains(defensive_team, case=False, na=False)]
    
    if off_row.empty or def_row.empty: return 0.0, ""
    
    off_rim_rate = off_row.iloc[0].get('rim_rate', 0)
    def_rim_allowed = def_row.iloc[0].get('opp_rim_rate', 0)
    off_arc_rate = off_row.iloc[0].get('arc_rate', 0)
    def_arc_allowed = def_row.iloc[0].get('opp_arc_rate', 0)
    
    total_adj = 0.0
    flags = []
    
    if off_rim_rate > 55.0 and def_rim_allowed < 50.0:
        total_adj -= 3.0
        flags.append("⛔ RIM WALL")
    elif off_rim_rate > 60.0 and def_rim_allowed > 60.0:
        total_adj += 3.0
        flags.append("🟢 LAYUP LINE")
    
    if off_arc_rate > 45.0 and def_arc_allowed > 40.0:
        total_adj += 2.5
        flags.append("🔥 SHOOTOUT")
    elif off_arc_rate > 45.0 and def_arc_allowed < 35.0:
        total_adj -= 2.0
        flags.append("🛡️ ARC WALL")
        
    return total_adj, " ".join(flags)

def calculate_turnover_impact(team_v, team_h, expected_tempo):
    v_to_advantage = team_v['DTO_Pct'] - team_h['TO_Pct']
    h_to_advantage = team_h['DTO_Pct'] - team_v['TO_Pct']
    v_to_points = v_to_advantage * expected_tempo / 100 * TURNOVER_POINT_VALUE
    h_to_points = h_to_advantage * expected_tempo / 100 * TURNOVER_POINT_VALUE
    return v_to_points, h_to_points

def calculate_rebounding_impact(team_v, team_h, expected_tempo):
    v_oreb_edge = team_v['OR_Pct'] - team_h['DOR_Pct']
    h_oreb_edge = team_h['OR_Pct'] - team_v['DOR_Pct']
    v_extra_poss = v_oreb_edge * expected_tempo * OREB_POSSESSION_RATE / 100
    h_extra_poss = h_oreb_edge * expected_tempo * OREB_POSSESSION_RATE / 100
    v_oreb_points = v_extra_poss * SECOND_CHANCE_PPP
    h_oreb_points = h_extra_poss * SECOND_CHANCE_PPP
    return v_oreb_points, h_oreb_points

def apply_rest_adjustment(days_rest):
    if days_rest == 0: return 0.97, 1.15
    elif days_rest == 1: return 0.99, 1.08
    elif days_rest >= 5: return 1.01, 1.05
    else: return 1.00, 1.00

def calculate_dynamic_variance(tempo, talent_diff, sos_diff, is_close_game, ft_rate_combined, base_variance=BASE_VARIANCE_SPREAD):
    tempo_factor = tempo / 68.0
    blowout_factor = max(0.85, 1.0 - (talent_diff / 200.0))
    sos_factor = max(0.90, min(1.10, 1.0 - (sos_diff * SOS_VARIANCE_FACTOR)))
    close_game_factor = 1.15 if is_close_game else 1.0
    ft_factor = max(0.90, min(1.05, 1.0 - (max(0, ft_rate_combined - 30) / 100)))
    
    return base_variance * tempo_factor * blowout_factor * sos_factor * close_game_factor * ft_factor

def apply_luck_regression(team_v, team_h):
    return (team_v['Luck'] - team_h['Luck']) * LUCK_REGRESSION_FACTOR

# 🆕 V9.2 NEW FUNCTIONS

def detect_blowout_potential(team_v, team_h):
    """
    FIX #1: Detect extreme talent mismatches and increase predicted margin.
    Based on 33-game analysis showing model underestimates blowouts.
    """
    talent_gap = abs(team_v.get('AdjEM', 0) - team_h.get('AdjEM', 0))
    
    if talent_gap > BLOWOUT_TALENT_THRESHOLD:
        return BLOWOUT_MULTIPLIER, "🔴 BLOWOUT ALERT"
    
    return 1.0, ""

def apply_low_major_penalty(team_v, team_h, variance_spread, variance_total):
    """
    FIX #2: Increase variance for low-major games (Rank > 200).
    These games showed 40% higher unpredictability in analysis.
    """
    v_rank = team_v.get('Rank', 0)
    h_rank = team_h.get('Rank', 0)
    
    if v_rank > LOW_MAJOR_RANK_THRESHOLD or h_rank > LOW_MAJOR_RANK_THRESHOLD:
        return variance_spread * LOW_MAJOR_VARIANCE_MULT, variance_total * 1.3, "⚠️ LOW-MAJOR"
    
    return variance_spread, variance_total, ""

def apply_total_sanity_caps(predicted_total, expected_tempo):
    """
    FIX #3: Cap totals based on reasonable pace limits.
    Prevents extreme outliers like 194-point games from being way off.
    """
    max_reasonable = expected_tempo * 2.2  # Even fast games rarely exceed 2.2 PPP
    min_reasonable = expected_tempo * 1.4  # Defensive slugfests floor at ~1.4 PPP
    
    if predicted_total > max_reasonable:
        return max_reasonable, "⬇️ CAPPED HIGH"
    elif predicted_total < min_reasonable:
        return min_reasonable, "⬆️ CAPPED LOW"
    
    return predicted_total, ""

# ======================================================
# 3. ENHANCED SIMULATION ENGINE (V9.2)
# ======================================================

def run_game_simulation(visitor, home, team_stats, style_db, neutral_site=False, 
                        market_spread=None, market_total=None,
                        v_rest_days=2, h_rest_days=2):
    
    kp_visitor = get_mapped_name(visitor)
    kp_home = get_mapped_name(home)
    
    try:
        v_data = team_stats[team_stats['TeamName'] == kp_visitor]
        h_data = team_stats[team_stats['TeamName'] == kp_home]
        
        if v_data.empty: v_data = team_stats[team_stats['TeamName'].str.contains(kp_visitor, case=False, na=False)]
        if h_data.empty: h_data = team_stats[team_stats['TeamName'].str.contains(kp_home, case=False, na=False)]
            
        if v_data.empty or h_data.empty:
            print(f"⚠️  Skipping: {visitor} -> {kp_visitor} | {home} -> {kp_home} (Not found)")
            return None
            
        team_v = v_data.iloc[0]
        team_h = h_data.iloc[0]
        
    except Exception as e:
        print(f"❌ Error processing {visitor} @ {home}: {e}")
        return None

    avg_tempo = team_stats['Tempo'].mean()
    nat_avg_eff = team_stats['Off_Eff'].mean()
    
    geo_mean_tempo = (team_v['Tempo'] * team_h['Tempo']) / avg_tempo
    tempo_diff = abs(team_v['Tempo'] - team_h['Tempo'])
    defensive_tempo_weight = 0.6
    
    if tempo_diff > 5.0:
        expected_tempo = (min(team_v['Tempo'], team_h['Tempo']) * defensive_tempo_weight + geo_mean_tempo * (1 - defensive_tempo_weight))
    else:
        expected_tempo = geo_mean_tempo

    v_style_pts, v_flags = get_style_adjustment(kp_visitor, kp_home, style_db)
    h_style_pts, h_flags = get_style_adjustment(kp_home, kp_visitor, style_db)

    adj_off_v = (team_v['Off_Eff'] * team_h['Def_Eff']) / nat_avg_eff + (v_style_pts / expected_tempo) * 100
    adj_off_h = (team_h['Off_Eff'] * team_v['Def_Eff']) / nat_avg_eff + (h_style_pts / expected_tempo) * 100

    luck_adj = apply_luck_regression(team_v, team_h)
    adj_off_v += (luck_adj / expected_tempo) * 100
    adj_off_h -= (luck_adj / expected_tempo) * 100

    v_to_points, h_to_points = calculate_turnover_impact(team_v, team_h, expected_tempo)
    v_oreb_points, h_oreb_points = calculate_rebounding_impact(team_v, team_h, expected_tempo)
    v_rest_mult, v_rest_var = apply_rest_adjustment(v_rest_days)
    h_rest_mult, h_rest_var = apply_rest_adjustment(h_rest_days)

    mean_v = adj_off_v * expected_tempo / 100
    mean_h = adj_off_h * expected_tempo / 100
    mean_v += v_to_points + v_oreb_points
    mean_h += h_to_points + h_oreb_points
    mean_v *= v_rest_mult
    mean_h *= h_rest_mult
    
    # 🆕 FIX #1: BLOWOUT DETECTION
    blowout_mult, blowout_flag = detect_blowout_potential(team_v, team_h)
    if blowout_mult > 1.0:
        if mean_h > mean_v:
            mean_h *= blowout_mult
        else:
            mean_v *= blowout_mult
    
    dynamic_hca = get_dynamic_hca(kp_home)
    if not neutral_site: mean_h += dynamic_hca

    talent_diff = abs(team_v['Off_Eff'] - team_h['Off_Eff']) + abs(team_v['Def_Eff'] - team_h['Def_Eff'])
    sos_diff = team_v['SOS'] - team_h['SOS']
    pred_margin_early = mean_h - mean_v
    is_close_game = abs(pred_margin_early) < 6
    
    ft_rate_combined = (team_v.get('FT_Rate', 30) + team_h.get('DFT_Rate', 30)) / 2
    
    variance_spread = calculate_dynamic_variance(expected_tempo, talent_diff, sos_diff, is_close_game, ft_rate_combined, base_variance=BASE_VARIANCE_SPREAD)
    variance_total = calculate_dynamic_variance(expected_tempo, talent_diff, sos_diff, is_close_game, ft_rate_combined, base_variance=BASE_VARIANCE_TOTAL)
    
    # 🆕 FIX #2: LOW-MAJOR VARIANCE PENALTY
    variance_spread, variance_total, low_major_flag = apply_low_major_penalty(team_v, team_h, variance_spread, variance_total)
    
    variance_spread *= (v_rest_var + h_rest_var) / 2
    variance_total *= (v_rest_var + h_rest_var) / 2

    np.random.seed(None)
    scores_v_spread = np.random.normal(mean_v, variance_spread, SIM_RUNS)
    scores_h_spread = np.random.normal(mean_h, variance_spread, SIM_RUNS)
    scores_v_total = np.random.normal(mean_v, variance_total, SIM_RUNS)
    scores_h_total = np.random.normal(mean_h, variance_total, SIM_RUNS)
    
    avg_v = np.mean(scores_v_spread)
    avg_h = np.mean(scores_h_spread)
    pred_total = np.mean(scores_v_total) + np.mean(scores_h_total)
    
    # 🆕 FIX #3: TOTAL SANITY CAPS
    pred_total, total_cap_flag = apply_total_sanity_caps(pred_total, expected_tempo)
    
    pred_margin = avg_h - avg_v
    win_prob_home = np.mean(scores_h_spread > scores_v_spread) * 100

    cover_prob_fav = 0.0
    cover_prob_dog = 0.0
    over_prob = 0.0
    under_prob = 0.0

    if market_spread is not None:
        home_favored = market_spread < 0
        spread_mag = abs(market_spread)
        if home_favored:
            cover_prob_fav = np.mean((scores_h_spread - scores_v_spread) > spread_mag) * 100
        else:
            cover_prob_fav = np.mean((scores_v_spread - scores_h_spread) > spread_mag) * 100
        cover_prob_dog = 100.0 - cover_prob_fav

    if market_total is not None:
        over_prob = np.mean((scores_v_total + scores_h_total) > market_total) * 100
        under_prob = 100.0 - over_prob

    model_spread_home = round(pred_margin, 1)
    model_total = round(pred_total, 1)
    model_line = -1 * model_spread_home
    
    spread_diff = abs(model_line - market_spread) if market_spread is not None else 0.0
    total_diff = abs(model_total - market_total) if market_total is not None else 0.0

    # Combine all flags
    analysis_flags = []
    if v_rest_days == 0: analysis_flags.append(f"V:B2B")
    if h_rest_days == 0: analysis_flags.append(f"H:B2B")
    if abs(team_v['Luck']) > 0.05: analysis_flags.append(f"V:Luck{team_v['Luck']:+.2f}")
    if abs(team_h['Luck']) > 0.05: analysis_flags.append(f"H:Luck{team_h['Luck']:+.2f}")
    if not neutral_site and dynamic_hca != BASE_HCA_POINTS: analysis_flags.append(f"HCA:{dynamic_hca:.1f}")
    if blowout_flag: analysis_flags.append(blowout_flag)
    if low_major_flag: analysis_flags.append(low_major_flag)
    if total_cap_flag: analysis_flags.append(total_cap_flag)

    return {
        'Visitor': visitor, 'V_Score': round(avg_v, 1), 'V_Flags': v_flags, 'V_Luck': f"{team_v['Luck']:.3f}", 'V_Rest': v_rest_days,
        'Home': home, 'H_Score': round(avg_h, 1), 'H_Flags': h_flags, 'H_Luck': f"{team_h['Luck']:.3f}", 'H_Rest': h_rest_days,
        'Predicted_Spread': model_spread_home, 'Market_Spread': market_spread, 'Spread_Diff': round(spread_diff, 1),
        'Predicted_Total': model_total, 'Market_Total': market_total, 'Total_Diff': round(total_diff, 1),
        'Win_Prob_Home': round(win_prob_home, 1), 'Tempo': round(expected_tempo, 1),
        'Variance_Spread': round(variance_spread, 1), 'Variance_Total': round(variance_total, 1),
        'Cover_Prob_Fav': round(cover_prob_fav, 1), 'Cover_Prob_Dog': round(cover_prob_dog, 1),
        'Over_Prob': round(over_prob, 1), 'Under_Prob': round(under_prob, 1),
        'Analysis_Flags': ' | '.join(analysis_flags)
    }

# ======================================================
# 4. EDGE DETECTION & MOBILE GENERATION (Unchanged)
# ======================================================

def detect_betting_edge(prediction, market_spread, market_total):
    """
    Enhanced edge detection with FIXED Kelly Logic (Uses Cover Prob, not Win Prob).
    """
    signals = []
    
    # --- 1. TOTALS EDGE ---
    total_edge = prediction['Total_Diff']
    
    if total_edge >= TOTAL_EDGE_THRESHOLD:
        confidence = 'HIGH' if total_edge >= HIGH_CONFIDENCE_TOTAL else 'MEDIUM'
        bet_side = 'OVER' if prediction['Predicted_Total'] > market_total else 'UNDER'
        
        # Correctly use Over/Under probabilities
        prob = prediction['Over_Prob'] if bet_side == 'OVER' else prediction['Under_Prob']
        
        # Kelly Criterion: f = (p - 0.5) / 0.5 (for even money bets approx)
        # We cap it at 5% to protect bankroll
        kelly_pct = max(0, min(((prob / 100) - 0.5) / 0.5, 0.05))
        
        if kelly_pct > 0:
            signals.append({
                'Type': 'TOTAL',
                'Bet': f"{bet_side} {market_total}",
                'Edge': total_edge,
                'Confidence': confidence,
                'Reasoning': f"Model {prediction['Predicted_Total']} vs Market {market_total} ({prob:.1f}% chance)",
                'Kelly_Size': f"{kelly_pct*100:.1f}%"
            })

    # --- 2. SPREAD EDGE ---
    spread_edge = prediction['Spread_Diff']
    model_line = -1 * prediction['Predicted_Spread']
    
    if spread_edge >= SPREAD_EDGE_THRESHOLD:
        confidence = 'HIGH' if spread_edge >= HIGH_CONFIDENCE_SPREAD else 'MEDIUM'
        
        # Determine which side we are betting
        if model_line < market_spread:
            bet_team = prediction['Home']
            # betting HOME
            bet_home = True
        else:
            bet_team = prediction['Visitor']
            # betting VISITOR
            bet_home = False
            
        # --- FIX: USE COVER PROBABILITY, NOT WIN PROBABILITY ---
        # 1. Is the team we are betting the Market Favorite or Underdog?
        # Market Spread Negative = Home Fav. Positive = Visitor Fav.
        
        if bet_home:
            # We are betting Home. Are they Fav or Dog?
            is_fav = market_spread < 0
        else:
            # We are betting Visitor. Are they Fav or Dog?
            is_fav = market_spread > 0
            
        # 2. Select the correct probability from the simulation results
        if is_fav:
            bet_prob = prediction['Cover_Prob_Fav']
        else:
            bet_prob = prediction['Cover_Prob_Dog']

        # Determine Display Line (e.g. +12.5 or -3.5)
        if is_fav:
            line_disp = f"-{abs(market_spread)}"
        else:
            line_disp = f"+{abs(market_spread)}"

        # 3. Calculate Kelly based on COVER probability
        kelly_pct = max(0, min(((bet_prob / 100) - 0.5) / 0.5, 0.05))
        
        # Luck Analysis for reasoning text
        reason_extra = ""
        team_luck = float(prediction['H_Luck']) if bet_home else float(prediction['V_Luck'])
        opp_luck = float(prediction['V_Luck']) if bet_home else float(prediction['H_Luck'])
        
        if team_luck < -0.050: reason_extra += " | Buying Low (Unlucky)"
        if opp_luck > 0.050: reason_extra += " | Fading Lucky Opp"
        
        if kelly_pct > 0:
            signals.append({
                'Type': 'SPREAD',
                'Bet': f"{bet_team} {line_disp}",
                'Edge': spread_edge,
                'Confidence': confidence,
                'Reasoning': f"Model Line {model_line:+.1f} vs Mkt {market_spread} ({bet_prob:.1f}% cover prob){reason_extra}",
                'Kelly_Size': f"{kelly_pct*100:.1f}%"
            })

    return signals

def generate_mobile_html(predictions, bet_signals_list, game_date):
    """Generates a phone-friendly 'Card View' HTML report."""
    html = []
    html.append(f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #f0f2f5; margin: 0; padding: 12px; color: #333; }}
            .header {{ text-align: center; margin-bottom: 20px; }}
            .header h2 {{ margin: 0; color: #667eea; }}
            .date {{ color: #666; font-size: 0.9em; }}
            .card {{ background: white; border-radius: 16px; padding: 16px; margin-bottom: 16px; box-shadow: 0 2px 8px rgba(0,0,0,0.05); border: 1px solid #fff; }}
            .card.has-bet {{ border: 1px solid #667eea; box-shadow: 0 4px 12px rgba(102, 126, 234, 0.15); }}
            .matchup {{ display: flex; justify-content: space-between; align-items: center; font-weight: 700; font-size: 1.1em; margin-bottom: 8px; }}
            .vs {{ color: #999; font-size: 0.8em; font-weight: normal; margin: 0 8px; }}
            .proj-score {{ text-align: center; background: #f8f9fa; border-radius: 8px; padding: 8px; font-size: 0.9em; color: #555; margin-bottom: 12px; }}
            .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 0.85em; }}
            .stat-box {{ background: #fff; border: 1px solid #eee; border-radius: 8px; padding: 8px; }}
            .label {{ display: block; color: #888; font-size: 0.8em; margin-bottom: 2px; }}
            .val {{ font-weight: 600; display: block; }}
            .edge-good {{ color: #27ae60; font-weight: bold; }}
            .bet-box {{ margin-top: 12px; padding: 12px; border-radius: 8px; background: #eef2ff; color: #4338ca; border: 1px solid #c7d2fe; }}
            .bet-high {{ background: #fff1f2; color: #be123c; border: 1px solid #fecdd3; }}
            .bet-header {{ display: flex; align-items: center; font-weight: bold; margin-bottom: 4px; }}
            .bet-reason {{ font-size: 0.8em; opacity: 0.9; }}
            .flags {{ margin-top: 8px; font-size: 0.75em; color: #d97706; background: #fffbeb; padding: 4px 8px; border-radius: 4px; display: inline-block; }}
        </style>
    </head>
    <body>
        <div class="header"><h2>📱 Bible V9.2</h2><div class="date">{game_date}</div></div>
    """)

    for i, pred in enumerate(predictions):
        signals = bet_signals_list[i]
        has_bet = len(signals) > 0
        card_class = "card has-bet" if has_bet else "card"
        
        html.append(f'<div class="{card_class}">')
        html.append(f'<div class="matchup"><span>{pred["Visitor"]}</span><span class="vs">@</span><span>{pred["Home"]}</span></div>')
        html.append(f'<div class="proj-score">Model: <b>{pred["V_Score"]}</b> - <b>{pred["H_Score"]}</b></div>')
        
        s_edge_style = "edge-good" if pred['Spread_Diff'] >= 2.0 else ""
        t_edge_style = "edge-good" if pred['Total_Diff'] >= 3.0 else ""
        
        html.append(f"""
            <div class="grid">
                <div class="stat-box"><span class="label">SPREAD (Mkt {pred['Market_Spread']})</span><span class="val">Model: {pred['Predicted_Spread']:+.1f}</span><span class="val {s_edge_style}">Edge: {pred['Spread_Diff']}</span></div>
                <div class="stat-box"><span class="label">TOTAL (Mkt {pred['Market_Total']})</span><span class="val">Model: {pred['Predicted_Total']}</span><span class="val {t_edge_style}">Edge: {pred['Total_Diff']}</span></div>
            </div>
        """)
        
        if pred['Analysis_Flags']: html.append(f'<div class="flags">📌 {pred["Analysis_Flags"]}</div>')
            
        if has_bet:
            for sig in signals:
                is_high = sig['Confidence'] == 'HIGH'
                box_class = "bet-box bet-high" if is_high else "bet-box"
                icon = "🔥" if is_high else "✅"
                html.append(f'<div class="{box_class}"><div class="bet-header">{icon} {sig["Type"]}: {sig["Bet"]}</div><div class="bet-reason">Kelly: {sig["Kelly_Size"]} • {sig["Reasoning"]}</div></div>')
        
        html.append("</div>")

    html.append("</body></html>")
    return "\n".join(html)

# ======================================================
# 5. MAIN EXECUTION & TRACKING (Unchanged except version number)
# ======================================================

def save_prediction_for_tracking(prediction, market_spread, market_total, game_date, bet_signals):
    tracker_path = os.path.join(TRACKING_DIR, "Performance_Tracker_V9_2.csv")
    row = {
        'Date': game_date, 'Visitor': prediction['Visitor'], 'Home': prediction['Home'],
        'V_Score': prediction['V_Score'], 'H_Score': prediction['H_Score'],
        'Predicted_Spread': prediction['Predicted_Spread'], 'Market_Spread': market_spread, 'Spread_Edge': prediction['Spread_Diff'],
        'Predicted_Total': prediction['Predicted_Total'], 'Market_Total': market_total, 'Total_Edge': prediction['Total_Diff'],
        'Bet_Signals': '; '.join([f"{s['Type']} {s['Bet']} ({s['Kelly_Size']})" for s in bet_signals])
    }
    df_row = pd.DataFrame([row])
    if os.path.exists(tracker_path):
        df_row.to_csv(tracker_path, mode='a', header=False, index=False)
    else:
        os.makedirs(TRACKING_DIR, exist_ok=True)
        df_row.to_csv(tracker_path, index=False)

def generate_daily_report(predictions, bet_signals_list):
    print("\n" + "="*90)
    print("📊 THE BIBLE V9.2 - DAILY BETTING REPORT (PERFORMANCE-TUNED)")
    print("="*90)
    bet_games = [(pred, sigs) for pred, sigs in zip(predictions, bet_signals_list) if sigs]
    if bet_games:
        print(f"\n🎯 RECOMMENDED BETS: {len(bet_games)} Games with Edge\n")
        for pred, signals in bet_games:
            print(f"\n🏀 {pred['Visitor']} @ {pred['Home']}")
            print(f"   Model: {pred['V_Score']} - {pred['H_Score']} (Total: {pred['Predicted_Total']})")
            if pred['Analysis_Flags']: print(f"   📌 {pred['Analysis_Flags']}")
            for sig in signals:
                emoji = "🔥" if sig['Confidence'] == 'HIGH' else "✅"
                print(f"\n   {emoji} {sig['Type']}: {sig['Bet']}")
                print(f"      Edge: {sig['Edge']} pts | {sig['Confidence']} | Kelly: {sig['Kelly_Size']}")
            print("-"*90)
    else:
        print("\n⚠️  No value bets identified today.")

if __name__ == "__main__":
    print("\n" + "="*90)
    print("🚀 THE BIBLE V9.2 - PERFORMANCE-TUNED EDITION")
    print("   Changes from V9.1:")
    print("   ✅ Blowout detection improved (15% multiplier for talent gap >25)")
    print("   ✅ Low-major variance increased (40% for teams ranked >200)")
    print("   ✅ HCA reduced by 0.3 pts across the board")
    print("   ✅ Total sanity caps added (1.4-2.2 PPP)")
    print("="*90 + "\n")
    
    team_stats, style_db = build_team_database()
    if team_stats is None:
        print("❌ Cannot run model without team data")
        exit(1)
    
    print(f"✅ Team Database Ready: {len(team_stats)} teams loaded\n")
    
# ==============================================================================
#  TODAY'S SLATE (Thursday, Jan 1)
#  Format: (Visitor, Home, Neutral, Home_Spread, Total, V_Rest, H_Rest)
#  Note: Home_Spread -> Negative (-) means Home is Favorite, Positive (+) means Home is Dog
# ==============================================================================

todays_games = [
    # Early Slate
    ("Virginia", "NC State", False, -4.5, 152.5, 2, 2),
    ("Clemson", "Pittsburgh", False, 3.5, 135.5, 2, 2),
    ("Kentucky", "Alabama", False, -5.5, 174.5, 2, 2),
    ("Northeastern", "Campbell", False, -3.5, 158.5, 2, 2),
    ("Providence", "St. John's", False, -12.5, 169.5, 2, 2),
    ("UTSA", "Temple", False, -10.5, 150.5, 2, 2),
    ("VCU", "Duquesne", False, 5.5, 164.5, 2, 2),
    ("Villanova", "Butler", False, -1.5, 146.5, 2, 2),
    ("Virginia Tech", "Wake Forest", False, -5.5, 151.5, 2, 2),
    ("Auburn", "Georgia", False, -5.5, 174.5, 2, 2),
    ("American", "Boston University", False, 1.5, 148.5, 2, 2),
    ("Chattanooga", "VMI", False, 5.5, 148.5, 2, 2),
    ("Colgate", "Army", False, 6.5, 147.5, 2, 2),
    ("Georgia State", "Coastal Carolina", False, -6.5, 146.5, 2, 2),
    ("Northern Illinois", "Kent State", False, -16.5, 158.5, 2, 2),
    ("Oklahoma State", "Texas Tech", False, -11.5, 168.5, 2, 2),
    ("Southern Miss", "Louisiana", False, 4.5, 130.5, 2, 2),
    ("Vermont", "New Hampshire", False, 7.5, 137.5, 2, 2),
    ("Wofford", "The Citadel", False, 10.5, 144.5, 2, 2),
    ("BYU", "Kansas State", False, 7.5, 172.5, 2, 2),
    ("Vanderbilt", "South Carolina", False, 11.5, 157.5, 2, 2),
    ("Kansas", "UCF", False, 4.5, 153.5, 2, 2),
    ("Houston", "Cincinnati", False, 7.5, 134.5, 2, 2),
    ("Baylor", "TCU", False, -1.5, 154.5, 2, 2),
    ("Boston College", "Georgia Tech", False, -6.5, 145.5, 2, 2),
    ("Bowling Green", "UMass", False, 2.5, 149.5, 2, 2),
    ("Dayton", "Loyola Chicago", False, 8.5, 141.5, 2, 2),
    ("Hofstra", "Drexel", False, 6.5, 136.5, 2, 2),
    ("NJIT", "Binghamton", False, -1.5, 141.5, 2, 2),
    ("Stetson", "Central Arkansas", False, -9.5, 146.5, 2, 2),
    ("Xavier", "DePaul", False, -2.5, 146.5, 2, 2),
    ("North Carolina", "SMU", False, -1.5, 156.5, 2, 2),
    ("Tennessee", "Arkansas", False, -2.5, 157.5, 2, 2),
    ("Duke", "Florida State", False, 15.5, 164.5, 2, 2),
    ("Arizona", "Utah", False, 18.5, 162.5, 2, 2),
    ("UCLA", "Iowa", False, -6.5, 138.5, 2, 2),
    ("Illinois", "Penn State", False, 16.5, 157.5, 2, 2),
    ("Purdue", "Wisconsin", False, 6.5, 151.5, 2, 2),
    ("Florida", "Missouri", False, 6.5, 153.5, 2, 2),
    
    

]

# ==============================================================================
#  EXECUTION & REPORTING
# ==============================================================================

game_date = datetime.now().strftime('%Y-%m-%d')
predictions = []
all_signals = []

print(f"🟢 Running V9.2 Simulations for {len(todays_games)} games...")
print("="*90 + "\n")

for visitor, home, neutral, mkt_spread, mkt_total, v_rest, h_rest in todays_games:
    pred = run_game_simulation(
        visitor, home, team_stats, style_db, neutral,
        market_spread=mkt_spread, market_total=mkt_total,
        v_rest_days=v_rest, h_rest_days=h_rest
    )
    if pred:
        signals = detect_betting_edge(pred, mkt_spread, mkt_total)
        save_prediction_for_tracking(pred, mkt_spread, mkt_total, game_date, signals)
        predictions.append(pred)
        all_signals.append(signals)

generate_daily_report(predictions, all_signals)

if predictions:
    df = pd.DataFrame(predictions)
    cols = [
        'Visitor', 'V_Score', 'V_Flags', 'V_Luck', 'V_Rest',
        'Home', 'H_Score', 'H_Flags', 'H_Luck', 'H_Rest',
        'Predicted_Spread', 'Market_Spread', 'Spread_Diff',
        'Predicted_Total', 'Market_Total', 'Total_Diff',
        'Win_Prob_Home', 'Tempo', 'Variance_Spread', 'Variance_Total',
        'Cover_Prob_Fav', 'Cover_Prob_Dog', 'Over_Prob', 'Under_Prob',
        'Analysis_Flags'
    ]
    
    # Only use columns that actually exist in the prediction data
    avail_cols = [c for c in cols if c in df.columns]
    df = df[avail_cols]

    # Save to CSV
    out_csv = os.path.join(OUTPUT_DIR, f"Bible_V9_2_Results_{game_date}.csv")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(out_csv, index=False)

    # Save to HTML
    out_html = os.path.join(OUTPUT_DIR, f"Bible_V9_2_Results_{game_date}.html")
    style = """<style>
    body { font-family: 'Segoe UI', Tahoma, sans-serif; margin: 20px; background: #f5f5f5; }
    h2 { color: #667eea; text-align: center; }
    table { border-collapse: collapse; width: 100%; background: white; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    th { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 12px; text-align: left; font-size: 12px; }
    td { padding: 10px; border-bottom: 1px solid #e0e0e0; font-size: 11px; }
    tr:hover { background: #f9f9f9; }
    </style>"""
    
    with open(out_html, 'w', encoding='utf-8') as f:
        f.write(style + f"<h2>🏀 THE BIBLE V9.2 - Predictions for {game_date}</h2>")
        f.write(df.to_html(index=False, classes='table', escape=False))

    # Save Mobile HTML
    out_mobile = os.path.join(OUTPUT_DIR, f"Bible_Mobile_V9_2_{game_date}.html")
    mobile_html_content = generate_mobile_html(predictions, all_signals, game_date)
    with open(out_mobile, 'w', encoding='utf-8') as f:
        f.write(mobile_html_content)

    print(f"\n✅ V9.2 Results saved:")
    print(f"   📄 CSV: {out_csv}")
    print(f"   🖥️ Desktop HTML: {out_html}")
    print(f"   📱 Mobile HTML: {out_mobile}")
    print(f"\n🚀 Track performance over 50+ games before betting real money!")

def generate_mobile_html(predictions, bet_signals_list, game_date):
    """
    Generates a premium 'Betting App' style mobile report.
    - Dark Mode
    - High Contrast for Sunlight
    - collapsible 'All Games' section
    """
    
    # Filter for games that actually have bets
    active_bets = []
    other_games = []
    
    for i, pred in enumerate(predictions):
        signals = bet_signals_list[i]
        if signals:
            active_bets.append((pred, signals))
        else:
            other_games.append(pred)

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
        <title>The Bible: {game_date}</title>
        <style>
            :root {{
                --bg-color: #121212;
                --card-bg: #1e1e1e;
                --text-main: #e0e0e0;
                --text-muted: #a0a0a0;
                --accent-green: #00e676;
                --accent-red: #ff5252;
                --accent-blue: #2979ff;
                --border-radius: 12px;
            }}
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                background-color: var(--bg-color);
                color: var(--text-main);
                margin: 0;
                padding: 16px;
                line-height: 1.4;
            }}
            .header {{
                text-align: center;
                margin-bottom: 24px;
                padding-bottom: 12px;
                border-bottom: 1px solid #333;
            }}
            .header h1 {{ margin: 0; font-size: 1.5rem; color: var(--text-main); letter-spacing: 1px; }}
            .header .date {{ color: var(--text-muted); font-size: 0.9rem; margin-top: 4px; }}
            
            .section-title {{
                font-size: 0.85rem;
                text-transform: uppercase;
                letter-spacing: 1.5px;
                color: var(--text-muted);
                margin: 20px 0 10px 4px;
                font-weight: 700;
            }}
            
            .card {{
                background: var(--card-bg);
                border-radius: var(--border-radius);
                padding: 16px;
                margin-bottom: 16px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.3);
                border-left: 4px solid transparent;
            }}
            
            .card.has-action {{ border-left-color: var(--accent-green); }}
            .card.warning {{ border-left-color: var(--accent-red); }}
            
            .matchup {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                font-weight: 600;
                font-size: 1.1rem;
                margin-bottom: 12px;
            }}
            .vs {{ color: var(--text-muted); font-size: 0.8rem; font-weight: 400; margin: 0 8px; }}
            
            .scores-row {{
                display: flex;
                justify-content: space-between;
                background: rgba(255,255,255,0.05);
                padding: 10px;
                border-radius: 8px;
                margin-bottom: 12px;
                font-size: 0.9rem;
            }}
            .score-item {{ text-align: center; flex: 1; }}
            .score-label {{ display: block; font-size: 0.7rem; color: var(--text-muted); margin-bottom: 2px; }}
            .score-val {{ display: block; font-weight: 700; color: #fff; }}
            
            .bet-signal {{
                background: rgba(0, 230, 118, 0.1);
                border: 1px solid rgba(0, 230, 118, 0.3);
                color: var(--accent-green);
                padding: 12px;
                border-radius: 8px;
                margin-top: 8px;
            }}
            
            .bet-signal.high-conf {{
                background: rgba(41, 121, 255, 0.15);
                border-color: var(--accent-blue);
                color: var(--accent-blue);
            }}
            
            .bet-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px; }}
            .bet-type {{ font-weight: 800; font-size: 0.8rem; text-transform: uppercase; }}
            .bet-line {{ font-size: 1.1rem; font-weight: 700; }}
            .kelly-badge {{ 
                background: #fff; color: #000; font-weight: 700; 
                font-size: 0.7rem; padding: 2px 6px; border-radius: 4px; 
            }}
            
            .reasoning {{ font-size: 0.8rem; opacity: 0.8; margin-top: 4px; }}
            
            .flags {{ 
                margin-top: 10px; 
                font-size: 0.75rem; 
                color: #ff9800; 
                background: rgba(255, 152, 0, 0.1); 
                padding: 6px; 
                border-radius: 6px; 
                display: inline-block;
            }}
            
            .no-bet-games {{ opacity: 0.6; display: none; }} /* Hidden by default */
            .show-btn {{
                background: none; border: 1px solid #333; color: var(--text-muted);
                width: 100%; padding: 12px; border-radius: 8px; margin-top: 20px;
                font-size: 0.9rem;
            }}
        </style>
        <script>
            function toggleGames() {{
                var x = document.getElementById("otherGames");
                if (x.style.display === "block") {{ x.style.display = "none"; }} 
                else {{ x.style.display = "block"; }}
            }}
        </script>
    </head>
    <body>
        <div class="header">
            <h1>📱 THE BIBLE</h1>
            <div class="date">{game_date}</div>
        </div>

        <div class="section-title">🔥 Action Card ({len(active_bets)})</div>
    """
    
    # 1. RENDER ACTIVE BETS
    if not active_bets:
        html += '<div class="card"><div style="text-align:center; padding:20px; color:#666;">No Plays Today</div></div>'
        
    for pred, signals in active_bets:
        html += f"""
        <div class="card has-action">
            <div class="matchup">
                <span>{pred['Visitor']}</span>
                <span class="vs">@</span>
                <span>{pred['Home']}</span>
            </div>
            
            <div class="scores-row">
                <div class="score-item">
                    <span class="score-label">MODEL SCORE</span>
                    <span class="score-val">{pred['V_Score']} - {pred['H_Score']}</span>
                </div>
                <div class="score-item">
                    <span class="score-label">MODEL LINE</span>
                    <span class="score-val">{pred['Predicted_Spread']:+.1f}</span>
                </div>
                <div class="score-item">
                    <span class="score-label">MODEL TOTAL</span>
                    <span class="score-val">{pred['Predicted_Total']}</span>
                </div>
            </div>
        """
        
        # Add Signals
        for sig in signals:
            is_high = sig['Confidence'] == 'HIGH'
            style_class = "bet-signal high-conf" if is_high else "bet-signal"
            icon = "💎" if is_high else "✅"
            
            html += f"""
            <div class="{style_class}">
                <div class="bet-header">
                    <span class="bet-type">{icon} {sig['Type']}</span>
                    <span class="kelly-badge">Kelly: {sig['Kelly_Size']}</span>
                </div>
                <div class="bet-line">{sig['Bet']}</div>
                <div class="reasoning">{sig['Reasoning']}</div>
            </div>
            """
            
        # Add Flags if they exist
        if pred['Analysis_Flags']:
            html += f'<div class="flags">⚠️ {pred["Analysis_Flags"]}</div>'
            
        html += "</div>"

    # 2. RENDER OTHER GAMES (Hidden by default)
    html += f"""
        <button class="show-btn" onclick="toggleGames()">Show {len(other_games)} Other Games ↓</button>
        <div id="otherGames" class="no-bet-games">
        <div class="section-title">Passed Games</div>
    """
    
    for pred in other_games:
        html += f"""
        <div class="card">
            <div class="matchup" style="font-size: 1rem;">
                <span>{pred['Visitor']}</span>
                <span class="vs">@</span>
                <span>{pred['Home']}</span>
            </div>
            <div class="scores-row">
                <div class="score-item"><span class="score-label">Line</span><span class="score-val">{pred['Predicted_Spread']:+.1f}</span></div>
                <div class="score-item"><span class="score-label">Total</span><span class="score-val">{pred['Predicted_Total']}</span></div>
            </div>
        </div>
        """

    html += """
        </div>
        <div style="text-align:center; margin-top:30px; font-size:0.7rem; color:#444;">
            Generated by The Bible V9.2
        </div>
    </body>
    </html>
    """
    return html
        
