import requests
import pandas as pd
import numpy as np
import os
import sys
from io import StringIO
from datetime import datetime, timedelta
from scipy import stats
from typing import Tuple, Dict, List, Optional
import warnings

warnings.filterwarnings('ignore')

# Define the base directory relative to this file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ==============================================================================
#   THE BIBLE V10.0 - PRODUCTION AUTOMATION EDITION
#   
#   Features:
#   1. "Daily Slate" Automator (Uses KenPom FanMatch with Date)
#   2. Automatic CSV Report Generation
#   3. Full Integration of PhD-Level Location & Bayesian Adjustments
# ==============================================================================

# --- CONFIGURATION ---
KP_API_KEY = "18ee6ae93a94ade96fc899611578ef42f57ad96e09845cb585ee3b2aab1444fa"
BASE_HCA_POINTS = 2.6
STYLE_DB_PATH = os.path.join(BASE_DIR, "cbb_style_2025_complete.csv")
QUADRANT_DATA_PATH = os.path.join(BASE_DIR, "team_quadrant_analysis_2026.csv")
ADJUSTED_EFF_PATH = os.path.join(BASE_DIR, "team_adjusted_efficiency_profiles_2026.csv")

# --- PhD-LEVEL LOCATION DATA ---
HOME_PERF_FILE = "team_home_performance_VALIDATED_2026.csv"
ROAD_PERF_FILE = "team_road_performance_VALIDATED_2026.csv"
LOCATION_SCALING = 0.35

CONFIDENCE_WEIGHTS = {'HIGH': 1.0, 'MEDIUM': 0.7, 'LOW': 0.3, 'INSUFFICIENT': 0.0}

# ==============================================================================
# MASTER TRANSLATION DICTIONARY (COMPLETE)
# ==============================================================================
KENPOM_TRANSLATION = {
    # --- ACC ---
    "Boston College Eagles": "Boston College", "California Golden Bears": "California",
    "Clemson Tigers": "Clemson", "Duke Blue Devils": "Duke", "Florida State Seminoles": "Florida St.",
    "Georgia Tech Yellow Jackets": "Georgia Tech", "Louisville Cardinals": "Louisville",
    "Miami Hurricanes": "Miami FL", "NC State Wolfpack": "N.C. State",
    "North Carolina Tar Heels": "North Carolina", "Notre Dame Fighting Irish": "Notre Dame",
    "Pittsburgh Panthers": "Pittsburgh", "SMU Mustangs": "SMU", "Stanford Cardinal": "Stanford",
    "Syracuse Orange": "Syracuse", "Virginia Cavaliers": "Virginia",
    "Virginia Tech Hokies": "Virginia Tech", "Wake Forest Demon Deacons": "Wake Forest",

    # --- BIG 12 ---
    "Arizona Wildcats": "Arizona", "Arizona State Sun Devils": "Arizona St.",
    "Baylor Bears": "Baylor", "BYU Cougars": "BYU", "UCF Golden Knights": "UCF",
    "Cincinnati Bearcats": "Cincinnati", "Colorado Buffaloes": "Colorado",
    "Houston Cougars": "Houston", "Iowa State Cyclones": "Iowa St.", "Kansas Jayhawks": "Kansas",
    "Kansas State Wildcats": "Kansas St.", "Oklahoma State Cowboys": "Oklahoma St.",
    "TCU Horned Frogs": "TCU", "Texas Tech Red Raiders": "Texas Tech", "Utah Utes": "Utah",
    "West Virginia Mountaineers": "West Virginia",

    # --- BIG EAST ---
    "Butler Bulldogs": "Butler", "Connecticut Huskies": "Connecticut", "UConn Huskies": "Connecticut",
    "Creighton Bluejays": "Creighton", "DePaul Blue Demons": "DePaul", "Georgetown Hoyas": "Georgetown",
    "Marquette Golden Eagles": "Marquette", "Providence Friars": "Providence",
    "Seton Hall Pirates": "Seton Hall", "St. John's Red Storm": "St. John's",
    "Villanova Wildcats": "Villanova", "Xavier Musketeers": "Xavier",

    # --- BIG TEN ---
    "Illinois Fighting Illini": "Illinois", "Indiana Hoosiers": "Indiana", "Iowa Hawkeyes": "Iowa",
    "Maryland Terrapins": "Maryland", "Michigan Wolverines": "Michigan",
    "Michigan State Spartans": "Michigan St.", "Minnesota Golden Gophers": "Minnesota",
    "Nebraska Cornhuskers": "Nebraska", "Northwestern Wildcats": "Northwestern",
    "Ohio State Buckeyes": "Ohio St.", "Oregon Ducks": "Oregon", "Penn State Nittany Lions": "Penn St.",
    "Purdue Boilermakers": "Purdue", "Rutgers Scarlet Knights": "Rutgers", "UCLA Bruins": "UCLA",
    "USC Trojans": "USC", "Washington Huskies": "Washington", "Wisconsin Badgers": "Wisconsin",

    # --- SEC ---
    "Alabama Crimson Tide": "Alabama", "Arkansas Razorbacks": "Arkansas", "Auburn Tigers": "Auburn",
    "Florida Gators": "Florida", "Georgia Bulldogs": "Georgia", "Kentucky Wildcats": "Kentucky",
    "LSU Tigers": "LSU", "Mississippi Rebels": "Ole Miss", "Ole Miss Rebels": "Ole Miss",
    "Mississippi State Bulldogs": "Mississippi St.", "Missouri Tigers": "Missouri",
    "Oklahoma Sooners": "Oklahoma", "South Carolina Gamecocks": "South Carolina",
    "Tennessee Volunteers": "Tennessee", "Texas Longhorns": "Texas", "Texas A&M Aggies": "Texas A&M",
    "Vanderbilt Commodores": "Vanderbilt",

    # --- MOUNTAIN WEST ---
    "Air Force Falcons": "Air Force", "Boise State Broncos": "Boise St.", "Colorado State Rams": "Colorado St.",
    "Fresno State Bulldogs": "Fresno St.", "Grand Canyon Antelopes": "Grand Canyon",
    "Nevada Wolf Pack": "Nevada", "New Mexico Lobos": "New Mexico", "San Diego State Aztecs": "San Diego St.",
    "San José State Spartans": "San Jose St.", "UNLV Runnin' Rebels": "UNLV",
    "Utah State Aggies": "Utah St.", "Wyoming Cowboys": "Wyoming",

    # --- WCC ---
    "Gonzaga Bulldogs": "Gonzaga", "Loyola Marymount Lions": "LMU", "Oregon State Beavers": "Oregon St.",
    "Pacific Tigers": "Pacific", "Pepperdine Waves": "Pepperdine", "Portland Pilots": "Portland",
    "Saint Mary's Gaels": "Saint Mary's", "San Diego Toreros": "San Diego",
    "San Francisco Dons": "San Francisco", "Santa Clara Broncos": "Santa Clara",
    "Seattle U Redhawks": "Seattle", "Washington State Cougars": "Washington St.",

    # --- GENERAL / MID-MAJORS ---
    "Albany Great Danes": "Albany", "UAB Blazers": "UAB", "VCU Rams": "VCU",
    "FAU Owls": "Florida Atlantic", "FIU Panthers": "FIU",
    "Middle Tennessee Blue Raiders": "Middle Tennessee", "Louisiana Tech Bulldogs": "Louisiana Tech",
    "Liberty Flames": "Liberty", "Dayton Flyers": "Dayton", "Saint Louis Billikens": "Saint Louis",
    "Loyola Chicago Ramblers": "Loyola Chicago", "Memphis Tigers": "Memphis",
    "South Florida Bulls": "South Florida", "Wichita State Shockers": "Wichita St.",
    "College of Charleston Cougars": "Charleston", "UNC Wilmington Seahawks": "UNCW",
    
    # --- COMMON FIXES ---
    "Miami (FL)": "Miami FL", "Miami (OH)": "Miami OH", "Saint Peter's": "Saint Peter's",
    "St. Peter's": "Saint Peter's", "St. Mary's": "Saint Mary's", "Ole Miss": "Mississippi",
    "Mississippi": "Mississippi", "UConn": "Connecticut", "Pitt": "Pittsburgh",
    "Southern Miss": "Southern Miss", "UL Monroe": "ULM", "Louisiana-Monroe": "ULM",
    "Louisiana Lafayette": "Louisiana", "Louisiana-Lafayette": "Louisiana", "UT Rio Grande Valley": "UTRGV",
    "UTRGV Vaqueros": "UTRGV", "Texas A&M-Corpus Christi": "Texas A&M Corpus Chris",
    "St. Thomas (MN)": "St. Thomas", "LIU": "Long Island", "Long Island University": "Long Island",
    "Kansas City Roos": "Kansas City", "UMKC": "Kansas City", "Omaha": "Nebraska Omaha",
    "Purdue Fort Wayne": "Purdue Fort Wayne", "IPFW": "Purdue Fort Wayne", "Green Bay": "Green Bay",
    "Milwaukee": "Milwaukee", "Detroit Mercy": "Detroit Mercy", "IUPUI": "IU Indy", "IU Indianapolis": "IU Indy",
    "Southwestern Christian": None, "Washington Adventist": None, "Southern Wesleyan": None,
}

def standardize_name(name):
    if not isinstance(name, str): return str(name)
    name = name.strip()
    return KENPOM_TRANSLATION.get(name, name)

# --- PARAMETERS ---
BASE_VARIANCE_TOTAL = 9.5
BASE_VARIANCE_SPREAD = 11.5
SIM_RUNS = 5000
LUCK_REGRESSION_FACTOR = 0.30
TURNOVER_POINT_VALUE = 1.2
OREB_POSSESSION_RATE = 0.25
SECOND_CHANCE_PPP = 1.05
BLOWOUT_TALENT_THRESHOLD = 25.0
BLOWOUT_MULTIPLIER = 1.15
BAYESIAN_PRIOR_WEIGHT = 4.0
QUADRANT_CREDIBILITY_THRESHOLD = 3
MAX_QUADRANT_ADJUSTMENT = 6.0
EB_VARIANCE_ALPHA = 0.15
LOW_MAJOR_RANK_THRESHOLD = 200
LOW_MAJOR_VARIANCE_MULT = 1.4
TOTAL_EDGE_THRESHOLD = 3.0
SPREAD_EDGE_THRESHOLD = 2.0
HIGH_CONFIDENCE_TOTAL = 5.0
HIGH_CONFIDENCE_SPREAD = 4.0

# ======================================================
# SECTION 1: DATA LOADING
# ======================================================

def get_kenpom_data(endpoint, arg=None):
    """
    Fetch data from KenPom API.
    Supports 'y' (Year) for ratings or 'd' (Date) for FanMatch.
    """
    # Default to current season if no arg provided, otherwise use the arg provided (like &d=...)
    query_param = f"&y={2026}" if arg is None else arg
    
    url = f"https://kenpom.com/api.php?endpoint={endpoint}{query_param}"
    headers = {"Authorization": f"Bearer {KP_API_KEY}", "User-Agent": "TheBibleModel/10.0-PROD"}
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        if endpoint == "misc-stats": 
            return pd.read_csv(StringIO(response.text))
        return pd.DataFrame(response.json())
    except Exception as e:
        print(f"⚠️  API Error ({endpoint}): {e}")
        return None

def get_daily_schedule():
    """Fetch today's games from KenPom FanMatch."""
    print("📅 Fetching Today's Schedule (FanMatch)...")
    
    # 1. Get Today's Date in YYYY-MM-DD format (Required by API)
    today_str = datetime.now().strftime('%Y-%m-%d')
    date_param = f"&d={today_str}"
    
    # 2. Call FanMatch with the date parameter
    df = get_kenpom_data("fanmatch", arg=date_param)
    
    if df is not None and not df.empty:
        # Standardize names immediately
        if 'Visitor' in df.columns: df['Visitor'] = df['Visitor'].apply(standardize_name)
        if 'Home' in df.columns: df['Home'] = df['Home'].apply(standardize_name)
        print(f"   ✅ Found {len(df)} games scheduled for {today_str}.")
        return df
        
    print(f"   ⚠️ No games found for {today_str} or API error.")
    return None

def load_quadrant_data():
    if not os.path.exists(QUADRANT_DATA_PATH): return None
    df = pd.read_csv(QUADRANT_DATA_PATH)
    df['Team'] = df['Team'].apply(standardize_name)
    return df

def load_efficiency_profiles():
    if not os.path.exists(ADJUSTED_EFF_PATH): return None
    df = pd.read_csv(ADJUSTED_EFF_PATH)
    df['Team'] = df['Team'].apply(standardize_name)
    return df

def load_validated_location_data():
    try:
        if not os.path.exists(HOME_PERF_FILE) or not os.path.exists(ROAD_PERF_FILE):
            print("   ℹ️  Validated location data not found. Using standard HCA.")
            return None, None
        h_df = pd.read_csv(HOME_PERF_FILE); r_df = pd.read_csv(ROAD_PERF_FILE)
        h_df['Team'] = h_df['Team'].apply(standardize_name)
        r_df['Team'] = r_df['Team'].apply(standardize_name)
        print(f"   ✅ Validated location data: {len(h_df)} home, {len(r_df)} road profiles")
        return h_df, r_df
    except Exception as e:
        print(f"   ⚠️  Could not load validated location data: {e}"); return None, None

def build_team_database():
    print("🏗️  Building Enhanced Team Database (V10)...")
    # These calls use the default 'y=2026' behavior
    ratings = get_kenpom_data("ratings")
    factors = get_kenpom_data("four-factors")
    
    if ratings is None or factors is None: return None, None, None, None, None, None
    
    if 'Rank' not in ratings.columns: ratings['Rank'] = ratings.index + 1
    ratings = ratings.rename(columns={'AdjO':'AdjOE', 'AdjD':'AdjDE', 'AdjT':'AdjTempo', 'SOS_AdjEM':'SOS'})
    ratings['TeamName'] = ratings['TeamName'].apply(standardize_name)
    factors['TeamName'] = factors['TeamName'].apply(standardize_name)
    
    stats = ratings[['TeamName', 'Rank', 'AdjEM', 'AdjOE', 'AdjDE', 'AdjTempo', 'Luck', 'SOS']].rename(columns={
        'AdjOE':'Off_Eff', 'AdjDE':'Def_Eff', 'AdjTempo':'Tempo'
    })
    stats = stats.merge(factors[['TeamName', 'OR_Pct', 'DOR_Pct', 'TO_Pct', 'DTO_Pct', 'FT_Rate', 'DFT_Rate']], on='TeamName', how='left')
    
    style = pd.DataFrame()
    if os.path.exists(STYLE_DB_PATH):
        try: style = pd.read_csv(STYLE_DB_PATH); style['Team'] = style['Team'].apply(standardize_name)
        except: pass
        
    quad = load_quadrant_data(); eff = load_efficiency_profiles()
    h_perf, r_perf = load_validated_location_data()
    print(f"   ✓ Core stats: {len(stats)} teams")
    return stats, style, quad, eff, h_perf, r_perf

# ======================================================
# SECTION 2: ADJUSTMENT LOGIC
# ======================================================

def calculate_validated_location_adjustment(home_team, visitor_team, visitor_rank, home_df, road_df):
    if home_df is None or road_df is None: return 0.0, 0.0, "", "NO_DATA"
    
    if visitor_rank <= 30: v_quad = 'Q1'
    elif visitor_rank <= 75: v_quad = 'Q2'
    elif visitor_rank <= 160: v_quad = 'Q3'
    else: v_quad = 'Q4'
    
    h_data = home_df[home_df['Team'] == home_team]
    if h_data.empty: h_adj=0.0; h_conf='NO_DATA'; h_reason=f"{home_team}: No home data"
    else:
        h_data = h_data.iloc[0]
        q_shrunk = f'{v_quad}_NetEff_Shrunk'; q_conf = f'{v_quad}_Confidence'
        if pd.notna(h_data.get(q_shrunk)) and h_data.get(q_conf) not in ['LOW', 'INSUFFICIENT', None]:
            h_adj = h_data[q_shrunk] * LOCATION_SCALING * CONFIDENCE_WEIGHTS.get(h_data[q_conf], 0.3)
            h_adj = np.clip(h_adj, -4.0, 4.0)
            h_conf = h_data[q_conf]
            h_reason = f"{home_team} Home vs {v_quad}: {h_data[q_shrunk]:+.1f} (Adj: {h_adj:+.1f})"
        else:
            h_adj = h_data['Overall_NetEff'] * LOCATION_SCALING * 0.5
            h_adj = np.clip(h_adj, -2.5, 2.5)
            h_conf = 'LOW'
            h_reason = f"{home_team} Home (Overall): {h_data['Overall_NetEff']:+.1f} (Adj: {h_adj:+.1f})"

    v_data = road_df[road_df['Team'] == visitor_team]
    if v_data.empty: v_adj=0.0; v_conf='NO_DATA'; v_reason=f"{visitor_team}: No road data"
    else:
        v_data = v_data.iloc[0]
        games = v_data['Total_Games']
        weight = 1.0 if games >= 7 else (0.7 if games >= 5 else 0.5)
        v_conf = 'HIGH' if games >= 7 else ('MEDIUM' if games >= 5 else 'LOW')
        v_adj = v_data['Overall_NetEff'] * LOCATION_SCALING * weight
        v_adj = np.clip(v_adj, -3.0, 3.0)
        v_reason = f"{visitor_team} Road: {v_data['Overall_NetEff']:+.1f} (Adj: {v_adj:+.1f})"
        
    reasoning = f"Loc: {h_reason} | {v_reason}"
    conf_map = {'NO_DATA':0, 'LOW':1, 'MEDIUM':2, 'HIGH':3}
    final_conf = [k for k,v in conf_map.items() if v == min(conf_map.get(h_conf,0), conf_map.get(v_conf,0))][0]
    return h_adj, v_adj, reasoning, final_conf

def compute_bayesian_quadrant_adjustment(team, opp_rank, quad_data):
    if quad_data is None or quad_data.empty: return 0.0, "", ""
    row = quad_data[quad_data['Team'] == team]
    if row.empty: return 0.0, "", ""
    row = row.iloc[0]
    
    if opp_rank <= 50: quad = 'Q1'
    elif opp_rank <= 100: quad = 'Q2'
    elif opp_rank <= 200: quad = 'Q3'
    else: quad = 'Q4'
    
    try:
        games = row[f'{quad}_Games']; net_eff = row[f'{quad}_NetEff']
        base_eff = row.get('AdjNetEff', 0); consistency = row.get('ConsistencyScore', 15.0)
    except: return 0.0, "", ""
    
    if pd.isna(net_eff) or games < QUADRANT_CREDIBILITY_THRESHOLD: return 0.0, "", ""
    shrinkage = games / (games + BAYESIAN_PRIOR_WEIGHT)
    delta = net_eff - base_eff
    if net_eff > 0 and delta < 0: delta = 0.0 
    adj = delta * shrinkage
    if consistency < 12.0: adj *= 1.2
    elif consistency > 20.0: adj *= 0.7
    adj = np.clip(adj, -MAX_QUADRANT_ADJUSTMENT, MAX_QUADRANT_ADJUSTMENT)
    flag = f"BAYES: {adj:+.1f} vs {quad}" if abs(adj) > 1.5 else ""
    return adj, flag, ""

def calculate_four_factors_edge(v, h):
    turnover = ((h.get('DTO_Pct',18)-v.get('TO_Pct',18)) - (v.get('DTO_Pct',18)-h.get('TO_Pct',18))) * TURNOVER_POINT_VALUE
    reb = ((h.get('OR_Pct',28)-v.get('DOR_Pct',28)) - (v.get('OR_Pct',28)-h.get('DOR_Pct',28))) * OREB_POSSESSION_RATE * SECOND_CHANCE_PPP
    return turnover + reb

def estimate_variance(v, h, v_rank, h_rank, gap, eff, base):
    var = base
    if v_rank > LOW_MAJOR_RANK_THRESHOLD or h_rank > LOW_MAJOR_RANK_THRESHOLD: var *= LOW_MAJOR_VARIANCE_MULT
    if eff is not None:
        v_d = eff[eff['Team'] == v]; h_d = eff[eff['Team'] == h]
        if not v_d.empty and not h_d.empty:
            emp = np.mean([v_d.iloc[0].get('OffEffStd', 15), h_d.iloc[0].get('DefEffStd', 15)])
            var = (1 - EB_VARIANCE_ALPHA) * var + EB_VARIANCE_ALPHA * emp
    if abs(gap) > BLOWOUT_TALENT_THRESHOLD: var *= 0.85
    return var

# ======================================================
# SECTION 3: CORE SIMULATION ENGINE
# ======================================================

def run_simulation(v_name, h_name, stats, style, quad, eff, h_perf, r_perf, spread=None, total=None):
    v = stats[stats['TeamName'] == v_name]; h = stats[stats['TeamName'] == h_name]
    if v.empty or h.empty: return {"error": "Team not found", "Visitor": v_name, "Home": h_name}
    v = v.iloc[0].to_dict(); h = h.iloc[0].to_dict()
    
    # 1. Base Efficiency
    v_off = v['Off_Eff'] - v.get('Luck',0) * LUCK_REGRESSION_FACTOR
    h_off = h['Off_Eff'] - h.get('Luck',0) * LUCK_REGRESSION_FACTOR
    v_def = v['Def_Eff']; h_def = h['Def_Eff']
    
    # 2. Bayesian Adj
    v_adj, v_flag, _ = compute_bayesian_quadrant_adjustment(v_name, h['Rank'], quad)
    h_adj, h_flag, _ = compute_bayesian_quadrant_adjustment(h_name, v['Rank'], quad)
    v_off += v_adj/2; v_def -= v_adj/2
    h_off += h_adj/2; h_def -= h_adj/2
    
    # 3. Location Adj
    h_loc, v_loc, loc_reason, loc_conf = calculate_validated_location_adjustment(h_name, v_name, v['Rank'], h_perf, r_perf)
    h_off += h_loc; v_off += v_loc
    
    # 4. Tempo & Scoring
    tempo = (v['Tempo'] * h['Tempo']) / 68.5
    v_ppp = (v_off * h_def) / 106.0; h_ppp = (h_off * v_def) / 106.0
    h_ppp += calculate_four_factors_edge(v, h) / tempo
    if loc_conf == 'NO_DATA': h_ppp += 2.6 / tempo
    
    # 5. Result
    v_score = (v_ppp * tempo) / 100.0; h_score = (h_ppp * tempo) / 100.0
    margin = h_score - v_score; proj_total = v_score + h_score
    
    # 6. Blowout
    gap = abs(v['AdjEM'] - h['AdjEM'])
    if gap > BLOWOUT_TALENT_THRESHOLD:
        margin *= BLOWOUT_MULTIPLIER
        if h_score > v_score: h_score = v_score + margin
        else: v_score = h_score - margin
        proj_total = v_score + h_score
        
    # 7. Monte Carlo
    np.random.seed(42)
    s_var = estimate_variance(v_name, h_name, v['Rank'], h['Rank'], gap, eff, BASE_VARIANCE_SPREAD)
    sims = np.random.normal(margin, s_var, SIM_RUNS)
    win_prob = np.mean(sims > 0) * 100
    
    # 8. Signals
    signals = []
    if spread is not None:
        market_margin = -spread 
        edge = abs(margin - market_margin)
        if edge >= SPREAD_EDGE_THRESHOLD:
            if margin > market_margin: side = h_name; bet_line = spread
            else: side = v_name; bet_line = -spread
            conf = "HIGH" if edge >= HIGH_CONFIDENCE_SPREAD else "MEDIUM"
            signals.append(f"SPREAD: {side} {bet_line:+.1f} (Edge: {edge:.1f}, {conf})")
            
    if total is not None:
        edge = abs(proj_total - total)
        if edge >= TOTAL_EDGE_THRESHOLD:
            side = "OVER" if proj_total > total else "UNDER"
            conf = "HIGH" if edge >= HIGH_CONFIDENCE_TOTAL else "MEDIUM"
            signals.append(f"TOTAL: {side} {total} (Edge: {edge:.1f}, {conf})")

    flags = []
    if v_flag: flags.append(f"V: {v_flag}")
    if h_flag: flags.append(f"H: {h_flag}")
    if loc_conf != 'NO_DATA': flags.append(f"PhD_Loc: {h_loc-v_loc:+.1f} Net")
    
    return {
        'Visitor': v_name, 'Home': h_name,
        'V_Score': round(v_score, 1), 'H_Score': round(h_score, 1),
        'Predicted_Spread': round(margin, 1), 'Predicted_Total': round(proj_total, 1),
        'Home_Win_Prob': round(win_prob, 1),
        'Signals': "; ".join(signals),
        'Analysis_Flags': " | ".join(flags),
        'PhD_Reasoning': loc_reason if loc_conf != 'NO_DATA' else "Standard HCA Used"
    }

# ======================================================
# SECTION 4: INTERFACE & AUTOMATION
# ======================================================

def run_daily_automation(stats, style, quad, eff, h_perf, r_perf):
    """Fetch schedule, run all games, save report."""
    schedule = get_daily_schedule()
    if schedule is None: return

    print(f"\n🚀 Running V10 Simulation on {len(schedule)} Games...")
    results = []
    
    for i, row in schedule.iterrows():
        v = row['Visitor']; h = row['Home']
        # Try to get market lines if FanMatch provides them, else None
        spread = None; total = None 
        
        res = run_simulation(v, h, stats, style, quad, eff, h_perf, r_perf, spread, total)
        if "error" not in res:
            # Format betting line for display (favorites negative)
            disp_line = -res['Predicted_Spread']
            
            results.append({
                'Time': row.get('Time', 'N/A'),
                'Visitor': v, 'Home': h,
                'V_Score': res['V_Score'], 'H_Score': res['H_Score'],
                'Model_Line': disp_line,
                'Model_Total': res['Predicted_Total'],
                'Win_Prob': res['Home_Win_Prob'],
                'Signals': res['Signals'],
                'Intelligence': res['Analysis_Flags']
            })
            # Print brief progress
            if i % 10 == 0: print(f"   ... Processed {i+1}/{len(schedule)} games")

    # Save Report
    if results:
        df_res = pd.DataFrame(results)
        filename = f"V10_Betting_Sheet_{datetime.now().strftime('%Y-%m-%d')}.csv"
        df_res.to_csv(filename, index=False)
        print(f"\n✅ DONE! processed {len(results)} games.")
        print(f"📄 Betting Sheet Saved: {filename}")

def run_single_game(stats, style, quad, eff, h_perf, r_perf):
    v = input("Visitor: "); h = input("Home: ")
    s = input("Spread (opt, Home Line e.g. -5.5): ")
    t = input("Total (opt): ")
    res = run_simulation(v, h, stats, style, quad, eff, h_perf, r_perf, float(s) if s else None, float(t) if t else None)
    
    if "error" in res: print(f"❌ Error: {res['error']}"); return

    disp_line = -res['Predicted_Spread']
    print(f"\n📊 PREDICTION:")
    print(f"   Score: {res['Visitor']} {res['V_Score']} - {res['Home']} {res['H_Score']}")
    print(f"   Line:  {res['Home']} {disp_line:+.1f}")
    print(f"   Total: {res['Predicted_Total']:.1f}")
    print(f"   Win%:  {res['Home_Win_Prob']}%")
    print(f"\n🧠 INTELLIGENCE:")
    print(f"   Flags: {res['Analysis_Flags']}")
    print(f"   Location Logic: {res['PhD_Reasoning']}")
    if res['Signals']: print(f"\n💰 SIGNALS: {res['Signals']}")
    print("="*80 + "\n")

if __name__ == "__main__":
    # Load Data Once
    stats, style, quad, eff, h_perf, r_perf = build_team_database()
    
    if stats is not None:
        # CHECK FOR AUTOMATION FLAG (Added for Batch File)
        if len(sys.argv) > 1 and sys.argv[1] == "auto":
            print("\n🤖 AUTOMATION MODE DETECTED")
            run_daily_automation(stats, style, quad, eff, h_perf, r_perf)
        
        # STANDARD INTERACTIVE MODE
        else:
            while True:
                print("\nTHE BIBLE V10 (PRODUCTION)")
                print("1. Predict Single Game")
                print("2. Run Full Daily Schedule (FanMatch)")
                print("3. Exit")
                choice = input("Select: ")
                
                if choice == "1": run_single_game(stats, style, quad, eff, h_perf, r_perf)
                elif choice == "2": run_daily_automation(stats, style, quad, eff, h_perf, r_perf)
                elif choice == "3": break
