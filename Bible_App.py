import streamlit as st
import pandas as pd
import numpy as np
import requests
import os
from io import StringIO
# --- ADD THESE IMPORTS TO THE TOP OF Bible_App.py ---
import Bible_Simulator_V10_EXPERIMENTAL as v10_engine

# --- ADD THIS CACHING FUNCTION (Put it after your imports) ---
@st.cache_data
def load_v10_data_cached():
    """Cache the heavy V10 database loading so the app runs instantly."""
    return v10_engine.build_team_database()

# ==============================================================================
#   THE BIBLE SCOUT V4.0 - SYNCED WITH V9.2 SIMULATOR
#   Features: 
#   - Updated to match V9.2 logic exactly
#   - Proper variance handling (separate for spread vs total)
#   - V9.2 adjustments: Blowout detection, Low-major variance, Total caps
#   - Cleaner display with model consistency
# ==============================================================================

# --- CONFIGURATION ---
# Read API key securely from Streamlit secrets (for deployment)
# or environment variable (for local testing)
try:
    KP_API_KEY = st.secrets["KP_API_KEY"]
except:
    import os
    KP_API_KEY = os.environ.get("KP_API_KEY", "")
    if not KP_API_KEY:
        st.error("⚠️ KenPom API key not found. Please configure in Streamlit secrets.")
        st.stop()

CURRENT_SEASON = 2026
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRACKING_PATH = os.path.join(BASE_DIR, "Performance_Tracker_V9_2.csv")
STYLE_PATH = os.path.join(BASE_DIR, "cbb_style_2025_complete.csv")# --- SIMULATION CONSTANTS (V9.2) ---
BASE_HCA_POINTS = 2.6  # V9.2 reduced from 2.9
VENUE_HCA = {
    'Duke': 4.2, 'Kentucky': 3.9, 'Kansas': 3.7, 'Gonzaga': 3.6, 'Villanova': 3.5,
    'Syracuse': 3.5, 'Louisville': 3.4, 'Michigan St.': 3.3, 'Wisconsin': 3.3,
    'North Carolina': 3.2, 'Arizona': 3.2, 'Virginia': 3.1, 'Purdue': 3.1,
    'Iowa St.': 3.0, 'Butler': 3.0, 'Creighton': 2.9, 'San Diego St.': 2.9,
    'Northwestern': 1.5, 'DePaul': 1.6, 'Georgia Tech': 1.7, 'Boston College': 1.8,
    'Rutgers': 2.0, 'Nebraska': 2.1, 'Wake Forest': 2.2,
}
BASE_VARIANCE_SPREAD = 11.5
BASE_VARIANCE_TOTAL = 9.5
SIM_RUNS = 3000

# --- V9.2 NEW PARAMETERS ---
BLOWOUT_TALENT_THRESHOLD = 25.0
BLOWOUT_MULTIPLIER = 1.15
LOW_MAJOR_RANK_THRESHOLD = 200
LOW_MAJOR_VARIANCE_MULT = 1.4

# --- ADJUSTMENT WEIGHTS ---
LUCK_REGRESSION_FACTOR = 0.30
TURNOVER_POINT_VALUE = 1.2
OREB_POSSESSION_RATE = 0.25
SECOND_CHANCE_PPP = 1.05
SOS_VARIANCE_FACTOR = 0.05

# --- PAGE CONFIG ---
st.set_page_config(page_title="Bible Scout V4.0", page_icon="🏀", layout="centered")

# --- CSS ---
st.markdown("""
    <style>
    .metric-card { background: white; border: 1px solid #e0e0e0; border-radius: 10px; padding: 10px; text-align: center; flex: 1; box-shadow: 0 2px 4px rgba(0,0,0,0.03); }
    .metric-label { font-size: 10px; color: #666; text-transform: uppercase; font-weight: 600; }
    .metric-val { font-size: 18px; font-weight: 800; color: #2c3e50; margin-top: 2px; }
    .good { color: #16a34a; }
    .bad { color: #dc2626; }
    .sim-result-box { background-color: #f8fafc; border: 2px solid #4f46e5; border-radius: 12px; padding: 20px; text-align: center; margin-top: 20px; }
    .score-display { font-size: 32px; font-weight: 900; color: #1e293b; margin: 10px 0; }
    .spread-display { font-size: 18px; color: #64748b; font-weight: 600; }
    .stat-row { display: flex; justify-content: space-between; font-size: 14px; padding: 4px 0; border-bottom: 1px solid #f0f0f0; }
    .warning-box { background: #fff3cd; border: 1px solid #ffc107; padding: 10px; border-radius: 8px; margin: 10px 0; }
    </style>
""", unsafe_allow_html=True)

# ==============================================================================
#   PART 1: DATA LOADING
# ==============================================================================

@st.cache_data
def load_all_data():
    data = {}
    try:
        # KenPom Ratings - using exact same logic as working simulator
        url = f"https://kenpom.com/api.php?endpoint=ratings&y={CURRENT_SEASON}"
        headers = {"Authorization": f"Bearer {KP_API_KEY}", "User-Agent": "BibleScout/4.0"}
        r1 = requests.get(url, headers=headers, timeout=15)
        df_ratings = pd.DataFrame(r1.json())
        
        # Four Factors
        url2 = f"https://kenpom.com/api.php?endpoint=four-factors&y={CURRENT_SEASON}"
        r2 = requests.get(url2, headers=headers, timeout=15)
        df_ff = pd.DataFrame(r2.json())
        
        # Generate Rank if missing (data comes sorted by rank)
        if 'Rank' not in df_ratings.columns:
            df_ratings['Rank'] = df_ratings.index + 1
        
        # EXACT RENAME LOGIC FROM WORKING SIMULATOR
        rename_map = {
            'AdjO': 'AdjOE',
            'AdjD': 'AdjDE',
            'AdjT': 'AdjTempo',
            'SOS_AdjEM': 'SOS'
        }
        df_ratings = df_ratings.rename(columns=rename_map)
        
        # Select needed columns
        needed_cols = ['TeamName', 'Rank', 'AdjEM', 'AdjOE', 'AdjDE', 'AdjTempo', 'Luck', 'SOS']
        
        # Safety check
        missing = [c for c in needed_cols if c not in df_ratings.columns]
        if missing:
            st.error(f"⚠️ API Error: Missing columns: {', '.join(missing)}")
            st.info(f"Available: {', '.join(df_ratings.columns)}")
            data['kp'] = pd.DataFrame()
            return data
        
        # Format as in simulator
        team_stats = df_ratings[needed_cols].rename(columns={
            'AdjOE': 'Off_Eff', 'AdjDE': 'Def_Eff', 'AdjTempo': 'Tempo'
        })
        
        # Merge with Four Factors
        team_stats = team_stats.merge(
            df_ff[['TeamName', 'OR_Pct', 'DOR_Pct', 'TO_Pct', 'DTO_Pct', 'FT_Rate', 'DFT_Rate']], 
            on='TeamName', how='left'
        )
        
        # Rename back for consistency with simulation code
        team_stats = team_stats.rename(columns={
            'Off_Eff': 'AdjOE', 'Def_Eff': 'AdjDE', 'Tempo': 'AdjTempo'
        })
        
        # Clean team names
        team_stats['TeamName'] = team_stats['TeamName'].str.replace(' State', ' St.', regex=False).str.replace('Saint ', 'St. ', regex=False)
        
        data['kp'] = team_stats
        
    except Exception as e:
        st.error(f"Error loading KenPom: {e}")
        data['kp'] = pd.DataFrame()
        
    # Style DB
    if os.path.exists(STYLE_PATH):
        data['style'] = pd.read_csv(STYLE_PATH)
    else:
        data['style'] = pd.DataFrame()

    # Tracker
    if os.path.exists(TRACKING_PATH):
        try:
            df_t = pd.read_csv(TRACKING_PATH)
            df_t['Date'] = pd.to_datetime(df_t['Date'])
            df_t = df_t.drop_duplicates(subset=['Visitor', 'Home', 'Date'])
            data['tracker'] = df_t
        except:
            data['tracker'] = pd.DataFrame()
    else:
        data['tracker'] = pd.DataFrame()
        
    return data

# ==============================================================================
#   PART 2: V9.2 SIMULATION ENGINE
# ==============================================================================

def get_style_adj(off, deff, style_db):
    if style_db.empty: return 0.0, ""
    try:
        off_r = style_db[style_db['play_team'].str.contains(off, case=False, na=False)]
        def_r = style_db[style_db['play_team'].str.contains(deff, case=False, na=False)]
        if off_r.empty or def_r.empty: return 0.0, ""
        
        rim = off_r.iloc[0].get('rim_rate', 0)
        def_rim = def_r.iloc[0].get('opp_rim_rate', 0)
        arc = off_r.iloc[0].get('arc_rate', 0)
        def_arc = def_r.iloc[0].get('opp_arc_rate', 0)
        
        adj = 0.0
        flags = []
        if rim > 55 and def_rim < 50: 
            adj -= 3.0
            flags.append("⛔ RIM WALL")
        elif rim > 60 and def_rim > 60:
            adj += 3.0
            flags.append("🟢 LAYUP LINE")
        
        if arc > 45 and def_arc > 40:
            adj += 2.5
            flags.append("🔥 SHOOTOUT")
        elif arc > 45 and def_arc < 35:
            adj -= 2.0
            flags.append("🛡️ ARC WALL")
            
        return adj, " ".join(flags)
    except:
        return 0.0, ""

def calculate_dynamic_variance(tempo, talent_diff, sos_diff, is_close_game, ft_rate_combined, base_variance):
    tempo_factor = tempo / 68.0
    blowout_factor = max(0.85, 1.0 - (talent_diff / 200.0))
    sos_factor = max(0.90, min(1.10, 1.0 - (sos_diff * SOS_VARIANCE_FACTOR)))
    close_game_factor = 1.15 if is_close_game else 1.0
    ft_factor = max(0.90, min(1.05, 1.0 - (max(0, ft_rate_combined - 30) / 100)))
    return base_variance * tempo_factor * blowout_factor * sos_factor * close_game_factor * ft_factor

def detect_blowout_potential(team_v, team_h):
    """V9.2: Detect talent mismatches"""
    talent_gap = abs(team_v.get('AdjEM', 0) - team_h.get('AdjEM', 0))
    if talent_gap > BLOWOUT_TALENT_THRESHOLD:
        return BLOWOUT_MULTIPLIER, "🔴 BLOWOUT ALERT"
    return 1.0, ""

def apply_low_major_penalty(team_v, team_h, variance_spread, variance_total):
    """V9.2: Increase variance for low-major games"""
    v_rank = team_v.get('Rank', 0)
    h_rank = team_h.get('Rank', 0)
    if v_rank > LOW_MAJOR_RANK_THRESHOLD or h_rank > LOW_MAJOR_RANK_THRESHOLD:
        return variance_spread * LOW_MAJOR_VARIANCE_MULT, variance_total * 1.3, "⚠️ LOW-MAJOR"
    return variance_spread, variance_total, ""

def apply_total_sanity_caps(predicted_total, expected_tempo):
    """V9.2: Cap extreme totals"""
    max_reasonable = expected_tempo * 2.2
    min_reasonable = expected_tempo * 1.4
    if predicted_total > max_reasonable:
        return max_reasonable, "⬇️ CAPPED HIGH"
    elif predicted_total < min_reasonable:
        return min_reasonable, "⬆️ CAPPED LOW"
    return predicted_total, ""

def calculate_betting_edge(res, market_spread, market_total, visitor, home):
    """Calculate betting edges and Kelly sizing based on market lines"""
    edges = []
    
    # --- SPREAD EDGE ---
    if market_spread is not None:
        model_line = -1 * res['Spread']  # Convert home margin to visitor spread
        spread_diff = abs(model_line - market_spread)
        
        if spread_diff >= 2.0:  # Edge threshold
            # Determine bet side
            if model_line < market_spread:
                bet_team = home
                bet_home = True
            else:
                bet_team = visitor
                bet_home = False
            
            # Get cover probability
            if bet_home:
                is_fav = market_spread < 0
            else:
                is_fav = market_spread > 0
            
            bet_prob = res['Cover_Prob_Fav'] if is_fav else res['Cover_Prob_Dog']
            
            # Kelly sizing
            kelly_pct = max(0, min(((bet_prob / 100) - 0.5) / 0.5, 0.05)) * 100
            
            # Display line
            line_disp = f"-{abs(market_spread)}" if is_fav else f"+{abs(market_spread)}"
            
            edges.append({
                'Type': 'SPREAD',
                'Bet': f"{bet_team} {line_disp}",
                'Edge': spread_diff,
                'Probability': bet_prob,
                'Kelly': kelly_pct,
                'Confidence': 'HIGH' if spread_diff >= 4.0 else 'MEDIUM'
            })
    
    # --- TOTAL EDGE ---
    if market_total is not None:
        total_diff = abs(res['Total'] - market_total)
        
        if total_diff >= 3.0:  # Edge threshold
            bet_side = 'OVER' if res['Total'] > market_total else 'UNDER'
            bet_prob = res['Over_Prob'] if bet_side == 'OVER' else res['Under_Prob']
            
            # Kelly sizing
            kelly_pct = max(0, min(((bet_prob / 100) - 0.5) / 0.5, 0.05)) * 100
            
            edges.append({
                'Type': 'TOTAL',
                'Bet': f"{bet_side} {market_total}",
                'Edge': total_diff,
                'Probability': bet_prob,
                'Kelly': kelly_pct,
                'Confidence': 'HIGH' if total_diff >= 5.0 else 'MEDIUM'
            })
    
    return edges

def run_hypothetical(visitor, home, neutral, data, market_spread=None, market_total=None):
    stats = data['kp']
    style = data['style']
    
    if stats.empty: return None
    
    # Get Teams
    try:
        v_match = stats[stats['TeamName'] == visitor]
        h_match = stats[stats['TeamName'] == home]
        
        if v_match.empty: v_match = stats[stats['TeamName'].str.contains(visitor, case=False, na=False)]
        if h_match.empty: h_match = stats[stats['TeamName'].str.contains(home, case=False, na=False)]
        
        if v_match.empty or h_match.empty: return None
            
        tv = v_match.iloc[0]
        th = h_match.iloc[0]
    except:
        return None
    
    # Check required columns with detailed error message
    required_cols = ['AdjOE', 'AdjDE', 'AdjTempo', 'Luck', 'SOS', 'Rank']
    missing_v = [col for col in required_cols if col not in tv.index or pd.isna(tv.get(col))]
    missing_h = [col for col in required_cols if col not in th.index or pd.isna(th.get(col))]
    
    if missing_v or missing_h:
        st.error(f"⚠️ Data Error:")
        if missing_v:
            st.write(f"**{visitor}** missing: {', '.join(missing_v)}")
        if missing_h:
            st.write(f"**{home}** missing: {', '.join(missing_h)}")
        st.info("💡 Try refreshing data or selecting different teams")
        return None
        
    # Calculate base stats
    avg_tempo = stats['AdjTempo'].mean()
    avg_eff = stats['AdjOE'].mean()
    
    # Tempo calculation
    geo_tempo = (tv['AdjTempo'] * th['AdjTempo']) / avg_tempo
    tempo_diff = abs(tv['AdjTempo'] - th['AdjTempo'])
    if tempo_diff > 5.0:
        tempo = (min(tv['AdjTempo'], th['AdjTempo']) * 0.6 + geo_tempo * 0.4)
    else:
        tempo = geo_tempo
    
    # Style adjustments
    v_s, v_f = get_style_adj(visitor, home, style)
    h_s, h_f = get_style_adj(home, visitor, style)
    
    # Efficiency with style
    oe_v = (tv['AdjOE'] * th['AdjDE']) / avg_eff + (v_s/tempo)*100
    oe_h = (th['AdjOE'] * tv['AdjDE']) / avg_eff + (h_s/tempo)*100
    
    # Luck regression
    luck_adj = (tv.get('Luck', 0) - th.get('Luck', 0)) * LUCK_REGRESSION_FACTOR
    oe_v += (luck_adj/tempo)*100
    oe_h -= (luck_adj/tempo)*100
    
    # Four Factors
    v_to_pts = (tv.get('DTO_Pct', 0) - th.get('TO_Pct', 0)) * tempo/100 * TURNOVER_POINT_VALUE
    h_to_pts = (th.get('DTO_Pct', 0) - tv.get('TO_Pct', 0)) * tempo/100 * TURNOVER_POINT_VALUE
    v_oreb_pts = (tv.get('OR_Pct', 0) - th.get('DOR_Pct', 0)) * tempo * OREB_POSSESSION_RATE/100 * SECOND_CHANCE_PPP
    h_oreb_pts = (th.get('OR_Pct', 0) - tv.get('DOR_Pct', 0)) * tempo * OREB_POSSESSION_RATE/100 * SECOND_CHANCE_PPP
    
    # Base scores
    mean_v = oe_v * tempo / 100 + v_to_pts + v_oreb_pts
    mean_h = oe_h * tempo / 100 + h_to_pts + h_oreb_pts
    
    # V9.2: Blowout detection
    blowout_mult, blowout_flag = detect_blowout_potential(tv, th)
    if blowout_mult > 1.0:
        if mean_h > mean_v:
            mean_h *= blowout_mult
        else:
            mean_v *= blowout_mult
    
    # HCA
    hca_val = 0.0
    if not neutral:
        hca_val = VENUE_HCA.get(home, BASE_HCA_POINTS)
        mean_h += hca_val
        
    # Variance calculation
    talent = abs(tv['AdjOE'] - th['AdjOE']) + abs(tv['AdjDE'] - th['AdjDE'])
    sos = tv.get('SOS', 0) - th.get('SOS', 0)
    close = abs(mean_h - mean_v) < 6
    ft = (tv.get('FT_Rate', 30) + th.get('DFT_Rate', 30)) / 2
    
    var_spread = calculate_dynamic_variance(tempo, talent, sos, close, ft, BASE_VARIANCE_SPREAD)
    var_total = calculate_dynamic_variance(tempo, talent, sos, close, ft, BASE_VARIANCE_TOTAL)
    
    # V9.2: Low-major variance adjustment
    var_spread, var_total, low_major_flag = apply_low_major_penalty(tv, th, var_spread, var_total)
    
    # Monte Carlo simulations
    np.random.seed(None)
    sim_v_spread = np.random.normal(mean_v, var_spread, SIM_RUNS)
    sim_h_spread = np.random.normal(mean_h, var_spread, SIM_RUNS)
    sim_v_total = np.random.normal(mean_v, var_total, SIM_RUNS)
    sim_h_total = np.random.normal(mean_h, var_total, SIM_RUNS)
    
    # Results from SPREAD simulation (for displayed scores)
    avg_v_spread = np.mean(sim_v_spread)
    avg_h_spread = np.mean(sim_h_spread)
    
    # Results from TOTAL simulation (for predicted total)
    pred_total = np.mean(sim_v_total) + np.mean(sim_h_total)
    
    # V9.2: Total sanity caps
    pred_total, total_cap_flag = apply_total_sanity_caps(pred_total, tempo)
    
    win_prob = np.mean(sim_h_spread > sim_v_spread) * 100
    
    # Calculate cover probabilities if market lines provided
    cover_prob_fav = 0.0
    cover_prob_dog = 0.0
    over_prob = 0.0
    under_prob = 0.0
    
    if market_spread is not None:
        home_favored = market_spread < 0
        spread_mag = abs(market_spread)
        if home_favored:
            cover_prob_fav = np.mean((sim_h_spread - sim_v_spread) > spread_mag) * 100
        else:
            cover_prob_fav = np.mean((sim_v_spread - sim_h_spread) > spread_mag) * 100
        cover_prob_dog = 100.0 - cover_prob_fav
    
    if market_total is not None:
        over_prob = np.mean((sim_v_total + sim_h_total) > market_total) * 100
        under_prob = 100.0 - over_prob
    
    # Collect flags
    flags = []
    if blowout_flag: flags.append(blowout_flag)
    if low_major_flag: flags.append(low_major_flag)
    if total_cap_flag: flags.append(total_cap_flag)
    if v_f: flags.append(f"V: {v_f}")
    if h_f: flags.append(f"H: {h_f}")
    
    return {
        'V_Score': avg_v_spread, 'H_Score': avg_h_spread,
        'Spread': avg_h_spread - avg_v_spread, 
        'Total': pred_total,  # From total simulation
        'HCA': hca_val, 'Tempo': tempo, 'Home_Win_Prob': win_prob,
        'V_Stats': tv, 'H_Stats': th,
        'Variance_Spread': var_spread, 'Variance_Total': var_total,
        'V_TO_Impact': v_to_pts, 'H_TO_Impact': h_to_pts,
        'V_OREB_Impact': v_oreb_pts, 'H_OREB_Impact': h_oreb_pts,
        'Cover_Prob_Fav': cover_prob_fav, 'Cover_Prob_Dog': cover_prob_dog,
        'Over_Prob': over_prob, 'Under_Prob': under_prob,
        'Flags': flags
    }

# ==============================================================================
#   PART 3: STREAMLIT INTERFACE
# ==============================================================================

db = load_all_data()
kp = db['kp']

st.sidebar.title("BibleOS v4.0")
st.sidebar.caption(f"Engine: V9.2 | Data: {CURRENT_SEASON}")
st.sidebar.info("⚡ Synced with V9.2 Simulator\n\n✅ Proper variance handling\n✅ Blowout detection\n✅ Low-major adjustments")

if st.sidebar.button("🔄 Refresh Data", type="primary"):
    st.cache_data.clear()
    st.rerun()

mode = st.sidebar.radio("Select Mode", ["🔍 Scout Team", "⚔️ Hypo-Sim"])

if mode == "🔍 Scout Team":
    st.title("🏀 Bible Scout")
    if not kp.empty:
        all_teams = sorted(kp['TeamName'].unique())
        team = st.selectbox("Search Team", all_teams, index=None, placeholder="Type to search...")
        
        if team:
            row = kp[kp['TeamName'] == team].iloc[0]
            st.header(f"{team}")
            
            c1, c2, c3, c4 = st.columns(4)
            luck = row.get('Luck', 0)
            rank = row.get('Rank', 0)
            
            c1.markdown(f'<div class="metric-card"><div class="metric-label">Rank</div><div class="metric-val">{rank}</div></div>', unsafe_allow_html=True)
            c2.markdown(f'<div class="metric-card"><div class="metric-label">Adj EM</div><div class="metric-val">{row.get("AdjEM", 0):.1f}</div></div>', unsafe_allow_html=True)
            c3.markdown(f'<div class="metric-card"><div class="metric-label">Off Eff</div><div class="metric-val">{row.get("AdjOE", 0):.1f}</div></div>', unsafe_allow_html=True)
            c4.markdown(f'<div class="metric-card"><div class="metric-label">Luck</div><div class="metric-val {"bad" if luck > 0.05 else "good" if luck < -0.05 else ""}">{luck:.3f}</div></div>', unsafe_allow_html=True)
            
            st.subheader("Recent Performance")
            tracker = db['tracker']
            if not tracker.empty:
                hist = tracker[(tracker['Visitor'] == team) | (tracker['Home'] == team)].sort_values('Date', ascending=False)
                if not hist.empty:
                    for _, g in hist.head(5).iterrows():
                        opp = g['Visitor'] if g['Home'] == team else g['Home']
                        st.caption(f"{g['Date'].strftime('%m/%d')} vs {opp}: {g['V_Score']:.0f} - {g['H_Score']:.0f}")
                else: st.info("No tracked games yet.")
            else: st.info("No tracker file found.")

elif mode == "⚔️ Hypo-Sim":
    st.title("⚔️ The Lab")
    st.caption(f"V9.2 Model Engine ({CURRENT_SEASON} Stats)")
    
    if not kp.empty:
        all_teams = sorted(kp['TeamName'].unique())
        c1, c2 = st.columns(2)
        visitor = c1.selectbox("Visitor", all_teams, index=None, placeholder="Select visitor...")
        home = c2.selectbox("Home", all_teams, index=None, placeholder="Select home...")
        neutral = st.checkbox("Neutral Court", value=False)
        
        # Market lines input
        st.markdown("---")
        st.subheader("📊 Market Lines (Optional)")
        m1, m2 = st.columns(2)
        market_spread = m1.number_input(
            "Market Spread", 
            value=None, 
            step=0.5, 
            format="%.1f",
            help="Enter home team's spread. E.g., -10.5 means home favored by 10.5"
        )
        market_total = m2.number_input(
            "Market Total", 
            value=None,
            format="%.1f",
            help="Enter the over/under total"
        )
        
# --- REPLACE THE OLD "Simulate Matchup" BUTTON WITH THIS BLOCK ---
        if st.button("🔮 Simulate Matchup", type="primary"):
            if visitor and home and visitor != home:
                
                # 1. Load V10 Data (Cached)
                with st.spinner("Initializing V10 Experimental Engine..."):
                    team_stats, style_db, quad_data, eff_profiles = load_v10_data_cached()
                
                if team_stats is None:
                    st.error("❌ Error: V10 Database could not be loaded. Check file paths.")
                else:
                    # 2. Run the Comparison Simulation
                    # Note: We pass the 'market_spread' and 'market_total' from your inputs
                    results_v10, results_v92 = v10_engine.run_comparison_test(
                        visitor=visitor, 
                        home=home, 
                        market_spread=market_spread, 
                        market_total=market_total
                    )
                    
                    # 3. Create Side-by-Side Layout
                    st.divider()
                    col_base, col_exp = st.columns(2)

                    # === LEFT COLUMN: V9.2 BASELINE ===
                    with col_base:
                        st.subheader("🛡️ V9.2 PROVEN (Baseline)")
                        st.markdown(f"""
                        <div class="sim-result-box" style="border-color: #94a3b8;">
                            <div style="font-size: 12px; color: #666; margin-bottom: 5px;">BASELINE PROJECTION</div>
                            <div class="score-display" style="font-size: 26px;">
                                {results_v92['Visitor']} {results_v92['V_Score']} — {results_v92['Home']} {results_v92['H_Score']}
                            </div>
                            <div style="margin-top: 10px; border-top: 1px solid #e2e8f0; padding-top: 10px;">
                                <div class="spread-display" style="font-size: 16px;">
                                    Line: {results_v92['Home']} {results_v92['Predicted_Spread']:+.1f} | Total: {results_v92['Predicted_Total']:.1f}
                                </div>
                                <div style="margin-top: 5px; font-weight: bold; color: #475569;">
                                    Win Prob: {results_v92['Home_Win_Prob']}%
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Display V9.2 Signals
                        if results_v92['Signals']:
                            st.write("")
                            for sig in results_v92['Signals']:
                                color = "#16a34a" if sig['Confidence'] == 'HIGH' else "#2563eb"
                                st.markdown(f"""
                                <div style="background: #f0fdf4; border-left: 4px solid {color}; padding: 10px; border-radius: 4px; margin-bottom: 5px;">
                                    <b>{sig['Type']}:</b> {sig['Bet']} ({sig['Confidence']})
                                </div>
                                """, unsafe_allow_html=True)

                    # === RIGHT COLUMN: V10 EXPERIMENTAL ===
                    with col_exp:
                        st.subheader("🧪 V10 EXPERIMENTAL (Beta)")
                        
                        # Calculate Deltas
                        spread_delta = results_v10['Predicted_Spread'] - results_v92['Predicted_Spread']
                        prob_delta = results_v10['Home_Win_Prob'] - results_v92['Home_Win_Prob']
                        
                        # Highlight notable deltas
                        delta_color = "red" if abs(spread_delta) >= 1.5 else "gray"
                        
                        st.markdown(f"""
                        <div class="sim-result-box" style="border-color: #4f46e5; background-color: #eef2ff;">
                            <div style="font-size: 12px; color: #4338ca; margin-bottom: 5px;">EXPERIMENTAL ADJUSTED</div>
                            <div class="score-display" style="font-size: 26px; color: #312e81;">
                                {results_v10['Visitor']} {results_v10['V_Score']} — {results_v10['Home']} {results_v10['H_Score']}
                            </div>
                            <div style="margin-top: 10px; border-top: 1px solid #c7d2fe; padding-top: 10px;">
                                <div class="spread-display" style="font-size: 16px; color: #3730a3;">
                                    Line: {results_v10['Home']} {results_v10['Predicted_Spread']:+.1f} 
                                    <span style="font-size: 12px; color: {delta_color};">(Δ {spread_delta:+.1f})</span>
                                </div>
                                <div style="margin-top: 5px; font-weight: bold; color: #312e81;">
                                    Win Prob: {results_v10['Home_Win_Prob']}% 
                                    <span style="font-size: 12px; color: #666;">(Δ {prob_delta:+.1f}%)</span>
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

                        # Display "Contextual Intelligence" (Bayesian Adjustments)
                        st.write("")
                        
                        # Visitor Adjustment
                        if abs(results_v10['V_Bayes_Adj']) > 0.1:
                            v_color = "#dc2626" if results_v10['V_Bayes_Adj'] < 0 else "#16a34a"
                            st.markdown(f"""
                            <div style="font-size: 13px; margin-bottom: 5px;">
                                📉 <b>{visitor} Adj:</b> <span style="color:{v_color}; font-weight:bold;">{results_v10['V_Bayes_Adj']:+.1f} pts</span>
                                <br><i style="color:#666;">{results_v10.get('V_Bayes_Reasoning', '').split('(')[0]}</i>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        # Home Adjustment
                        if abs(results_v10['H_Bayes_Adj']) > 0.1:
                            h_color = "#dc2626" if results_v10['H_Bayes_Adj'] < 0 else "#16a34a"
                            st.markdown(f"""
                            <div style="font-size: 13px; margin-bottom: 5px;">
                                📉 <b>{home} Adj:</b> <span style="color:{h_color}; font-weight:bold;">{results_v10['H_Bayes_Adj']:+.1f} pts</span>
                                <br><i style="color:#666;">{results_v10.get('H_Bayes_Reasoning', '').split('(')[0]}</i>
                            </div>
                            """, unsafe_allow_html=True)

                        # V10 Signals (If different from V9.2)
                        if results_v10['Signals']:
                            st.divider()
                            st.caption("🧪 V10 Specific Signals")
                            for sig in results_v10['Signals']:
                                st.success(f"💰 {sig['Type']}: {sig['Bet']} ({sig['Confidence']})")
            else:
                st.error("Please select both a Visitor and Home team.")
