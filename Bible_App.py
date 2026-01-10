import streamlit as st
import pandas as pd
import numpy as np
import requests
import os
from io import StringIO
# --- IMPORT THE NEW V10 PRODUCTION ENGINE ---
import Bible_Simulator_V10_EXPERIMENTAL as v10_engine

# --- CACHING THE NEW DATABASE (Now returns 6 items) ---
@st.cache_data
def load_v10_data_cached():
    """Cache the heavy V10 database loading so the app runs instantly."""
    return v10_engine.build_team_database()

# ==============================================================================
#   THE BIBLE SCOUT V10.0 - PRODUCTION DASHBOARD
#   Features: 
#   - Side-by-Side: V9.2 (Baseline) vs V10 (PhD-Level)
#   - Visualizes Bayesian & Location Adjustments
# ==============================================================================

# --- CONFIGURATION ---
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
STYLE_PATH = os.path.join(BASE_DIR, "cbb_style_2025_complete.csv")

# --- SIMULATION CONSTANTS (V9.2) ---
BASE_HCA_POINTS = 2.6
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
BLOWOUT_TALENT_THRESHOLD = 25.0
BLOWOUT_MULTIPLIER = 1.15
LOW_MAJOR_RANK_THRESHOLD = 200
LOW_MAJOR_VARIANCE_MULT = 1.4
LUCK_REGRESSION_FACTOR = 0.30
TURNOVER_POINT_VALUE = 1.2
OREB_POSSESSION_RATE = 0.25
SECOND_CHANCE_PPP = 1.05
SOS_VARIANCE_FACTOR = 0.05

# --- PAGE CONFIG ---
st.set_page_config(page_title="Bible Scout V10", page_icon="🏀", layout="centered")

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
#   PART 1: DATA LOADING (LEGACY V9.2 SUPPORT)
# ==============================================================================

@st.cache_data
def load_all_data():
    data = {}
    try:
        url = f"https://kenpom.com/api.php?endpoint=ratings&y={CURRENT_SEASON}"
        headers = {"Authorization": f"Bearer {KP_API_KEY}", "User-Agent": "BibleScout/4.0"}
        r1 = requests.get(url, headers=headers, timeout=15)
        df_ratings = pd.DataFrame(r1.json())
        
        url2 = f"https://kenpom.com/api.php?endpoint=four-factors&y={CURRENT_SEASON}"
        r2 = requests.get(url2, headers=headers, timeout=15)
        df_ff = pd.DataFrame(r2.json())
        
        if 'Rank' not in df_ratings.columns: df_ratings['Rank'] = df_ratings.index + 1
        
        rename_map = {'AdjO': 'AdjOE', 'AdjD': 'AdjDE', 'AdjT': 'AdjTempo', 'SOS_AdjEM': 'SOS'}
        df_ratings = df_ratings.rename(columns=rename_map)
        
        needed_cols = ['TeamName', 'Rank', 'AdjEM', 'AdjOE', 'AdjDE', 'AdjTempo', 'Luck', 'SOS']
        missing = [c for c in needed_cols if c not in df_ratings.columns]
        if missing:
            data['kp'] = pd.DataFrame(); return data
        
        team_stats = df_ratings[needed_cols].rename(columns={'AdjOE': 'Off_Eff', 'AdjDE': 'Def_Eff', 'AdjTempo': 'Tempo'})
        team_stats = team_stats.merge(df_ff[['TeamName', 'OR_Pct', 'DOR_Pct', 'TO_Pct', 'DTO_Pct', 'FT_Rate', 'DFT_Rate']], on='TeamName', how='left')
        team_stats = team_stats.rename(columns={'Off_Eff': 'AdjOE', 'Def_Eff': 'AdjDE', 'Tempo': 'AdjTempo'})
        team_stats['TeamName'] = team_stats['TeamName'].str.replace(' State', ' St.', regex=False).str.replace('Saint ', 'St. ', regex=False)
        data['kp'] = team_stats
        
    except: data['kp'] = pd.DataFrame()
        
    if os.path.exists(STYLE_PATH): data['style'] = pd.read_csv(STYLE_PATH)
    else: data['style'] = pd.DataFrame()

    if os.path.exists(TRACKING_PATH):
        try:
            df_t = pd.read_csv(TRACKING_PATH)
            df_t['Date'] = pd.to_datetime(df_t['Date'])
            data['tracker'] = df_t.drop_duplicates(subset=['Visitor', 'Home', 'Date'])
        except: data['tracker'] = pd.DataFrame()
    else: data['tracker'] = pd.DataFrame()
        
    return data

# ==============================================================================
#   PART 2: V9.2 SIMULATION ENGINE (BASELINE)
# ==============================================================================

def get_style_adj(off, deff, style_db):
    if style_db.empty: return 0.0, ""
    try:
        off_r = style_db[style_db['play_team'].str.contains(off, case=False, na=False)]
        def_r = style_db[style_db['play_team'].str.contains(deff, case=False, na=False)]
        if off_r.empty or def_r.empty: return 0.0, ""
        
        rim = off_r.iloc[0].get('rim_rate', 0); def_rim = def_r.iloc[0].get('opp_rim_rate', 0)
        arc = off_r.iloc[0].get('arc_rate', 0); def_arc = def_r.iloc[0].get('opp_arc_rate', 0)
        
        adj = 0.0; flags = []
        if rim > 55 and def_rim < 50: adj -= 3.0; flags.append("⛔ RIM WALL")
        elif rim > 60 and def_rim > 60: adj += 3.0; flags.append("🟢 LAYUP LINE")
        if arc > 45 and def_arc > 40: adj += 2.5; flags.append("🔥 SHOOTOUT")
        elif arc > 45 and def_arc < 35: adj -= 2.0; flags.append("🛡️ ARC WALL")
        return adj, " ".join(flags)
    except: return 0.0, ""

def run_hypothetical(visitor, home, neutral, data, market_spread=None, market_total=None):
    stats = data['kp']; style = data['style']
    if stats.empty: return None
    
    try:
        v_match = stats[stats['TeamName'] == visitor]
        h_match = stats[stats['TeamName'] == home]
        if v_match.empty: v_match = stats[stats['TeamName'].str.contains(visitor, case=False, na=False)]
        if h_match.empty: h_match = stats[stats['TeamName'].str.contains(home, case=False, na=False)]
        if v_match.empty or h_match.empty: return None
        tv = v_match.iloc[0]; th = h_match.iloc[0]
    except: return None
    
    avg_tempo = stats['AdjTempo'].mean(); avg_eff = stats['AdjOE'].mean()
    geo_tempo = (tv['AdjTempo'] * th['AdjTempo']) / avg_tempo
    tempo = (min(tv['AdjTempo'], th['AdjTempo']) * 0.6 + geo_tempo * 0.4) if abs(tv['AdjTempo'] - th['AdjTempo']) > 5.0 else geo_tempo
    
    v_s, v_f = get_style_adj(visitor, home, style); h_s, h_f = get_style_adj(home, visitor, style)
    oe_v = (tv['AdjOE'] * th['AdjDE']) / avg_eff + (v_s/tempo)*100
    oe_h = (th['AdjOE'] * tv['AdjDE']) / avg_eff + (h_s/tempo)*100
    
    luck_adj = (tv.get('Luck', 0) - th.get('Luck', 0)) * LUCK_REGRESSION_FACTOR
    oe_v += (luck_adj/tempo)*100; oe_h -= (luck_adj/tempo)*100
    
    v_to = (tv.get('DTO_Pct', 0) - th.get('TO_Pct', 0)) * tempo/100 * TURNOVER_POINT_VALUE
    h_to = (th.get('DTO_Pct', 0) - tv.get('TO_Pct', 0)) * tempo/100 * TURNOVER_POINT_VALUE
    v_reb = (tv.get('OR_Pct', 0) - th.get('DOR_Pct', 0)) * tempo * OREB_POSSESSION_RATE/100 * SECOND_CHANCE_PPP
    h_reb = (th.get('OR_Pct', 0) - tv.get('DOR_Pct', 0)) * tempo * OREB_POSSESSION_RATE/100 * SECOND_CHANCE_PPP
    
    mean_v = oe_v * tempo / 100 + v_to + v_reb
    mean_h = oe_h * tempo / 100 + h_to + h_reb
    
    talent_gap = abs(tv['AdjEM'] - th['AdjEM'])
    if talent_gap > BLOWOUT_TALENT_THRESHOLD:
        if mean_h > mean_v: mean_h *= BLOWOUT_MULTIPLIER
        else: mean_v *= BLOWOUT_MULTIPLIER
    
    if not neutral: mean_h += VENUE_HCA.get(home, BASE_HCA_POINTS)
        
    return {
        'V_Score': mean_v, 'H_Score': mean_h, 'Spread': mean_h - mean_v, 
        'Total': mean_v + mean_h, 'Home_Win_Prob': 50 + (mean_h - mean_v)*2.5
    }

# ==============================================================================
#   PART 3: STREAMLIT INTERFACE
# ==============================================================================

db = load_all_data()
kp = db['kp']

st.sidebar.title("BibleOS v10.0")
st.sidebar.caption(f"Engine: V10 Production | Data: {CURRENT_SEASON}")
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
            c1.metric("Rank", row.get("Rank", 0))
            c2.metric("AdjEM", f"{row.get('AdjEM', 0):.1f}")
            c3.metric("Off Eff", f"{row.get('AdjOE', 0):.1f}")
            c4.metric("Luck", f"{row.get('Luck', 0):.3f}")

elif mode == "⚔️ Hypo-Sim":
    st.title("⚔️ The Lab")
    st.caption(f"V10 Production Engine ({CURRENT_SEASON} Stats)")
    
    if not kp.empty:
        all_teams = sorted(kp['TeamName'].unique())
        c1, c2 = st.columns(2)
        visitor = c1.selectbox("Visitor", all_teams, index=None, placeholder="Select visitor...")
        home = c2.selectbox("Home", all_teams, index=None, placeholder="Select home...")
        neutral = st.checkbox("Neutral Court", value=False)
        
        st.markdown("---")
        st.subheader("📊 Market Lines")
        m1, m2 = st.columns(2)
        market_spread = m1.number_input("Market Spread (Home Line)", value=None, step=0.5, format="%.1f")
        market_total = m2.number_input("Market Total", value=None, format="%.1f")
        
        if st.button("🔮 Simulate Matchup", type="primary"):
            if visitor and home and visitor != home:
                
                # --- 1. LOAD V10 DATABASE (6 ITEMS) ---
                with st.spinner("Initializing V10 Engine..."):
                    # THIS IS THE CRITICAL UPDATE: Unpack 6 items instead of 4
                    stats, style, quad, eff, h_perf, r_perf = load_v10_data_cached()
                
                if stats is None:
                    st.error("❌ Error: V10 Database could not be loaded.")
                else:
                    # --- 2. RUN V10 SIMULATION ---
                    results_v10 = v10_engine.run_simulation(
                        visitor, home, stats, style, quad, eff, h_perf, r_perf, market_spread, market_total
                    )
                    
                    # --- 3. RUN V9.2 BASELINE (INTERNAL) ---
                    results_v92 = run_hypothetical(visitor, home, neutral, db)
                    
                    # --- 4. DISPLAY RESULTS ---
                    st.divider()
                    col_base, col_exp = st.columns(2)

                    # === LEFT: V9.2 BASELINE ===
                    with col_base:
                        st.subheader("🛡️ V9.2 (Baseline)")
                        if results_v92:
                            st.markdown(f"""
                            <div class="sim-result-box" style="border-color: #94a3b8;">
                                <div style="font-size: 12px; color: #666;">BASELINE PROJECTION</div>
                                <div class="score-display">
                                    {visitor} {results_v92['V_Score']:.1f}<br>{home} {results_v92['H_Score']:.1f}
                                </div>
                                <div class="spread-display">
                                    Line: {home} {-(results_v92['Spread']):+.1f} | Total: {results_v92['Total']:.1f}
                                </div>
                            </div>
                            """, unsafe_allow_html=True)

                    # === RIGHT: V10 PRODUCTION ===
                    with col_exp:
                        st.subheader("🧪 V10 (Quad & Home/Away Metrics)")
                        
                        # Calculate Deltas
                        spread_delta = results_v10['Predicted_Spread'] - results_v92['Spread']
                        delta_color = "red" if abs(spread_delta) >= 1.5 else "gray"
                        
                        # Fix display line sign
                        v10_disp_line = -results_v10['Predicted_Spread']

                        st.markdown(f"""
                        <div class="sim-result-box" style="border-color: #4f46e5; background-color: #eef2ff;">
                            <div style="font-size: 12px; color: #4338ca;">PhD-LEVEL ADJUSTED</div>
                            <div class="score-display" style="color: #312e81;">
                                {results_v10['Visitor']} {results_v10['V_Score']}<br>{results_v10['Home']} {results_v10['H_Score']}
                            </div>
                            <div class="spread-display" style="color: #3730a3;">
                                Line: {results_v10['Home']} {v10_disp_line:+.1f} 
                                <span style="font-size: 12px; color: {delta_color};">(Δ {spread_delta:+.1f})</span>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

                        # INTELLIGENCE
                        st.write("")
                        if "PhD_Loc" in results_v10['Analysis_Flags']:
                             st.info(f"📍 **Location Logic:** {results_v10['PhD_Reasoning']}")
                        
                        if results_v10['Signals']:
                            st.write("")
                            # Split string signals (e.g., "SPREAD: Team -5...")
                            if isinstance(results_v10['Signals'], str):
                                sigs = results_v10['Signals'].split("; ")
                                for s in sigs: st.success(f"💰 {s}")
                            elif isinstance(results_v10['Signals'], list):
                                for s in results_v10['Signals']: st.success(f"💰 {s}")
            else:
                st.error("Please select both teams.")
