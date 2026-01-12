import streamlit as st
import pandas as pd
import numpy as np
import requests
import os
from datetime import datetime
import Bible_Simulator_V10_EXPERIMENTAL as v10_engine

# --- CACHING FUNCTION ---
@st.cache_data
def load_v10_data_cached():
    """Safely loads V10 database."""
    data = v10_engine.build_team_database()
    if len(data) == 7: # Updated to 7 items with market lines
        return data
    elif len(data) == 6:
        s, st_db, q, e, h, r = data
        return s, st_db, q, e, h, r, None
    else:
        return None, None, None, None, None, None, None

# --- CONFIGURATION ---
try:
    KP_API_KEY = st.secrets["KP_API_KEY"]
except:
    import os
    KP_API_KEY = os.environ.get("KP_API_KEY", "")

CURRENT_SEASON = 2026
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRACKING_PATH = os.path.join(BASE_DIR, "Performance_Tracker_V9_2.csv")
STYLE_PATH = os.path.join(BASE_DIR, "cbb_style_2025_complete.csv")

# --- PAGE CONFIG ---
st.set_page_config(page_title="Bible Scout V10", page_icon="🏀", layout="wide")

# --- CSS ---
st.markdown("""
    <style>
    .metric-card { background: #ffffff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 15px; text-align: center; box-shadow: 0 1px 2px rgba(0,0,0,0.05); }
    .metric-label { font-size: 11px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.5px; font-weight: 600; }
    .metric-val { font-size: 24px; font-weight: 800; color: #111827; margin-top: 4px; }
    .herf-badge { background: #4f46e5; color: white; padding: 4px 12px; border-radius: 20px; font-weight: 700; font-size: 14px; display: inline-block; }
    
    .scout-header { border-bottom: 2px solid #f3f4f6; padding-bottom: 15px; margin-bottom: 20px; display: flex; align-items: center; justify-content: space-between; }
    .team-title { font-size: 36px; font-weight: 900; color: #1f2937; }
    
    .report-card-table { font-size: 13px; width: 100%; border-collapse: collapse; }
    .report-card-table th { text-align: center; color: #6b7280; font-weight: 600; padding: 8px; border-bottom: 1px solid #e5e7eb; background: #f9fafb; }
    .report-card-table td { padding: 8px; border-bottom: 1px solid #f3f4f6; color: #374151; text-align: center; }
    .win { color: #166534; font-weight: 700; background: #dcfce7; padding: 2px 6px; border-radius: 4px; }
    .loss { color: #991b1b; font-weight: 700; background: #fee2e2; padding: 2px 6px; border-radius: 4px; }
    .market-col { border-left: 2px solid #e5e7eb; }
    </style>
""", unsafe_allow_html=True)

# --- LOAD DATA ---
stats, style, quad, eff, h_perf, r_perf, market_lines = load_v10_data_cached()

# --- LOAD TRACKER ---
tracker_df = pd.DataFrame()
if os.path.exists(TRACKING_PATH):
    try:
        tracker_df = pd.read_csv(TRACKING_PATH)
        tracker_df['Date'] = pd.to_datetime(tracker_df['Date'])
    except: pass

# ==============================================================================
#   HERF RANK ALGORITHM
# ==============================================================================
def calculate_herf_score(row, team_name, tracker):
    # 1. Base Efficiency (50%)
    base_score = row['AdjEM'] * 0.6
    
    # 2. Fundamentals (20%) - Glass & Ball Security
    fund_score = (row['OR_Pct'] - 29.0) * 0.3 - (row['TO_Pct'] - 18.0) * 0.4
    
    # 3. Market Trend (30%) - "The Hot Hand"
    trend_score = 0.0
    trend_desc = "No Recent Games"
    
    if not tracker.empty:
        games = tracker[(tracker['Visitor'] == team_name) | (tracker['Home'] == team_name)].sort_values('Date', ascending=False).head(5)
        if not games.empty:
            covers = []
            for _, g in games.iterrows():
                is_home = g['Home'] == team_name
                actual_margin = g['H_Score'] - g['V_Score'] if is_home else g['V_Score'] - g['H_Score']
                if 'Closing_Spread' in g and pd.notna(g['Closing_Spread']):
                    spread = g['Closing_Spread'] # Spread is always Home Spread
                    # If Home (-5), Actual +10 -> Cover by 15.
                    # If Visitor, Spread -5 (Home Fav), Actual (Visitor loses by 2) -> Cover by 3.
                    cover_margin = (actual_margin - (-spread)) if is_home else (-(actual_margin) - spread)
                    covers.append(cover_margin)
            
            if covers:
                avg_cover = np.mean(covers)
                trend_score = avg_cover * 0.8
                icon = "🔥" if avg_cover > 0 else "❄️"
                trend_desc = f"{icon} ATS Margin: {avg_cover:+.1f} (L{len(covers)})"
    
    total_herf = base_score + fund_score + trend_score
    return total_herf, trend_desc

# --- APPLY HERF RANK ---
if stats is not None:
    herf_data = []
    for _, row in stats.iterrows():
        score, trend = calculate_herf_score(row, row['TeamName'], tracker_df)
        herf_data.append({'TeamName': row['TeamName'], 'Herf_Score': score, 'Herf_Trend': trend})
    
    herf_df = pd.DataFrame(herf_data)
    herf_df['Herf_Rank'] = herf_df['Herf_Score'].rank(ascending=False).astype(int)
    stats = stats.merge(herf_df, on='TeamName')

# ==============================================================================
#   V9.2 BASELINE ENGINE (INTERNAL FOR REPORT CARD)
# ==============================================================================
def run_base_simulation(visitor, home, data_pack):
    # Simplified V9.2 Logic for Comparison
    try:
        kp = data_pack['kp']
        tv = kp[kp['TeamName'] == visitor].iloc[0]
        th = kp[kp['TeamName'] == home].iloc[0]
        
        avg_eff = kp['AdjOE'].mean(); avg_tempo = kp['AdjTempo'].mean()
        tempo = (tv['AdjTempo'] * th['AdjTempo']) / avg_tempo
        
        oe_v = (tv['AdjOE'] * th['AdjDE']) / avg_eff
        oe_h = (th['AdjOE'] * tv['AdjDE']) / avg_eff
        
        # Add HCA (Standard 2.6)
        mean_v = oe_v * tempo / 100
        mean_h = (oe_h * tempo / 100) + 2.6
        
        return mean_h - mean_v # Spread (Home Margin)
    except: return None

# ==============================================================================
#   REPORT CARD GENERATOR
# ==============================================================================
def generate_report_card(team_name, tracker_df):
    if tracker_df is None or tracker_df.empty: return []
    
    team_games = tracker_df[(tracker_df['Visitor'] == team_name) | (tracker_df['Home'] == team_name)].copy()
    team_games = team_games.sort_values('Date', ascending=False)
    
    report = []
    # Package data for Base Sim
    data_pack = {'kp': stats, 'style': style} 
    
    for _, game in team_games.iterrows():
        is_home = (game['Home'] == team_name)
        opponent = game['Visitor'] if is_home else game['Home']
        
        v_score = game['V_Score']; h_score = game['H_Score']
        actual_home_margin = h_score - v_score 
        
        # 1. Market Line
        market_spread = game.get('Closing_Spread', None) # Home Spread (e.g. -5.5)
        
        # 2. V10 Projection
        try:
            res_v10 = v10_engine.run_simulation(game['Visitor'], game['Home'], stats, style, quad, eff, h_perf, r_perf)
            v10_margin = res_v10['Predicted_Spread'] # Home Margin
        except: v10_margin = 0
            
        # 3. Base Projection
        base_margin = run_base_simulation(game['Visitor'], game['Home'], data_pack)
        if base_margin is None: base_margin = 0
        
        # 4. Grading
        v10_grade = "-"; base_grade = "-"
        
        if pd.notna(market_spread):
            # "Winning" means being closer to the actual result than the market line
            market_err = abs(actual_home_margin - (-market_spread))
            v10_err = abs(actual_home_margin - v10_margin)
            base_err = abs(actual_home_margin - base_margin)
            
            # Did V10 find value?
            if v10_err < market_err: v10_grade = "✅" 
            else: v10_grade = "❌"
            
            # Did Base find value?
            if base_err < market_err: base_grade = "✅"
            else: base_grade = "❌"

        # Format for Display (All Perspective of Home Team Spread)
        # We display what the line WAS for the Home Team
        market_disp = f"{market_spread}" if pd.notna(market_spread) else "-"
        # Convert margins to spreads (Margin +5 = Spread -5)
        v10_disp = f"{-v10_margin:.1f}"
        base_disp = f"{-base_margin:.1f}"
        result_disp = f"{'W' if (is_home and h_score > v_score) or (not is_home and v_score > h_score) else 'L'} {h_score}-{v_score}"

        report.append({
            'Date': game['Date'].strftime('%m/%d'),
            'Matchup': f"{'@ ' if not is_home else 'vs '}{opponent}",
            'Result': result_disp,
            'Mkt_Line': market_disp,
            'Base_Proj': base_disp,
            'Base_G': base_grade,
            'V10_Proj': v10_disp,
            'V10_G': v10_grade
        })
    return report

# --- MAIN APP ---
st.sidebar.title("BibleOS v10.0")
mode = st.sidebar.radio("Select Mode", ["🔍 Scout Team", "⚔️ The Lab"])

if mode == "🔍 Scout Team":
    if stats is None:
        st.error("Data not loaded.")
        st.stop()
        
    all_teams = sorted(stats['TeamName'].unique())
    col1, col2 = st.columns([1, 3])
    with col1:
        team = st.selectbox("Select Team", all_teams, index=None, placeholder="Search...")
    
    if team:
        row = stats[stats['TeamName'] == team].iloc[0]
        herf_rank = row['Herf_Rank']
        kp_rank = row['Rank']
        trend_text = row['Herf_Trend']
        
        st.markdown(f"""
        <div class="scout-header">
            <div>
                <div class="team-title">{team}</div>
                <div class="herf-badge">Herf Rank #{herf_rank}</div>
                <span style="font-size:14px; font-weight:600; color:#6b7280; margin-left:10px;">(KenPom #{kp_rank})</span>
            </div>
            <div style="text-align:right;">
                <div style="font-size:12px; color:#6b7280; font-weight:600;">MARKET TREND</div>
                <div style="font-size:16px; font-weight:700; color:#111827;">{trend_text}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        tab_overview, tab_quads, tab_report = st.tabs(["📊 Overview", "🎯 Quadrant Analysis", "📝 Model Report Card"])
        
        with tab_overview:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Adj. Efficiency", f"{row['AdjEM']:.1f}")
            c2.metric("Offense", f"{row['AdjOE']:.1f}")
            c3.metric("Defense", f"{row['AdjDE']:.1f}")
            c4.metric("Tempo", f"{row['AdjTempo']:.1f}")
            
            st.divider()
            st.caption("Fundamentals (Herf Score Inputs)")
            f1, f2, f3, f4 = st.columns(4)
            f1.metric("Off Reb %", f"{row.get('OR_Pct', 0):.1f}%")
            f2.metric("Turnover %", f"{row.get('TO_Pct', 0):.1f}%")
            f3.metric("eFG%", f"{row.get('eFG_Pct', 0):.1f}%")
            f4.metric("FT Rate", f"{row.get('FT_Rate', 0):.1f}")

        with tab_quads:
            if quad is not None and not quad.empty:
                q_row = quad[quad['Team'] == team]
                if not q_row.empty:
                    q_data = q_row.iloc[0]
                    q_display = pd.DataFrame({
                        'Metric': ['Record', 'Net Rating', 'Offense', 'Defense'],
                        'Q1 (Elite)': [f"{q_data.get('Q1_Wins',0)}-{q_data.get('Q1_Losses',0)}", f"{q_data.get('Q1_NetEff', 0):+.1f}", f"{q_data.get('Q1_OffEff', 0):.1f}", f"{q_data.get('Q1_DefEff', 0):.1f}"],
                        'Q2 (Good)': [f"{q_data.get('Q2_Wins',0)}-{q_data.get('Q2_Losses',0)}", f"{q_data.get('Q2_NetEff', 0):+.1f}", f"{q_data.get('Q2_OffEff', 0):.1f}", f"{q_data.get('Q2_DefEff', 0):.1f}"],
                        'Q3 (Avg)': [f"{q_data.get('Q3_Wins',0)}-{q_data.get('Q3_Losses',0)}", f"{q_data.get('Q3_NetEff', 0):+.1f}", f"{q_data.get('Q3_OffEff', 0):.1f}", f"{q_data.get('Q3_DefEff', 0):.1f}"],
                        'Q4 (Bad)': [f"{q_data.get('Q4_Wins',0)}-{q_data.get('Q4_Losses',0)}", f"{q_data.get('Q4_NetEff', 0):+.1f}", f"{q_data.get('Q4_OffEff', 0):.1f}", f"{q_data.get('Q4_DefEff', 0):.1f}"]
                    })
                    st.table(q_display.set_index('Metric'))
                else: st.warning("No Quadrant Data.")
            else: st.error("Database Error.")

        with tab_report:
            report_data = generate_report_card(team, tracker_df)
            if report_data:
                st.write("**Model Accuracy Audit** (Did we beat the market?)")
                rep_df = pd.DataFrame(report_data)
                
                html = """<table class='report-card-table'>
                <thead>
                    <tr>
                        <th style="text-align:left;">Date</th>
                        <th style="text-align:left;">Matchup</th>
                        <th>Result</th>
                        <th class="market-col">Market</th>
                        <th>Base Proj</th>
                        <th>Grade</th>
                        <th>V10 Proj</th>
                        <th>Grade</th>
                    </tr>
                </thead><tbody>"""
                
                for _, g in rep_df.iterrows():
                    html += f"""
                    <tr>
                        <td style="text-align:left;">{g['Date']}</td>
                        <td style="text-align:left;">{g['Matchup']}</td>
                        <td>{g['Result']}</td>
                        <td class="market-col" style="font-weight:bold;">{g['Mkt_Line']}</td>
                        <td>{g['Base_Proj']}</td>
                        <td>{g['Base_G']}</td>
                        <td style="font-weight:bold; color:#4f46e5;">{g['V10_Proj']}</td>
                        <td>{g['V10_G']}</td>
                    </tr>"""
                html += "</tbody></table>"
                st.markdown(html, unsafe_allow_html=True)
            else: st.info("No tracked games found.")

elif mode == "⚔️ The Lab":
    st.title("⚔️ The Lab")
    if stats is not None:
        all_teams = sorted(stats['TeamName'].unique())
        c1, c2 = st.columns(2)
        visitor = c1.selectbox("Visitor", all_teams, index=None, placeholder="Select visitor...")
        home = c2.selectbox("Home", all_teams, index=None, placeholder="Select home...")
        neutral = st.checkbox("Neutral Court", value=False)
        
        st.markdown("---")
        def_spread = None; def_total = None
        if visitor and home and market_lines is not None:
            match = market_lines[(market_lines['Visitor'] == visitor) & (market_lines['Home'] == home)]
            if not match.empty:
                def_spread = float(match.iloc[0]['Market_Spread_Home'])
                def_total = float(match.iloc[0]['Market_Total'])
                st.success(f"💡 Found Market Line: {home} {def_spread} | Total {def_total}")

        m1, m2 = st.columns(2)
        market_spread = m1.number_input("Market Spread (Home Line)", value=def_spread, step=0.5, format="%.1f")
        market_total = m2.number_input("Market Total", value=def_total, format="%.1f")
        
        if st.button("🔮 Simulate Matchup", type="primary"):
            if visitor and home and visitor != home:
                with st.spinner("Running V10 Engine..."):
                    results_v10 = v10_engine.run_simulation(visitor, home, stats, style, quad, eff, h_perf, r_perf, market_spread, market_total)
                    st.divider()
                    st.subheader("🧪 V10 Production Model")
                    v10_disp_line = -results_v10['Predicted_Spread']
                    st.markdown(f"""
                    <div class="sim-result-box" style="border-color: #4f46e5; background-color: #eef2ff;">
                        <div style="font-size: 12px; color: #4338ca;">PhD-LEVEL ADJUSTED</div>
                        <div class="score-display" style="color: #312e81;">{results_v10['Visitor']} {results_v10['V_Score']}<br>{results_v10['Home']} {results_v10['H_Score']}</div>
                        <div class="spread-display" style="color: #3730a3;">Line: {results_v10['Home']} {v10_disp_line:+.1f}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    st.write("")
                    if "PhD_Loc" in results_v10['Analysis_Flags']: st.info(f"📍 **Location Logic:** {results_v10['PhD_Reasoning']}")
                    if results_v10['Signals']:
                        st.write("")
                        if isinstance(results_v10['Signals'], str):
                            sigs = results_v10['Signals'].split("; ")
                            for s in sigs: st.success(f"💰 {s}")
                        elif isinstance(results_v10['Signals'], list):
                            for s in results_v10['Signals']: st.success(f"💰 {s}")
            else: st.error("Please select both teams.")
