import streamlit as st
import pandas as pd
import numpy as np
import os
import requests
from io import StringIO

# --- IMPORT ENGINES ---
import Bible_Simulator_V10_1_TQPR as v10_engine
import Bible_Simulator_V9 as v9_engine  # <--- Make sure you renamed '2_Bible_Simulator.py'

# --- CONFIGURATION ---
st.set_page_config(page_title="The Bible Dual-Core", layout="wide", page_icon="🏀")

# --- CSS STYLING ---
st.markdown("""
<style>
    .model-header { font-size: 18px; font-weight: bold; color: #374151; margin-bottom: 10px; text-align: center; border-bottom: 2px solid #e5e7eb; padding-bottom: 5px; }
    .score-big { font-size: 26px; font-weight: 800; text-align: center; }
    .line-disp { font-size: 16px; font-weight: 600; text-align: center; color: #4b5563; margin-top: 5px; }
    .edge-box { text-align: center; margin-top: 10px; padding: 10px; border-radius: 8px; background: #f9fafb; }
    .edge-val { font-size: 20px; font-weight: 800; }
    .green { color: #16a34a; }
    .red { color: #dc2626; }
    .v9-card { background-color: #fff1f2; border: 1px solid #fda4af; border-radius: 10px; padding: 15px; }
    .v10-card { background-color: #eff6ff; border: 1px solid #93c5fd; border-radius: 10px; padding: 15px; }
    .flag { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; margin: 2px; }
    .flag-paper { background: #fef3c7; color: #b45309; border: 1px solid #fcd34d; }
    .flag-kentucky { background: #fee2e2; color: #991b1b; border: 1px solid #fca5a5; }
</style>
""", unsafe_allow_html=True)

# --- DATA LOADING ---
@st.cache_data(ttl=3600)
def load_all_data():
    """Loads data for BOTH models efficiently."""
    
    # 1. Load V10 Bundle (It has almost everything)
    v10_data = v10_engine.build_team_database()
    
    # 2. Load V9 Dependencies
    # V9 needs 'style_db' and 'kp_data' explicitly
    # We can reuse the style file from V10 if it exists, or load it manually
    style_path = "cbb_style_2025_complete.csv"
    if os.path.exists(style_path):
        style_db = pd.read_csv(style_path)
    else:
        style_db = pd.DataFrame() # Fallback
        
    # We also need a clean KenPom DF for V9
    # We can fetch it or extract it from V10 bundle if available
    # Safest is to just fetch it once here to ensure V9 format
    kp_df = pd.DataFrame()
    try:
        KP_API_KEY = "18ee6ae93a94ade96fc899611578ef42f57ad96e09845cb585ee3b2aab1444fa"
        url = f"https://kenpom.com/api.php?endpoint=ratings&y=2026"
        r = requests.get(url, headers={"Authorization": f"Bearer {KP_API_KEY}"})
        if r.status_code == 200:
            # Handle API wrapper
            d = r.json()
            if isinstance(d, dict) and 'ratings' in d: d = d['ratings']
            elif isinstance(d, dict) and 'data' in d: d = d['data']
            kp_df = pd.DataFrame(d)
            # Ensure numeric columns for V9
            cols = ['AdjEM', 'AdjO', 'AdjD', 'AdjT']
            for c in cols:
                if c in kp_df.columns: kp_df[c] = pd.to_numeric(kp_df[c])
            kp_df.rename(columns={'TeamName': 'Team'}, inplace=True)
    except:
        pass

    return v10_data, style_db, kp_df

# Load Data
with st.spinner("Initializing Dual-Core Engines..."):
    v10_bundle, v9_style, v9_kp = load_all_data()

# --- SIDEBAR ---
with st.sidebar:
    st.header("🏀 Bible Dual-Core")
    st.success("System: ONLINE")
    st.info("Loaded Engines:\n- V9.2 (Classic)\n- V10.1 (TQPR)")

# --- MAIN UI ---
st.title("🔮 Matchup Simulator")

col1, col2 = st.columns(2)
with col1:
    visitor = st.text_input("Visitor Team", placeholder="e.g. Duke")
with col2:
    home = st.text_input("Home Team", placeholder="e.g. North Carolina")

c1, c2 = st.columns(2)
line = c1.number_input("Market Spread (Home)", value=0.0, step=0.5)
total = c2.number_input("Market Total", value=145.0, step=0.5)

if st.button("Run Simulation", type="primary", use_container_width=True):
    if visitor and home:
        
        # --- RUN V9 (CLASSIC) ---
        # Signature: simulate_matchup(visitor, home, spread, total, style_db, kp_data)
        try:
            res_v9 = v9_engine.simulate_matchup(visitor, home, line, total, v9_style, v9_kp)
        except Exception as e:
            res_v9 = {"error": str(e)}

        # --- RUN V10 (TQPR) ---
        # Signature: simulate_matchup(visitor, home, spread, total, data_bundle)
        try:
            res_v10 = v10_engine.simulate_matchup(visitor, home, line, total, v10_bundle)
        except Exception as e:
            res_v10 = {"error": str(e)}

        st.divider()
        
        # --- DISPLAY SIDE-BY-SIDE ---
        col_v9, col_v10 = st.columns(2)
        
        # === V9 COLUMN ===
        with col_v9:
            st.markdown('<div class="model-header">🏛️ V9.2 (Classic)</div>', unsafe_allow_html=True)
            if "error" in res_v9:
                st.error(f"V9 Error: {res_v9['error']}")
            else:
                st.markdown(f"""
                <div class="v9-card">
                    <div class="score-big">{res_v9['Visitor']} {res_v9['V_Score']}</div>
                    <div class="score-big">{res_v9['Home']} {res_v9['H_Score']}</div>
                    <div class="line-disp">Line: {res_v9['Home']} {-res_v9['Predicted_Spread']:+.1f}</div>
                    
                    <div class="edge-box">
                        <div style="font-size:12px; color:#6b7280;">EDGE</div>
                        <div class="edge-val {'green' if res_v9['Edge_Spread'] > 0 else 'red'}">
                            {res_v9['Edge_Spread']:+.1f}
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

        # === V10 COLUMN ===
        with col_v10:
            st.markdown('<div class="model-header">🧠 V10.1 (TQPR Smart)</div>', unsafe_allow_html=True)
            if "error" in res_v10:
                st.error(f"V10 Error: {res_v10['error']}")
            else:
                # Build Tags
                flags_html = ""
                if 'TQPR_Flags' in res_v10 and res_v10['TQPR_Flags']:
                    if "PAPER" in res_v10['TQPR_Flags']:
                        flags_html += f"<span class='flag flag-paper'>🐯 {res_v10['TQPR_Flags']}</span>"
                    if "KENTUCKY" in res_v10['TQPR_Flags']:
                        flags_html += f"<span class='flag flag-kentucky'>📉 {res_v10['TQPR_Flags']}</span>"
                
                st.markdown(f"""
                <div class="v10-card">
                    <div class="score-big">{res_v10['Visitor']} {res_v10['V_Score']}</div>
                    <div class="score-big">{res_v10['Home']} {res_v10['H_Score']}</div>
                    <div class="line-disp">Line: {res_v10['Home']} {-res_v10['Predicted_Spread']:+.1f}</div>
                    
                    <div class="edge-box">
                        <div style="font-size:12px; color:#6b7280;">EDGE</div>
                        <div class="edge-val {'green' if res_v10['Edge_Spread'] > 0 else 'red'}">
                            {res_v10['Edge_Spread']:+.1f}
                        </div>
                    </div>
                    <div style="text-align:center; margin-top:5px;">{flags_html}</div>
                </div>
                """, unsafe_allow_html=True)
                
                if 'PhD_Reasoning' in res_v10 and res_v10['PhD_Reasoning']:
                    st.info(f"**Loc:** {res_v10['PhD_Reasoning']}")
