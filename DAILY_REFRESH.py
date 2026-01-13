"""
DAILY_REFRESH.py
====================================================
THE BIBLE V10.1 - DAILY AUTOMATION PROTOCOL
====================================================
1. Updates Box Scores (Scraper)
2. Updates Quadrant Records (Analyzer)
3. Updates Location Stats (PhD Engine)
4. Updates Power Ratings (TQPR Engine)
5. Launches App
"""

import os
import time
import subprocess
import sys

# ==================================================
# CONFIGURATION - VERIFY FILENAMES HERE
# ==================================================
SCRAPER_SCRIPT = "1_Data_Miner.py"             # <--- VERIFY THIS NAME
QUADRANT_SCRIPT = "04_quadrant_performance_analyzer.py"
PHD_SCRIPT = "phd_location_enhancement_API.py"
TQPR_SCRIPT = "kenpom_quadrant_v3_FIXED.py"
APP_SCRIPT = "Bible_App.py"
# ==================================================

def run_step(script_name, step_desc):
    print(f"\n{'='*60}")
    print(f"🔄 STEP {step_desc}: Running {script_name}...")
    print(f"{'='*60}")
    
    if not os.path.exists(script_name):
        print(f"❌ ERROR: Could not find file: {script_name}")
        return False
        
    try:
        # Run the script and wait for it to finish
        result = subprocess.run([sys.executable, script_name], check=True)
        if result.returncode == 0:
            print(f"✅ SUCCESS: {script_name} completed.")
            return True
        else:
            print(f"❌ FAILED: {script_name} crashed.")
            return False
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False

def main():
    print("\n🏀 INITIALIZING BIBLE V10.1 DAILY UPDATE PROTOCOL 🏀")
    start_time = time.time()

    # --- STEP 1: SCRAPE FRESH DATA ---
    # This updates 'master_box_scores_2026.csv'
    if not run_step(SCRAPER_SCRIPT, "1/4 - DATA MINER"):
        x = input("⚠️ Data Miner failed. Continue anyway? (y/n): ")
        if x.lower() != 'y': return

    # --- STEP 2: ANALYZE QUADRANTS ---
    # This updates 'team_quadrant_analysis_2026.csv' using the new box scores
    if not run_step(QUADRANT_SCRIPT, "2/4 - QUADRANT ANALYZER"):
        print("❌ Critical Error: Quadrant data missing.")
        return

    # --- STEP 3: PHD LOCATION ENHANCEMENT ---
    # This updates 'team_home_performance_VALIDATED_2026.csv'
    if not run_step(PHD_SCRIPT, "3/4 - PhD LOCATION ENGINE"):
        print("❌ Critical Error: Location data missing.")
        return

    # --- STEP 4: TQPR RANKINGS ---
    # This updates 'tqpr_full_rankings.csv'
    if not run_step(TQPR_SCRIPT, "4/4 - TQPR GENERATOR"):
        print("❌ Critical Error: TQPR rankings missing.")
        return

    # --- SUMMARY ---
    elapsed = round(time.time() - start_time, 2)
    print(f"\n✨ UPDATE COMPLETE in {elapsed} seconds.")
    print("   All V10.1 models are now synchronized.")
    
    # --- LAUNCH APP ---
    print("\n🚀 LAUNCHING THE BIBLE APP...")
    subprocess.run(["streamlit", "run", APP_SCRIPT])

if __name__ == "__main__":
    main()
