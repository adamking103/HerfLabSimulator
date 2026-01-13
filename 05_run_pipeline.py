"""
run_pipeline.py
===============
THE BIBLE - Master Pipeline Runner

Executes the full data pipeline in sequence:
1. Scrape game logs (Old method - preserves Efficiency/SOS logic)
2. Calculate raw efficiency profiles
3. Apply SOS adjustments
4. Scrape ROBUST Box Scores (New method - captures 4 Factors & Scores)
5. Analyze quadrant performance (Uses robust data)

Usage:
    python run_pipeline.py              # Run full pipeline
    python run_pipeline.py --skip-scrape # Skip scraping (use existing data)
    python run_pipeline.py --step 3     # Run only step 3+
"""

import subprocess
import sys
import os
import argparse
from datetime import datetime

# Pipeline steps
STEPS = [
    ("01_master_game_log_scraper.py", "Game Log Scraper (Legacy)"),
    ("02_efficiency_processor.py", "Efficiency Processor"),
    ("03_sos_adjustment_processor.py", "SOS Adjustment"),
    ("06_box_score_scraper_fixed.py", "Robust Box Score Scraper"), # <--- NEW STEP
    ("04_quadrant_performance_analyzer.py", "Quadrant Analysis"),
]

# Expected outputs for validation
EXPECTED_FILES = [
    "master_game_logs_2026.csv",
    "team_raw_efficiency_profiles_2026.csv",
    "team_adjusted_efficiency_profiles_2026.csv",
    "master_box_scores_2026.csv", # <--- NEW OUTPUT
    "team_quadrant_analysis_2026.csv",
]


def run_step(script: str, name: str) -> bool:
    """Runs a single pipeline step."""
    print(f"\n{'='*60}")
    print(f"🔄 Running: {name}")
    print(f"   Script: {script}")
    print('='*60)
    
    try:
        result = subprocess.run(
            [sys.executable, script],
            check=True,
            capture_output=False  # Let output stream to console
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running {script}: {e}")
        return False
    except FileNotFoundError:
        print(f"❌ Script not found: {script}")
        return False

def validate_outputs() -> bool:
    """Checks if expected output files exist."""
    print("\n🔍 Validating pipeline outputs...")
    missing = []
    for f in EXPECTED_FILES:
        if os.path.exists(f):
            size_kb = os.path.getsize(f) / 1024
            print(f"   ✅ Found {f} ({size_kb:.1f} KB)")
        else:
            print(f"   ❌ MISSING {f}")
            missing.append(f)
    
    return len(missing) == 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run The Bible Data Pipeline')
    parser.add_argument('--step', type=int, default=1, help='Start from specific step (1-5)')
    parser.add_argument('--skip-scrape', action='store_true', help='Skip scraping steps')
    parser.add_argument('--validate-only', action='store_true', help='Only validate existing outputs')
    args = parser.parse_args()
    
    print("="*60)
    print("THE BIBLE - Data Pipeline v2.1 (Robust Integration)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    if args.validate_only:
        success = validate_outputs()
        sys.exit(0 if success else 1)
    
    # Determine which steps to run
    start_step = args.step - 1  # Convert to 0-indexed
    
    if args.skip_scrape:
        print("⏭️  Skipping scraping steps (using existing data)")
        # If skip_scrape is on, we skip steps 0 (Old Scraper) and 3 (New Scraper)
        # But we must run the processors.
        # This logic is a bit complex with two scrapers, so simplistic skip:
        if start_step == 0: start_step = 1 
    
    # Run pipeline
    for i, (script, name) in enumerate(STEPS):
        if i < start_step:
            print(f"⏭️  Skipping step {i+1}: {name}")
            continue
            
        # Handle --skip-scrape logic specifically for the two scrapers
        if args.skip_scrape and "Scraper" in name:
             print(f"⏭️  Skipping scraper step {i+1}: {name}")
             continue

        success = run_step(script, name)
        
        if not success:
            print(f"\n❌ Pipeline failed at step {i+1}: {name}")
            print("Fix the error and re-run with --step", i+1)
            sys.exit(1)
    
    # Validate
    print("\n")
    if validate_outputs():
        print("\n" + "="*60)
        print("🎉 PIPELINE COMPLETE - All outputs validated")
        print("="*60)
