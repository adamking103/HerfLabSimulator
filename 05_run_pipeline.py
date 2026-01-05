"""
run_pipeline.py
===============
THE BIBLE - Master Pipeline Runner

Executes the full data pipeline in sequence:
1. Scrape game logs from ESPN
2. Calculate raw efficiency profiles
3. Apply SOS adjustments
4. Analyze quadrant performance

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
    ("01_master_game_log_scraper.py", "Game Log Scraper"),
    ("02_efficiency_processor.py", "Efficiency Processor"),
    ("03_sos_adjustment_processor.py", "SOS Adjustment"),
    ("04_quadrant_performance_analyzer.py", "Quadrant Analysis"),
]

# Expected outputs for validation
EXPECTED_FILES = [
    "master_game_logs_2026.csv",
    "team_raw_efficiency_profiles_2026.csv",
    "team_adjusted_efficiency_profiles_2026.csv",
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
            capture_output=False,
            text=True
        )
        
        if result.returncode == 0:
            print(f"✅ {name} completed successfully")
            return True
        else:
            print(f"❌ {name} failed with return code {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ Error running {name}: {e}")
        return False


def validate_outputs() -> bool:
    """Validates that all expected output files exist."""
    print(f"\n{'='*60}")
    print("🔍 Validating outputs...")
    print('='*60)
    
    all_valid = True
    for filepath in EXPECTED_FILES:
        if os.path.exists(filepath):
            size = os.path.getsize(filepath)
            print(f"  ✅ {filepath} ({size:,} bytes)")
        else:
            print(f"  ❌ {filepath} - NOT FOUND")
            all_valid = False
    
    return all_valid


def main():
    parser = argparse.ArgumentParser(description="THE BIBLE Data Pipeline Runner")
    parser.add_argument('--skip-scrape', action='store_true', 
                       help='Skip the scraping step (use existing data)')
    parser.add_argument('--step', type=int, default=1,
                       help='Start from step N (1-4)')
    parser.add_argument('--validate-only', action='store_true',
                       help='Only validate existing outputs')
    args = parser.parse_args()
    
    print("="*60)
    print("THE BIBLE - Data Pipeline v2.0")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    if args.validate_only:
        success = validate_outputs()
        sys.exit(0 if success else 1)
    
    # Determine which steps to run
    start_step = args.step - 1  # Convert to 0-indexed
    
    if args.skip_scrape and start_step == 0:
        start_step = 1
        print("⏭️  Skipping scrape step (using existing data)")
    
    # Run pipeline
    for i, (script, name) in enumerate(STEPS):
        if i < start_step:
            print(f"⏭️  Skipping step {i+1}: {name}")
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
        print("\nNext steps:")
        print("  1. Review team_quadrant_analysis_2026.csv for betting angles")
        print("  2. Check Paper Tiger teams for fade candidates")
        print("  3. Integrate into The Bible simulator")
    else:
        print("\n⚠️  Some outputs missing - check logs above")
        sys.exit(1)


if __name__ == "__main__":
    main()
