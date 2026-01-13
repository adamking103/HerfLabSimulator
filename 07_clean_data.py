"""
07_clean_data.py
================
THE BIBLE - Final Polish
Fixes date formats and ensures all stats are clean for the Analyzer.
"""

import pandas as pd
from datetime import datetime
import logging

INPUT_FILE = "master_box_scores_2026.csv"
OUTPUT_FILE = "master_box_scores_2026.csv" # Overwrite with clean version

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

def fix_date(date_str):
    """Converts 'Mon, Nov 3' to '2025-11-03'."""
    try:
        if '-' in str(date_str) and str(date_str).count('-') == 2:
            return date_str # Already fixed
            
        # Parse "Mon, Nov 3"
        # Remove the Day of Week if present
        clean_str = str(date_str).split(',')[-1].strip() # "Nov 3"
        
        # Parse month/day
        dt = datetime.strptime(clean_str, "%b %d")
        
        # Assign Year (Season 2025-2026 logic)
        if dt.month >= 10: # Oct, Nov, Dec
            year = 2025
        else: # Jan, Feb, Mar
            year = 2026
            
        return dt.replace(year=year).strftime("%Y-%m-%d")
    except Exception as e:
        return date_str

def main():
    logger.info("🧹 Cleaning Master File...")
    
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        logger.error("❌ File not found. Run the scraper first.")
        return

    # 1. Fix Dates
    df['Date'] = df['Date'].apply(fix_date)
    
    # 2. Check for "Placeholder" remnants (Optional safety)
    # If eFG is exactly 50.0 AND TO is exactly 18.0 AND OR is 30.0... mark as suspicious?
    # No, we trust the scraper now.
    
    # 3. Rounding
    cols_to_round = ['Possessions', 'eFG%', 'TO%', 'OR%', 'FTR']
    for col in cols_to_round:
        if col in df.columns:
            df[col] = df[col].round(1)

    # 4. Save
    df.to_csv(OUTPUT_FILE, index=False)
    
    # 5. Verify
    logger.info(f"✅ Data Cleaned! ({len(df)} rows)")
    logger.info("First 3 rows of clean data:")
    print(df[['Date', 'Team', 'TeamScore', 'Possessions', 'eFG%']].head(3))
    logger.info("\n👉 You are ready to run Step 4.")

if __name__ == "__main__":
    main()
