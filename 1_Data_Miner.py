import requests
import pandas as pd
import numpy as np
import time
import os
import threading
from datetime import date, timedelta, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==============================================================================
#   THE BIBLE: ESPN SHOT CHART SCRAPER V2.3 (DOUBLE TURBO + GEO FIX)
#   
#   UPDATES (2025-12-25):
#   - FIXED: Coordinate geometry (Layups now correctly register near rim)
#   - FIXED: Schedule building is now parallel (10x faster startup)
#   - ADJUSTED: RIM_DISTANCE set to 5.5 to catch dunks/layups taking off >4ft away
# ==============================================================================

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR)
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "cbb_style_2025_complete.csv")
LOG_PATH = os.path.join(BASE_DIR, f"scraper_log_{datetime.now().strftime('%Y%m%d')}.txt")

# Date range - Auto-updates to current season-to-date
SEASON_START = date(2025, 11, 2)  # Start of 2025-26 season
TODAY = date.today()

# Threading Settings
MAX_WORKERS = 10  # Number of simultaneous downloads

# Lock for thread-safe printing/logging
log_lock = threading.Lock()

# Court coordinates (Adjusted for valid dunk take-off distances)
RIM_DISTANCE = 5.5      
THREE_POINT_LINE = 22.0   

# ======================================================
# 1. LOGGING & UTILS
# ======================================================

def log_message(message, console=True):
    """Thread-safe logging."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    
    with log_lock:
        if console:
            print(message)
        
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        with open(LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(log_entry + "\n")


def get_espn_data(url, retries=3):
    """Fetch data from ESPN API with retry logic."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException:
            if attempt < retries - 1:
                time.sleep(1)
            continue
    
    return None

# ======================================================
# 2. GEOMETRY & CLASSIFICATION (FIXED)
# ======================================================

def calculate_shot_distance(x, y):
    """
    Calculate distance from shot to nearest basket.
    
    CORRECTED LOGIC (2025-12-25):
    - Input 'x' is actually Width (0-50) -> Maps to our Y
    - Input 'y' is actually Length (0-94) -> Maps to our X
    - No 0.94 scaling needed (Data is in raw feet)
    """
    real_x = y  # Length dimension (The long way)
    real_y = x  # Width dimension (The short way)

    # Standard Hoop Locations (Center of rim)
    # Left Hoop: 5.25 ft from baseline
    # Right Hoop: 88.75 ft from baseline
    hoop_left = (5.25, 25.0)
    hoop_right = (88.75, 25.0)

    # Calculate Euclidean distance to both hoops
    dist_1 = np.sqrt((real_x - hoop_left[0])**2 + (real_y - hoop_left[1])**2)
    dist_2 = np.sqrt((real_x - hoop_right[0])**2 + (real_y - hoop_right[1])**2)

    return min(dist_1, dist_2)


def classify_shot_zone(distance):
    """Classify shot into rim, mid-range, or three-point."""
    if distance < RIM_DISTANCE:
        return 'rim'
    elif distance < THREE_POINT_LINE:
        return 'mid'
    else:
        return 'arc'


def is_made_shot(play):
    """Determine if a shot was made based on play data."""
    if play.get('scoringPlay', False):
        return True
    
    desc = play.get('text', '') or play.get('shortText', '') or ""
    desc_lower = desc.lower()

    # Made indicators
    if any(word in desc_lower for word in ['made', 'makes', 'good']):
        return True
    
    # Missed indicators
    if any(word in desc_lower for word in ['missed', 'misses', 'no good']):
        return False
    
    # For dunks/layups without explicit made/missed
    if any(word in desc_lower for word in ['dunk', 'layup', 'tip in', 'tip-in']):
        return 'missed' not in desc_lower and 'misses' not in desc_lower
    
    return False

# ======================================================
# 3. CORE PARSING LOGIC (DOUBLE TURBO)
# ======================================================

def fetch_games_for_date(date_obj):
    """Helper to fetch games for a single date (Thread-safe)."""
    date_str = date_obj.strftime("%Y%m%d")
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&limit=1000&groups=50"
    
    scoreboard = get_espn_data(url)
    if not scoreboard:
        return []

    daily_games = []
    for event in scoreboard.get('events', []):
        gid = event['id']
        status = event.get('status', {}).get('type', {}).get('state', '')
        
        # Only process completed games
        if status == 'post':
            try:
                comps = event['competitions'][0]['competitors']
                h = next(c for c in comps if c['homeAway'] == 'home')
                a = next(c for c in comps if c['homeAway'] == 'away')
                
                home_data = {'id': h['team']['id'], 'name': h['team']['displayName']}
                away_data = {'id': a['team']['id'], 'name': a['team']['displayName']}
                
                daily_games.append((gid, date_str, home_data, away_data))
            except Exception:
                continue
    return daily_games


def process_single_game(game_meta):
    """Worker function to process one game."""
    game_id, date_str, home_data, away_data = game_meta
    
    url = f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?region=us&lang=en&event={game_id}"
    data = get_espn_data(url)
    
    if not data or 'plays' not in data:
        return []
    
    shots = []
    home_id = str(home_data['id'])
    away_id = str(away_data['id'])
    
    for play in data['plays']:
        desc = play.get('text', "")
        is_scoring = play.get('scoringPlay', False)
        shot_keywords = ['made', 'missed', 'dunk', 'layup', 'jumper', 'three', 'tip-in']
        
        if not (is_scoring or any(k in desc.lower() for k in shot_keywords)):
            continue
        
        coords = play.get('coordinate', {})
        x = coords.get('x')
        y = coords.get('y')
        
        if x is None or y is None:
            continue
        
        play_team_id = str(play.get('team', {}).get('id', ''))
        
        if play_team_id == home_id:
            shooting_team = home_data['name']
            defending_team = away_data['name']
        elif play_team_id == away_id:
            shooting_team = away_data['name']
            defending_team = home_data['name']
        else:
            continue
        
        distance = calculate_shot_distance(x, y)
        
        shots.append({
            'game_id': game_id,
            'date': date_str,
            'shooting_team': shooting_team,
            'defending_team': defending_team,
            'x': x,
            'y': y,
            'distance': round(distance, 1),
            'zone': classify_shot_zone(distance),
            'made': is_made_shot(play),
            'description': desc
        })
    
    return shots


def scrape_season_games(start_date=SEASON_START, end_date=TODAY):
    """
    DOUBLE TURBO: Parallelizes both Schedule Building AND Game Downloading.
    """
    log_message("\n" + "="*60)
    log_message("🚀 ESPN SCRAPER V2.3 (DOUBLE TURBO)")
    log_message("="*60)
    
    # --- PHASE 1: BUILD SCHEDULE (PARALLEL) ---
    log_message(f"📅 Building schedule from {start_date} to {end_date}...")
    
    total_days = (end_date - start_date).days + 1
    date_list = [start_date + timedelta(days=x) for x in range(total_days)]
    
    game_queue = []
    processed_ids = set()
    
    # fast scan of the calendar
    log_message(f"   ⚡ Scanning {total_days} days of calendars in parallel...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Map dates to the fetch function
        future_to_date = {executor.submit(fetch_games_for_date, d): d for d in date_list}
        
        for future in as_completed(future_to_date):
            day_games = future.result()
            for game in day_games:
                gid = game[0]
                if gid not in processed_ids:
                    game_queue.append(game)
                    processed_ids.add(gid)

    log_message(f"✅ Schedule built: Found {len(game_queue)} completed games")
    
    if not game_queue:
        log_message("⚠️  No games found in date range!")
        return pd.DataFrame()
    
    # --- PHASE 2: DOWNLOAD SHOTS (PARALLEL) ---
    all_shots = []
    completed = 0
    total = len(game_queue)
    
    log_message(f"⚡ Downloading shot data for {total} games...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_game = {executor.submit(process_single_game, game): game for game in game_queue}
        
        for future in as_completed(future_to_game):
            try:
                result = future.result()
                if result:
                    all_shots.extend(result)
                
                completed += 1
                if completed % 100 == 0:
                    log_message(f"   ✓ {completed}/{total} games processed... ({len(all_shots):,} shots)")
            
            except Exception as e:
                continue
    
    log_message(f"\n✅ Scraping complete: {len(all_shots):,} shots")
    return pd.DataFrame(all_shots) if all_shots else pd.DataFrame()


# ======================================================
# 4. DATA CLEANING & STATISTICS
# ======================================================

def clean_shot_data(df):
    """Remove invalid shots and standardize team names."""
    if df.empty: return df
    
    initial_count = len(df)
    
    # Remove shots with unrealistic distances
    df_clean = df[df['distance'] < 94].copy()
    
    removed = initial_count - len(df_clean)
    if removed > 0:
        log_message(f"🧹 Removed {removed} invalid shots ({removed/initial_count*100:.1f}%)")
    
    # Standardize team names
    for col in ['shooting_team', 'defending_team']:
        df_clean[col] = (df_clean[col]
                        .str.replace(' State', ' St.', regex=False)
                        .str.replace('Saint ', 'St. ', regex=False))
    
    return df_clean


def calculate_team_stats(df):
    """Calculate offensive and defensive statistics for all teams."""
    log_message("\n📊 Calculating team statistics...")
    
    if df.empty: return None
    
    # Helper columns
    df['is_rim'] = df['zone'] == 'rim'
    df['is_mid'] = df['zone'] == 'mid'
    df['is_arc'] = df['zone'] == 'arc'
    df['made_int'] = df['made'].astype(int)
    
    # --- OFFENSIVE STATS ---
    off_agg = df.groupby('shooting_team').agg(
        games=('game_id', 'nunique'),
        total_shots=('made', 'count'),
        rim_att=('is_rim', 'sum'),
        rim_makes=('made_int', lambda x: x[df.loc[x.index, 'is_rim']].sum()),
        mid_att=('is_mid', 'sum'),
        mid_makes=('made_int', lambda x: x[df.loc[x.index, 'is_mid']].sum()),
        arc_att=('is_arc', 'sum'),
        arc_makes=('made_int', lambda x: x[df.loc[x.index, 'is_arc']].sum()),
    ).reset_index()
    
    # Rates and Percentages
    off_agg['rim_rate'] = (off_agg['rim_att'] / off_agg['total_shots'] * 100).round(1)
    off_agg['rim_pct'] = (off_agg['rim_makes'] / off_agg['rim_att'].replace(0, 1) * 100).round(1)
    off_agg['mid_rate'] = (off_agg['mid_att'] / off_agg['total_shots'] * 100).round(1)
    off_agg['mid_pct'] = (off_agg['mid_makes'] / off_agg['mid_att'].replace(0, 1) * 100).round(1)
    off_agg['arc_rate'] = (off_agg['arc_att'] / off_agg['total_shots'] * 100).round(1)
    off_agg['arc_pct'] = (off_agg['arc_makes'] / off_agg['arc_att'].replace(0, 1) * 100).round(1)
    
    # --- DEFENSIVE STATS ---
    def_agg = df.groupby('defending_team').agg(
        opp_total_shots=('made', 'count'),
        opp_rim_att=('is_rim', 'sum'),
        opp_rim_makes=('made_int', lambda x: x[df.loc[x.index, 'is_rim']].sum()),
        opp_mid_att=('is_mid', 'sum'),
        opp_mid_makes=('made_int', lambda x: x[df.loc[x.index, 'is_mid']].sum()),
        opp_arc_att=('is_arc', 'sum'),
        opp_arc_makes=('made_int', lambda x: x[df.loc[x.index, 'is_arc']].sum()),
    ).reset_index()
    
    # Opponent Rates and Percentages
    def_agg['opp_rim_rate'] = (def_agg['opp_rim_att'] / def_agg['opp_total_shots'] * 100).round(1)
    def_agg['opp_rim_pct'] = (def_agg['opp_rim_makes'] / def_agg['opp_rim_att'].replace(0, 1) * 100).round(1)
    def_agg['opp_mid_rate'] = (def_agg['opp_mid_att'] / def_agg['opp_total_shots'] * 100).round(1)
    def_agg['opp_mid_pct'] = (def_agg['opp_mid_makes'] / def_agg['opp_mid_att'].replace(0, 1) * 100).round(1)
    def_agg['opp_arc_rate'] = (def_agg['opp_arc_att'] / def_agg['opp_total_shots'] * 100).round(1)
    def_agg['opp_arc_pct'] = (def_agg['opp_arc_makes'] / def_agg['opp_arc_att'].replace(0, 1) * 100).round(1)
    
    # --- MERGE ---
    final = pd.merge(
        off_agg[['shooting_team', 'games', 'rim_rate', 'rim_pct', 'mid_rate', 'mid_pct', 'arc_rate', 'arc_pct']], 
        def_agg[['defending_team', 'opp_rim_rate', 'opp_rim_pct', 'opp_mid_rate', 'opp_mid_pct', 'opp_arc_rate', 'opp_arc_pct']], 
        left_on='shooting_team', 
        right_on='defending_team', 
        how='inner'
    )
    
    final = final.rename(columns={'shooting_team': 'play_team'}).drop(columns='defending_team')
    log_message(f"   ✅ Stats calculated for {len(final)} teams")
    return final


def save_data(df):
    """Save shot chart data to CSV with backup."""
    if df is None or df.empty:
        log_message("❌ No data to save")
        return
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if os.path.exists(OUTPUT_PATH):
        try:
            backup_path = os.path.join(OUTPUT_DIR, f"cbb_style_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            os.rename(OUTPUT_PATH, backup_path)
            log_message(f"📦 Old file backed up: {backup_path}")
        except Exception as e:
            log_message(f"⚠️ Could not backup old file: {e}")
    
    df.to_csv(OUTPUT_PATH, index=False)
    log_message(f"💾 Saved to: {OUTPUT_PATH}")
    
    log_message("\n📋 Top 5 Defensive Rim Protectors:")
    sample = df.nsmallest(5, 'opp_rim_rate')[['play_team', 'opp_rim_rate', 'opp_rim_pct']]
    log_message(sample.to_string(index=False))


# ======================================================
# 5. MAIN EXECUTION
# ======================================================

if __name__ == "__main__":
    start_time = time.time()
    
    # Step 1: Scrape games
    raw_df = scrape_season_games()
    
    # Step 2: Process and save
    if not raw_df.empty:
        clean_df = clean_shot_data(raw_df)
        stats_df = calculate_team_stats(clean_df)
        
        if stats_df is not None:
            # Filter for sample size
            stats_df = stats_df[stats_df['games'] >= 3]
            save_data(stats_df)
            
            elapsed = time.time() - start_time
            log_message(f"\n🏁 COMPLETE in {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        else:
            log_message("❌ Failed to calculate statistics")
    else:
        log_message("❌ No data collected")
