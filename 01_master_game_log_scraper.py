"""
01_master_game_log_scraper.py
=============================
THE BIBLE - Data Pipeline Step 1

Purpose: Scrapes every D1 game from ESPN, filters out non-D1 opponents,
         and properly assigns scores based on W/L result.

Key Fixes from V1:
- Removed flawed possession calculation (deferred to Step 2 with KenPom tempo)
- Added comprehensive opponent name cleaning
- Added date parsing for recency weighting downstream
- Added location detection (home/away/neutral)
- Better error handling and logging
- Added validation checks

Output: master_game_logs_2026.csv
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime
import logging

# ============================================================================
# CONFIGURATION
# ============================================================================
SEASON_YEAR = 2026
OUTPUT_FILE = "master_game_logs_2026.csv"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
REQUEST_DELAY = 0.35  # Be respectful to ESPN's servers

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# TEAM NAME STANDARDIZATION
# ============================================================================
# Comprehensive mascot list for cleaning opponent names
MASCOTS = [
    # SEC
    'crimson tide', 'razorbacks', 'tigers', 'gators', 'bulldogs', 'wildcats',
    'rebels', 'commodores', 'gamecocks', 'volunteers', 'aggies', 'longhorns',
    # Big Ten
    'buckeyes', 'wolverines', 'spartans', 'nittany lions', 'hawkeyes', 
    'golden gophers', 'badgers', 'boilermakers', 'hoosiers', 'fighting illini',
    'cornhuskers', 'scarlet knights', 'terrapins', 'bruins', 'trojans',
    'ducks', 'huskies', 'cougars',
    # Big 12
    'jayhawks', 'red raiders', 'horned frogs', 'bears', 'cyclones', 
    'sooners', 'cowboys', 'mountaineers', 'bearcats', 'knights', 'cougars',
    'sun devils', 'buffaloes', 'utes', 'cardinals',
    # ACC
    'blue devils', 'tar heels', 'wolfpack', 'demon deacons', 'cavaliers',
    'hokies', 'yellow jackets', 'seminoles', 'hurricanes', 'orange',
    'panthers', 'fighting irish', 'eagles', 'cardinal',
    # Big East
    'blue jays', 'hoyas', 'red storm', 'pirates', 'friars', 'musketeers',
    'golden eagles', 'bluejays', 'villanova wildcats',
    # Other common
    'tritons', 'billikens', 'gaels', 'toreros', 'dons', 'waves', 'broncos',
    'aztecs', 'rebels', 'running rebels', 'falcons', 'owls', 'mean green',
    'miners', 'roadrunners', 'thundering herd', 'rockets', 'redhawks',
    'bobcats', 'zips', 'flashes', 'penguins', 'flames', 'phoenix',
    'anteaters', 'gauchos', 'matadors', 'titans', 'highlanders', 'aggies',
    'mustangs', 'leopards', 'patriots', 'monarchs', 'dukes', 'spiders',
    'rams', 'explorers', 'hawks', 'jaspers', 'peacocks', 'red foxes',
    'bonnies', 'flyers', 'raiders', 'colonels', 'bison', 'leathernecks'
]

# Known name variations that need explicit mapping
TEAM_ALIASES = {
    # California schools
    'uc san diego': 'UC San Diego',
    'ucsd': 'UC San Diego',
    'uc davis': 'UC Davis',
    'uc irvine': 'UC Irvine',
    'uc riverside': 'UC Riverside',
    'uc santa barbara': 'UC Santa Barbara',
    'ucsb': 'UC Santa Barbara',
    
    # State abbreviations
    'usc': 'USC',
    'ucla': 'UCLA',
    'unlv': 'UNLV',
    'utep': 'UTEP',
    'utsa': 'UTSA',
    'unc': 'North Carolina',
    'uconn': 'UConn',
    'smu': 'SMU',
    'tcu': 'TCU',
    'lsu': 'LSU',
    'vcu': 'VCU',
    'fiu': 'FIU',
    'fau': 'FAU',
    
    # Saint vs St.
    "saint mary's": "Saint Mary's",
    "st. mary's": "Saint Mary's",
    "saint john's": "St. John's",
    "saint joseph's": "Saint Joseph's",
    "saint louis": "Saint Louis",
    "st. louis": "Saint Louis",
    "saint peter's": "Saint Peter's",
    "saint bonaventure": "St. Bonaventure",
    
    # Other variations
    'miami (fl)': 'Miami',
    'miami florida': 'Miami',
    'miami (oh)': 'Miami (OH)',
    'miami ohio': 'Miami (OH)',
    "hawai'i": "Hawaii",
    'hawaii rainbow warriors': 'Hawaii',
    'ole miss': 'Ole Miss',
    'mississippi': 'Ole Miss',
    'pitt': 'Pittsburgh',
    'nc state': 'NC State',
    'north carolina state': 'NC State',
}


def clean_team_name(raw_name: str) -> str:
    """
    Cleans and standardizes team names for consistent matching.
    """
    if not isinstance(raw_name, str):
        return ""
    
    name = raw_name.strip()
    
    # Remove ranking prefix (e.g., "#5 Duke" -> "Duke")
    name = re.sub(r'^#\d+\s+', '', name)
    
    # Remove @ or vs prefix
    name = re.sub(r'^(@|vs\.?)\s*', '', name)
    
    # Remove trailing asterisks or special chars
    name = re.sub(r'[\*†‡]+$', '', name).strip()
    
    # Check aliases first (case-insensitive)
    name_lower = name.lower()
    if name_lower in TEAM_ALIASES:
        return TEAM_ALIASES[name_lower]
    
    # Remove mascots (case-insensitive)
    for mascot in MASCOTS:
        pattern = r'\s+' + re.escape(mascot) + r'$'
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    
    return name.strip()


def get_master_team_list() -> dict:
    """
    Fetches the full list of D1 teams and their ESPN IDs.
    Returns dict: {team_name: espn_id}
    """
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?limit=400"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        teams = data['sports'][0]['leagues'][0]['teams']
        
        team_dict = {}
        for t in teams:
            name = t['team']['displayName']
            tid = t['team']['id']
            team_dict[name] = tid
            
        logger.info(f"Loaded {len(team_dict)} D1 teams from ESPN API")
        return team_dict
        
    except Exception as e:
        logger.error(f"Error fetching team list: {e}")
        return {}


def parse_location(opp_cell_text: str, opp_name: str) -> str:
    """
    Determines game location from the opponent cell text.
    Returns: 'Home', 'Away', or 'Neutral'
    """
    text = opp_cell_text.strip()
    
    if text.startswith('@'):
        return 'Away'
    elif text.startswith('vs') and 'neutral' in text.lower():
        return 'Neutral'
    elif text.startswith('vs'):
        return 'Home'
    elif '@' in text:
        return 'Away'
    else:
        # Default assumption: if no indicator, likely home
        return 'Home'


def parse_date(date_text: str) -> str:
    """
    Parses date text from ESPN schedule into standardized format.
    """
    try:
        # ESPN format is typically "Mon, Nov 4" or "Nov 4"
        date_text = date_text.strip()
        
        # Remove day of week if present
        if ',' in date_text:
            date_text = date_text.split(',')[1].strip()
        
        # Parse and standardize
        for fmt in ['%b %d', '%B %d']:
            try:
                parsed = datetime.strptime(date_text, fmt)
                # Assign year based on month (Nov-Dec = current year, Jan-Apr = next year)
                if parsed.month >= 9:
                    parsed = parsed.replace(year=SEASON_YEAR - 1)
                else:
                    parsed = parsed.replace(year=SEASON_YEAR)
                return parsed.strftime('%Y-%m-%d')
            except ValueError:
                continue
                
        return ""
    except:
        return ""


def scrape_team_schedule(team_name: str, espn_id: str, session: requests.Session) -> list:
    """
    Scrapes a team's schedule from ESPN and returns game data.
    Only includes games against D1 opponents.
    """
    url = f"https://www.espn.com/mens-college-basketball/team/schedule/_/id/{espn_id}/season/{SEASON_YEAR}"
    
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        games = []
        rows = soup.find_all('tr', class_='Table__TR')
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 3:
                continue
            
            # --- NON-D1 FILTER ---
            # ESPN links D1 teams to their profiles. No <a> tag = non-D1 opponent.
            opp_cell = cells[1]
            if not opp_cell.find('a'):
                continue
            
            # --- PARSE OPPONENT ---
            opp_raw = opp_cell.get_text(strip=True)
            opp_clean = clean_team_name(opp_raw)
            
            if not opp_clean:
                continue
            
            # --- PARSE LOCATION ---
            location = parse_location(opp_raw, opp_clean)
            
            # --- PARSE DATE ---
            date_str = parse_date(cells[0].get_text(strip=True))
            
            # --- PARSE RESULT & SCORES ---
            result_text = cells[2].get_text(strip=True)
            
            # Match patterns like "W 85-72", "L 68-75", "W 102-98 OT"
            score_match = re.search(r'([WL])\s*(\d+)-(\d+)', result_text)
            
            if not score_match:
                # Game hasn't been played yet or cancelled
                continue
            
            result_letter = score_match.group(1)
            score_a = int(score_match.group(2))
            score_b = int(score_match.group(3))
            
            # --- SCORE ASSIGNMENT FIX ---
            # ESPN shows YOUR score first in wins, opponent's first in losses
            # Actually, ESPN always shows winner-loser format
            # So we use W/L to determine which is which
            if result_letter == 'W':
                tm_score = max(score_a, score_b)
                opp_score = min(score_a, score_b)
            else:
                tm_score = min(score_a, score_b)
                opp_score = max(score_a, score_b)
            
            # Check for overtime
            is_ot = 'OT' in result_text.upper()
            
            games.append({
                'Date': date_str,
                'Team': team_name,
                'Opponent': opp_clean,
                'Location': location,
                'Result': result_letter,
                'TeamScore': tm_score,
                'OpponentScore': opp_score,
                'Margin': tm_score - opp_score,
                'TotalPoints': tm_score + opp_score,
                'IsOT': is_ot
            })
        
        return games
        
    except requests.RequestException as e:
        logger.warning(f"Request error for {team_name}: {e}")
        return []
    except Exception as e:
        logger.warning(f"Parse error for {team_name}: {e}")
        return []


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates and cleans the scraped data.
    """
    initial_count = len(df)
    
    # Remove any rows with missing critical fields
    df = df.dropna(subset=['Team', 'Opponent', 'TeamScore', 'OpponentScore'])
    
    # Remove impossible scores
    df = df[(df['TeamScore'] >= 0) & (df['OpponentScore'] >= 0)]
    df = df[(df['TeamScore'] <= 200) & (df['OpponentScore'] <= 200)]
    
    # Remove games where both scores are 0 (cancelled/forfeit)
    df = df[~((df['TeamScore'] == 0) & (df['OpponentScore'] == 0))]
    
    # Verify W/L matches score differential
    df['ScoreCheck'] = df.apply(
        lambda r: (r['Result'] == 'W' and r['TeamScore'] > r['OpponentScore']) or
                  (r['Result'] == 'L' and r['TeamScore'] < r['OpponentScore']),
        axis=1
    )
    
    invalid_count = (~df['ScoreCheck']).sum()
    if invalid_count > 0:
        logger.warning(f"Found {invalid_count} games with W/L mismatch - fixing...")
        # Fix the result based on actual scores
        df.loc[df['TeamScore'] > df['OpponentScore'], 'Result'] = 'W'
        df.loc[df['TeamScore'] < df['OpponentScore'], 'Result'] = 'L'
    
    df = df.drop(columns=['ScoreCheck'])
    
    final_count = len(df)
    if initial_count != final_count:
        logger.info(f"Validation removed {initial_count - final_count} invalid rows")
    
    return df


def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("THE BIBLE - Step 1: Master Game Log Scraper")
    logger.info("=" * 60)
    
    # Get team list
    teams = get_master_team_list()
    if not teams:
        logger.error("Failed to load team list. Exiting.")
        return
    
    # Setup session
    session = requests.Session()
    session.headers.update(HEADERS)
    
    all_games = []
    errors = []
    
    for i, (name, tid) in enumerate(teams.items(), 1):
        logger.info(f"[{i}/{len(teams)}] Scraping: {name}")
        
        games = scrape_team_schedule(name, tid, session)
        
        if games:
            all_games.extend(games)
            logger.info(f"    -> Found {len(games)} D1 games")
        else:
            errors.append(name)
        
        time.sleep(REQUEST_DELAY)
    
    if not all_games:
        logger.error("No games scraped. Check ESPN connectivity.")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(all_games)
    
    # Validate
    df = validate_data(df)
    
    # Sort by date
    df = df.sort_values(['Date', 'Team']).reset_index(drop=True)
    
    # Save
    df.to_csv(OUTPUT_FILE, index=False)
    
    # Summary
    logger.info("=" * 60)
    logger.info("SCRAPE COMPLETE")
    logger.info(f"Total games: {len(df)}")
    logger.info(f"Unique teams: {df['Team'].nunique()}")
    logger.info(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
    logger.info(f"Output file: {OUTPUT_FILE}")
    
    if errors:
        logger.warning(f"Teams with errors ({len(errors)}): {errors[:10]}...")
    
    # Quick sanity checks
    logger.info("\nSanity Checks:")
    logger.info(f"  Avg points per game: {df['TotalPoints'].mean():.1f}")
    logger.info(f"  Home games: {(df['Location'] == 'Home').sum()}")
    logger.info(f"  Away games: {(df['Location'] == 'Away').sum()}")
    logger.info(f"  OT games: {df['IsOT'].sum()}")


if __name__ == "__main__":
    main()
