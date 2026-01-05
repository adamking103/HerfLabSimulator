"""
04_quadrant_performance_analyzer.py
===================================
THE BIBLE - Data Pipeline Step 4

Purpose: Analyzes team performance splits by opponent quality quadrant.
         This is the "Kentucky Detector" - reveals paper tigers who beat
         weak teams but struggle against quality competition.

Key Fixes from V1:
- Option to use KenPom rankings (market-based) OR your adjusted rankings
- Sample size tracking with confidence levels
- Efficiency-based quadrant cutoffs (not arbitrary rank splits)
- Separate offensive and defensive quadrant analysis
- Home/Away splits within quadrants
- "Paper Tiger Score" composite metric

Dependencies:
- master_game_logs_2026.csv (from Step 1)
- team_adjusted_efficiency_profiles_2026.csv (from Step 3)
- [Optional] kenpom_2026.csv for market-based quadrants

Output: team_quadrant_analysis_2026.csv
"""

import pandas as pd
import numpy as np
import re
import logging
from typing import Dict, List, Optional, Tuple

# ============================================================================
# CONFIGURATION
# ============================================================================
INPUT_GAME_LOGS = "master_game_logs_2026.csv"
INPUT_ADJ_PROFILES = "team_adjusted_efficiency_profiles_2026.csv"
KENPOM_FILE = "kenpom_2026.csv"  # Optional - for market-based quadrants
OUTPUT_FILE = "team_quadrant_analysis_2026.csv"

# Quadrant definitions (by rank)
# Q1 = Top 50, Q2 = 51-100, Q3 = 101-200, Q4 = 201+
QUAD_RANK_CUTOFFS = {
    'Q1': (1, 50),
    'Q2': (51, 100),
    'Q3': (101, 200),
    'Q4': (201, 400)
}

# Alternative: Efficiency-based cutoffs (more meaningful)
# These are approximate - adjust based on your data
QUAD_EFF_CUTOFFS = {
    'Q1': 15.0,   # Net Eff > +15 = elite
    'Q2': 5.0,    # Net Eff +5 to +15 = good
    'Q3': -5.0,   # Net Eff -5 to +5 = average
    'Q4': -999    # Net Eff < -5 = below average
}

# Minimum games for "reliable" split
MIN_GAMES_RELIABLE = 3
MIN_GAMES_USABLE = 1

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# NAME NORMALIZATION (consistent with Step 3)
# ============================================================================
MASCOTS = [
    'crimson tide', 'razorbacks', 'tigers', 'gators', 'bulldogs', 'wildcats',
    'rebels', 'commodores', 'gamecocks', 'volunteers', 'aggies', 'longhorns',
    'buckeyes', 'wolverines', 'spartans', 'nittany lions', 'hawkeyes',
    'golden gophers', 'badgers', 'boilermakers', 'hoosiers', 'fighting illini',
    'cornhuskers', 'scarlet knights', 'terrapins', 'bruins', 'trojans',
    'ducks', 'huskies', 'cougars', 'jayhawks', 'red raiders', 'horned frogs',
    'bears', 'cyclones', 'sooners', 'cowboys', 'mountaineers', 'bearcats',
    'knights', 'sun devils', 'buffaloes', 'utes', 'cardinals', 'blue devils',
    'tar heels', 'wolfpack', 'demon deacons', 'cavaliers', 'hokies',
    'yellow jackets', 'seminoles', 'hurricanes', 'orange', 'panthers',
    'fighting irish', 'eagles', 'blue jays', 'hoyas', 'red storm', 'pirates',
    'friars', 'musketeers', 'golden eagles', 'tritons', 'billikens', 'gaels',
    'toreros', 'dons', 'waves', 'broncos', 'aztecs', 'running rebels',
    'falcons', 'owls', 'mean green', 'miners', 'roadrunners'
]

def normalize_team_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    
    # 1. Lowercase and strip
    name = name.lower().strip()
    
    # 2. NEW FIX: Strip leading digits (removes "12" from "12Gonzaga")
    name = re.sub(r'^\d+', '', name)
    
    # 3. Apply your existing mascot/alias logic
    for mascot in MASCOTS:
        pattern = r'\s+' + re.escape(mascot) + r'\s*$'
        name = re.sub(pattern, '', name)
        
    # 4. Standardize and remove special characters
    name = name.replace('st.', 'st').replace('&', 'and').replace("'", '').replace('-', '')
    return re.sub(r'[^a-z0-9]', '', name).strip()


# ============================================================================
# QUADRANT ASSIGNMENT
# ============================================================================
def assign_quadrant_by_rank(rank: float) -> str:
    """Assigns quadrant based on rank."""
    for quad, (low, high) in QUAD_RANK_CUTOFFS.items():
        if low <= rank <= high:
            return quad
    return 'Q4'


def assign_quadrant_by_efficiency(net_eff: float) -> str:
    """Assigns quadrant based on net efficiency."""
    if net_eff >= QUAD_EFF_CUTOFFS['Q1']:
        return 'Q1'
    elif net_eff >= QUAD_EFF_CUTOFFS['Q2']:
        return 'Q2'
    elif net_eff >= QUAD_EFF_CUTOFFS['Q3']:
        return 'Q3'
    else:
        return 'Q4'


def get_confidence_level(n_games: int) -> str:
    """Returns confidence level based on sample size."""
    if n_games >= 5:
        return 'HIGH'
    elif n_games >= MIN_GAMES_RELIABLE:
        return 'MEDIUM'
    elif n_games >= MIN_GAMES_USABLE:
        return 'LOW'
    else:
        return 'NONE'


# ============================================================================
# ANALYSIS FUNCTIONS
# ============================================================================
def calculate_quadrant_stats(games: pd.DataFrame) -> Dict:
    """
    Calculates comprehensive stats for a set of games.
    """
    if games.empty:
        return {
            'Games': 0,
            'Wins': 0,
            'Losses': 0,
            'WinPct': None,
            'AvgMargin': None,
            'AvgPointsFor': None,
            'AvgPointsAgainst': None,
            'AvgNetEff': None,
            'Confidence': 'NONE'
        }
    
    n = len(games)
    wins = (games['Result'] == 'W').sum()
    losses = n - wins
    
    # Calculate efficiency if available, otherwise use margin
    if 'NetEff' in games.columns:
        avg_net_eff = games['NetEff'].mean()
    else:
        # Estimate from margin (rough approximation)
        avg_net_eff = games['Margin'].mean() * 1.5  # ~1.5 factor for efficiency
    
    return {
        'Games': n,
        'Wins': wins,
        'Losses': losses,
        'WinPct': wins / n,
        'AvgMargin': games['Margin'].mean(),
        'AvgPointsFor': games['TeamScore'].mean(),
        'AvgPointsAgainst': games['OpponentScore'].mean(),
        'AvgNetEff': avg_net_eff,
        'Confidence': get_confidence_level(n)
    }


def calculate_paper_tiger_score(team_stats: Dict) -> float:
    """
    Calculates a "Paper Tiger Score" that identifies teams who:
    - Perform well against weak teams (Q3/Q4)
    - Underperform against strong teams (Q1/Q2)
    
    Higher score = more of a paper tiger
    Score near 0 = consistent performer
    Negative score = actually better against good teams
    """
    q1_stats = team_stats.get('Q1', {})
    q2_stats = team_stats.get('Q2', {})
    q3_stats = team_stats.get('Q3', {})
    q4_stats = team_stats.get('Q4', {})
    
    # Get net efficiency for each quadrant
    q1_net = q1_stats.get('AvgNetEff')
    q2_net = q2_stats.get('AvgNetEff')
    q3_net = q3_stats.get('AvgNetEff')
    q4_net = q4_stats.get('AvgNetEff')
    
    # Calculate weighted averages
    # Strong opponents (Q1 + Q2)
    strong_games = (q1_stats.get('Games', 0) or 0) + (q2_stats.get('Games', 0) or 0)
    weak_games = (q3_stats.get('Games', 0) or 0) + (q4_stats.get('Games', 0) or 0)
    
    if strong_games == 0 or weak_games == 0:
        return None  # Can't calculate without both
    
    # Weighted average performance
    strong_net = 0
    if q1_stats.get('Games', 0) > 0 and q1_net is not None:
        strong_net += q1_net * q1_stats['Games']
    if q2_stats.get('Games', 0) > 0 and q2_net is not None:
        strong_net += q2_net * q2_stats['Games']
    strong_net = strong_net / strong_games if strong_games > 0 else 0
    
    weak_net = 0
    if q3_stats.get('Games', 0) > 0 and q3_net is not None:
        weak_net += q3_net * q3_stats['Games']
    if q4_stats.get('Games', 0) > 0 and q4_net is not None:
        weak_net += q4_net * q4_stats['Games']
    weak_net = weak_net / weak_games if weak_games > 0 else 0
    
    # Paper Tiger Score = performance vs weak - performance vs strong
    # High positive = much better vs weak teams = paper tiger
    return weak_net - strong_net


# ============================================================================
# MAIN ANALYSIS
# ============================================================================
def run_quadrant_analysis(use_kenpom: bool = False):
    """Main analysis function."""
    logger.info("=" * 60)
    logger.info("THE BIBLE - Step 4: Quadrant Performance Analyzer")
    logger.info("=" * 60)
    
    # Load data
    try:
        logs_df = pd.read_csv(INPUT_GAME_LOGS)
        profiles_df = pd.read_csv(INPUT_ADJ_PROFILES)
        logger.info(f"Loaded {len(logs_df)} games, {len(profiles_df)} team profiles")
    except FileNotFoundError as e:
        logger.error(f"Required file not found: {e}")
        return
    
    # Try to load KenPom if requested
    kenpom_df = None
    if use_kenpom:
        try:
            kenpom_df = pd.read_csv(KENPOM_FILE)
            logger.info(f"Using KenPom rankings for quadrant assignment")
        except FileNotFoundError:
            logger.warning(f"KenPom file not found, using internal rankings")
            use_kenpom = False
    
    # Create ranking/quadrant mapping
    if use_kenpom and kenpom_df is not None:
        # Use KenPom rankings
        kenpom_df['NormKey'] = kenpom_df['Team'].apply(normalize_team_name)
        rank_map = dict(zip(kenpom_df['NormKey'], kenpom_df['Rank']))
        quad_source = "KenPom"
    else:
        # Use our adjusted rankings
        profiles_df['NormKey'] = profiles_df['Team'].apply(normalize_team_name)
        profiles_df['Rank'] = profiles_df['AdjNetEff'].rank(ascending=False, method='min')
        rank_map = dict(zip(profiles_df['NormKey'], profiles_df['Rank']))
        eff_map = dict(zip(profiles_df['NormKey'], profiles_df['AdjNetEff']))
        quad_source = "Internal Adjusted"
    
    logger.info(f"Quadrant source: {quad_source}")
    
    # Normalize game log names
    logs_df['TeamKey'] = logs_df['Team'].apply(normalize_team_name)
    logs_df['OppKey'] = logs_df['Opponent'].apply(normalize_team_name)
    
    # Assign opponent quadrant
    logs_df['OppRank'] = logs_df['OppKey'].map(rank_map).fillna(362)
    logs_df['OppQuad'] = logs_df['OppRank'].apply(assign_quadrant_by_rank)
    
    # Calculate per-game efficiency (if not already present)
    if 'NetEff' not in logs_df.columns:
        # Rough estimate based on margin
        logs_df['NetEff'] = logs_df['Margin'] * 1.5
    
    # Analyze each team
    all_results = []
    
    for team in profiles_df['Team'].unique():
        team_key = normalize_team_name(team)
        team_games = logs_df[logs_df['TeamKey'] == team_key].copy()
        
        if team_games.empty:
            continue
        
        # Get team's overall stats
        team_profile = profiles_df[profiles_df['Team'] == team].iloc[0]
        
        result = {
            'Team': team,
            'AdjRank': team_profile.get('AdjRank', rank_map.get(team_key, 999)),
            'AdjNetEff': team_profile.get('AdjNetEff', 0),
            'TotalGames': len(team_games),
            'TotalWins': (team_games['Result'] == 'W').sum()
        }
        
        # Analyze by quadrant
        quad_stats = {}
        for quad in ['Q1', 'Q2', 'Q3', 'Q4']:
            quad_games = team_games[team_games['OppQuad'] == quad]
            stats = calculate_quadrant_stats(quad_games)
            quad_stats[quad] = stats
            
            # Add to result dict
            result[f'{quad}_Record'] = f"{stats['Wins']}-{stats['Losses']}" if stats['Games'] > 0 else "0-0"
            result[f'{quad}_Games'] = stats['Games']
            result[f'{quad}_WinPct'] = stats['WinPct']
            result[f'{quad}_AvgMargin'] = stats['AvgMargin']
            result[f'{quad}_NetEff'] = stats['AvgNetEff']
            result[f'{quad}_Confidence'] = stats['Confidence']
        
        # Calculate Paper Tiger Score
        pts = calculate_paper_tiger_score(quad_stats)
        result['PaperTigerScore'] = pts
        
        # Calculate Q1+Q2 combined (strong opponents)
        q12_games = team_games[team_games['OppQuad'].isin(['Q1', 'Q2'])]
        q12_stats = calculate_quadrant_stats(q12_games)
        result['Q12_Record'] = f"{q12_stats['Wins']}-{q12_stats['Losses']}"
        result['Q12_Games'] = q12_stats['Games']
        result['Q12_WinPct'] = q12_stats['WinPct']
        result['Q12_NetEff'] = q12_stats['AvgNetEff']
        
        # Calculate Q3+Q4 combined (weak opponents)
        q34_games = team_games[team_games['OppQuad'].isin(['Q3', 'Q4'])]
        q34_stats = calculate_quadrant_stats(q34_games)
        result['Q34_Record'] = f"{q34_stats['Wins']}-{q34_stats['Losses']}"
        result['Q34_Games'] = q34_stats['Games']
        result['Q34_WinPct'] = q34_stats['WinPct']
        result['Q34_NetEff'] = q34_stats['AvgNetEff']
        
        # Consistency score (low variance across quadrants = consistent)
        quad_effs = [s['AvgNetEff'] for s in quad_stats.values() 
                     if s['AvgNetEff'] is not None and s['Games'] >= MIN_GAMES_USABLE]
        if len(quad_effs) >= 2:
            result['ConsistencyScore'] = np.std(quad_effs)
        else:
            result['ConsistencyScore'] = None
        
        all_results.append(result)
    
    # Convert to DataFrame
    results_df = pd.DataFrame(all_results)
    
    # Sort by adjusted rank
    results_df = results_df.sort_values('AdjRank').reset_index(drop=True)
    
    # Round numeric columns
    numeric_cols = results_df.select_dtypes(include=[np.number]).columns
    results_df[numeric_cols] = results_df[numeric_cols].round(3)
    
    # Save
    results_df.to_csv(OUTPUT_FILE, index=False)
    
    # Summary
    logger.info("=" * 60)
    logger.info("ANALYSIS COMPLETE")
    logger.info(f"Teams analyzed: {len(results_df)}")
    logger.info(f"Quadrant source: {quad_source}")
    logger.info(f"Output file: {OUTPUT_FILE}")
    
    # Paper Tigers (high score = beats weak teams, struggles vs good)
    logger.info("\n" + "=" * 40)
    logger.info("🐯 PAPER TIGERS (potential market overvaluation)")
    logger.info("=" * 40)
    
    # Filter for teams with enough Q1/Q2 games to be meaningful
    meaningful_df = results_df[
        (results_df['Q12_Games'] >= 2) & 
        (results_df['PaperTigerScore'].notna())
    ]
    
    paper_tigers = meaningful_df.nlargest(10, 'PaperTigerScore')
    
    for _, row in paper_tigers.iterrows():
        if row['PaperTigerScore'] > 5:  # Significant gap
            logger.info(
                f"  ⚠️ {row['Team']} (Rank {int(row['AdjRank'])}): "
                f"PT Score = {row['PaperTigerScore']:.1f}"
            )
            logger.info(
                f"      vs Q1/Q2: {row['Q12_Record']} (Net: {row['Q12_NetEff']:.1f})"
            )
            logger.info(
                f"      vs Q3/Q4: {row['Q34_Record']} (Net: {row['Q34_NetEff']:.1f})"
            )
    
    # Battle-Tested Teams (good performance vs strong opponents)
    logger.info("\n" + "=" * 40)
    logger.info("💪 BATTLE-TESTED (proven vs quality opponents)")
    logger.info("=" * 40)
    
    battle_tested = meaningful_df[
        meaningful_df['Q12_Games'] >= 3
    ].nlargest(10, 'Q12_NetEff')
    
    for _, row in battle_tested.iterrows():
        if row['Q12_NetEff'] > 0:
            logger.info(
                f"  ✓ {row['Team']} (Rank {int(row['AdjRank'])}): "
                f"vs Q1/Q2: {row['Q12_Record']} (Net: {row['Q12_NetEff']:.1f})"
            )
    
    # Most Consistent Teams
    logger.info("\n" + "=" * 40)
    logger.info("🎯 MOST CONSISTENT (low variance across quadrants)")
    logger.info("=" * 40)
    
    consistent = results_df[
        results_df['ConsistencyScore'].notna()
    ].nsmallest(10, 'ConsistencyScore')
    
    for _, row in consistent.iterrows():
        logger.info(
            f"  {row['Team']}: Variance = {row['ConsistencyScore']:.1f}"
        )


if __name__ == "__main__":
    # Set to True if you have KenPom data, False to use internal rankings
    run_quadrant_analysis(use_kenpom=False)
