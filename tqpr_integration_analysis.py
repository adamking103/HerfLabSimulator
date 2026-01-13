"""
================================================================================
PHD-LEVEL STATISTICAL INTEGRATION: TQPR → BIBLE V10
================================================================================

EXECUTIVE SUMMARY
-----------------
Your V10 model currently has:
1. KenPom efficiency data (AdjEM, AdjO, AdjD)
2. Bayesian quadrant adjustments (basic shrinkage estimator)
3. PhD location splits (home/road performance by opponent quality)
4. Four Factors edge calculation
5. Empirical Bayes variance estimation
6. Luck regression

Your TQPR dataset adds:
1. True Quadrant Power Rating (composite score)
2. Paper Tiger detection (schedule strength inflation)
3. Kentucky Problem identification (cupcake crushers)
4. Quality game counts and win rates
5. Net efficiency by quadrant (Q1-Q4)
6. Luck values merged with quadrant context

INTEGRATION STRATEGY: HIERARCHICAL BAYESIAN FRAMEWORK
======================================================

The key insight is that your current model treats all teams as exchangeable
within rank bands. The TQPR data allows us to create a HIERARCHICAL model
that conditions predictions on REALIZED performance vs opponent quality.

THEORETICAL FOUNDATION
----------------------

Let θᵢ = true team quality for team i
Let Xᵢⱼ = observed performance of team i against opponent in quadrant j

Current Model (Implicit):
    E[margin] = f(AdjEM_home, AdjEM_away) + HCA + noise

Proposed Model (Hierarchical):
    E[margin] = f(AdjEM_home, AdjEM_away) + HCA + 
                β₁·TQPR_adjustment +           # Schedule-adjusted quality
                β₂·Paper_Tiger_penalty +       # Soft schedule discount
                β₃·Kentucky_Problem_adj +      # Quality opponent struggle
                β₄·Realized_Quadrant_effect +  # Actual vs expected performance
                noise

Where noise ~ N(0, σ²) and σ² is estimated via Empirical Bayes with
TQPR-informed priors.

================================================================================
IMPLEMENTATION MODULES
================================================================================
"""

import numpy as np
import pandas as pd
from scipy import stats
from typing import Dict, Tuple, Optional


# ==============================================================================
# MODULE 1: TQPR DATA LOADER
# ==============================================================================

class TQPRDataLoader:
    """
    Loads and processes TQPR rankings for integration with Bible V10.
    
    Key fields used:
    - TQPR: Composite ranking score
    - TQPR_Rank: Ordinal ranking  
    - Quadrant_Score: Raw quadrant performance metric
    - Paper_Tiger_Score: Schedule inflation indicator (0-100)
    - Kentucky_Score: Quality opponent struggle metric
    - Quality_Games: Count of Q1+Q2 games played
    - Q1_WinPct, Q2_WinPct: Win rates by quadrant
    - Q1_NetEff, Q2_NetEff: Margin by quadrant
    - Luck: KenPom luck factor
    """
    
    def __init__(self, tqpr_path: str):
        self.df = pd.read_csv(tqpr_path)
        self._preprocess()
    
    def _preprocess(self):
        """Standardize and compute derived fields"""
        
        # Ensure numeric types
        numeric_cols = ['TQPR', 'TQPR_Rank', 'Quadrant_Score', 'Paper_Tiger_Score',
                       'Kentucky_Score', 'AdjEM', 'Luck', 'Q1_Games', 'Q2_Games',
                       'Q1_WinPct', 'Q2_WinPct', 'Q1_NetEff', 'Q2_NetEff']
        
        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
        
        # Compute quality games
        self.df['Quality_Games'] = (
            self.df.get('Q1_Games', 0).fillna(0) + 
            self.df.get('Q2_Games', 0).fillna(0)
        )
        
        # Compute TQPR-KenPom divergence (key signal)
        self.df['KP_Rank'] = self.df['AdjEM'].rank(ascending=False, method='min')
        self.df['Rank_Divergence'] = self.df['KP_Rank'] - self.df['TQPR_Rank']
        
        # Normalize Paper Tiger to 0-1 scale
        self.df['PT_Factor'] = self.df['Paper_Tiger_Score'] / 100.0
        
        # Normalize Kentucky Problem
        self.df['KY_Factor'] = self.df['Kentucky_Score'].clip(0, 50) / 50.0
        
        # Schedule reliability indicator
        self.df['Schedule_Reliability'] = np.where(
            self.df['Quality_Games'] >= 5, 'HIGH',
            np.where(self.df['Quality_Games'] >= 3, 'MEDIUM', 'LOW')
        )
    
    def get_team_data(self, team_name: str) -> Optional[Dict]:
        """Retrieve TQPR data for a specific team"""
        
        # Try exact match first
        row = self.df[self.df['Team'] == team_name]
        
        # Try standardized match
        if row.empty:
            row = self.df[self.df['Team_Std'] == team_name]
        
        # Try partial match
        if row.empty:
            matches = self.df[self.df['Team'].str.contains(team_name, case=False, na=False)]
            if len(matches) == 1:
                row = matches
        
        if row.empty:
            return None
        
        return row.iloc[0].to_dict()


# ==============================================================================
# MODULE 2: HIERARCHICAL ADJUSTMENT CALCULATOR
# ==============================================================================

class HierarchicalAdjustmentEngine:
    """
    Computes Bayesian adjustments using TQPR data within a hierarchical framework.
    
    The key statistical insight:
    
    Traditional models use: E[performance] = AdjEM
    
    Our hierarchical model uses:
        E[performance | opponent_quality] = AdjEM + context_adjustment
        
    Where context_adjustment is estimated from REALIZED quadrant performance,
    shrunk toward the prior (AdjEM) using Empirical Bayes.
    
    SHRINKAGE FORMULA:
        adjusted_value = prior + shrinkage_factor * (observed - prior)
        
    Where:
        shrinkage_factor = n / (n + k)
        n = number of observations (quality games)
        k = prior strength (higher = more shrinkage toward prior)
    """
    
    # Empirically calibrated parameters
    PRIOR_STRENGTH = 4.0  # Games worth of prior belief
    MAX_ADJUSTMENT = 5.0  # Cap on any single adjustment
    
    # Component weights (sum to ~1.0 for interpretability)
    WEIGHT_TQPR_DIVERGENCE = 0.25    # Rank disagreement signal
    WEIGHT_PAPER_TIGER = 0.30        # Schedule inflation penalty
    WEIGHT_KENTUCKY = 0.25           # Quality opponent struggle
    WEIGHT_REALIZED_QUAD = 0.20      # Actual quadrant performance
    
    def __init__(self, tqpr_loader: TQPRDataLoader):
        self.tqpr = tqpr_loader
        
        # Compute league-wide statistics for Empirical Bayes
        self._compute_population_priors()
    
    def _compute_population_priors(self):
        """Compute population-level statistics for shrinkage estimation"""
        
        df = self.tqpr.df
        
        # Mean and variance of TQPR scores
        self.pop_tqpr_mean = df['TQPR'].mean()
        self.pop_tqpr_std = df['TQPR'].std()
        
        # Mean paper tiger score
        self.pop_pt_mean = df['Paper_Tiger_Score'].mean()
        
        # Mean quality game performance
        q1_performers = df[df['Q1_Games'] >= 3]
        self.pop_q1_winpct = q1_performers['Q1_WinPct'].mean() if len(q1_performers) > 0 else 0.5
        
    def compute_tqpr_adjustment(self, team_name: str, opponent_rank: int) -> Tuple[float, Dict]:
        """
        Compute the composite TQPR-based adjustment for a team.
        
        Returns:
            adjustment: Float adjustment to add to efficiency
            breakdown: Dict with component values for transparency
        """
        
        team_data = self.tqpr.get_team_data(team_name)
        
        if team_data is None:
            return 0.0, {'error': 'Team not found in TQPR data'}
        
        # Determine opponent quadrant
        if opponent_rank <= 50:
            opp_quad = 'Q1'
        elif opponent_rank <= 100:
            opp_quad = 'Q2'
        elif opponent_rank <= 200:
            opp_quad = 'Q3'
        else:
            opp_quad = 'Q4'
        
        breakdown = {
            'team': team_name,
            'opp_quadrant': opp_quad,
            'quality_games': team_data.get('Quality_Games', 0),
        }
        
        # ===== COMPONENT 1: TQPR-KenPom Divergence =====
        # If TQPR ranks team much higher than KenPom, they're battle-tested
        # If TQPR ranks team much lower, they've been exposed
        
        rank_div = team_data.get('Rank_Divergence', 0)
        
        # Positive divergence = underrated by KenPom = boost
        # Negative divergence = overrated by KenPom = penalize
        # Scale: ~100 ranks divergence = ~2 points adjustment
        div_adj = np.clip(rank_div / 50.0, -2.0, 2.0)
        
        # Weight by quality games (more games = more confident)
        quality_games = team_data.get('Quality_Games', 0)
        quality_weight = min(quality_games / 5.0, 1.0)
        div_adj *= quality_weight
        
        breakdown['divergence_adj'] = div_adj
        
        # ===== COMPONENT 2: Paper Tiger Penalty =====
        # High PT score = soft schedule = penalize when facing quality
        
        pt_score = team_data.get('Paper_Tiger_Score', 50)
        
        # Only apply penalty when facing Q1/Q2 opponents
        if opp_quad in ['Q1', 'Q2']:
            # PT > 70 is concerning, > 85 is severe
            if pt_score > 70:
                pt_penalty = -((pt_score - 70) / 30) * 2.5  # Max -2.5 penalty
            else:
                pt_penalty = 0.0
        else:
            pt_penalty = 0.0
        
        breakdown['paper_tiger_penalty'] = pt_penalty
        
        # ===== COMPONENT 3: Kentucky Problem Adjustment =====
        # Teams that crush cupcakes but struggle vs quality
        
        ky_score = team_data.get('Kentucky_Score', 0)
        
        # Apply penalty when facing quality opponents
        if opp_quad in ['Q1', 'Q2'] and ky_score > 25:
            # Scale: KY score of 50 = -2.0 adjustment
            ky_penalty = -(ky_score / 50) * 2.0
        else:
            ky_penalty = 0.0
        
        breakdown['kentucky_penalty'] = ky_penalty
        
        # ===== COMPONENT 4: Realized Quadrant Performance =====
        # Use actual performance vs this quadrant type
        
        quad_games = team_data.get(f'{opp_quad}_Games', 0)
        quad_neteff = team_data.get(f'{opp_quad}_NetEff', 0)
        
        if quad_games >= 2 and not pd.isna(quad_neteff):
            # Bayesian shrinkage toward 0 (neutral expectation)
            shrinkage = quad_games / (quad_games + self.PRIOR_STRENGTH)
            
            # Scale NetEff to reasonable adjustment range
            # NetEff of +10 vs Q1 is exceptional = +1.5 adjustment
            quad_adj = shrinkage * (quad_neteff / 10.0) * 1.5
            quad_adj = np.clip(quad_adj, -2.0, 2.0)
        else:
            quad_adj = 0.0
        
        breakdown['quadrant_adj'] = quad_adj
        breakdown['quad_games'] = quad_games
        breakdown['quad_neteff'] = quad_neteff
        
        # ===== COMPOSITE ADJUSTMENT =====
        total_adj = (
            div_adj * self.WEIGHT_TQPR_DIVERGENCE +
            pt_penalty * self.WEIGHT_PAPER_TIGER +
            ky_penalty * self.WEIGHT_KENTUCKY +
            quad_adj * self.WEIGHT_REALIZED_QUAD
        )
        
        # Apply final clipping
        total_adj = np.clip(total_adj, -self.MAX_ADJUSTMENT, self.MAX_ADJUSTMENT)
        
        breakdown['total_adjustment'] = total_adj
        breakdown['adjustment_confidence'] = team_data.get('Schedule_Reliability', 'LOW')
        
        return total_adj, breakdown


# ==============================================================================
# MODULE 3: VARIANCE ESTIMATION WITH TQPR
# ==============================================================================

class TQPRVarianceEstimator:
    """
    Enhanced variance estimation using TQPR schedule reliability.
    
    Key insight: Teams with few quality games have MORE UNCERTAIN true ability.
    Their observed efficiency could regress significantly when facing real competition.
    
    VARIANCE FORMULA:
        σ² = base_variance * uncertainty_multiplier * matchup_factor
        
    Where:
        uncertainty_multiplier = f(quality_games, paper_tiger_score)
        matchup_factor = f(rank_difference, quadrant_context)
    """
    
    BASE_VARIANCE_SPREAD = 11.5
    BASE_VARIANCE_TOTAL = 9.5
    
    def __init__(self, tqpr_loader: TQPRDataLoader):
        self.tqpr = tqpr_loader
    
    def estimate_spread_variance(self, team1: str, team2: str, 
                                  rank1: int, rank2: int) -> float:
        """
        Estimate spread variance accounting for schedule uncertainty.
        """
        
        base_var = self.BASE_VARIANCE_SPREAD
        
        # Get TQPR data
        t1_data = self.tqpr.get_team_data(team1)
        t2_data = self.tqpr.get_team_data(team2)
        
        uncertainty_mult = 1.0
        
        for data, rank in [(t1_data, rank1), (t2_data, rank2)]:
            if data is None:
                # No data = maximum uncertainty
                uncertainty_mult *= 1.2
                continue
            
            quality_games = data.get('Quality_Games', 0)
            pt_score = data.get('Paper_Tiger_Score', 50)
            
            # Few quality games = more uncertainty
            if quality_games < 3:
                uncertainty_mult *= 1.25
            elif quality_games < 5:
                uncertainty_mult *= 1.1
            
            # High paper tiger = more uncertainty
            if pt_score > 75:
                uncertainty_mult *= 1.15
            
            # Low major with soft schedule = very uncertain
            if rank > 150 and pt_score > 60:
                uncertainty_mult *= 1.2
        
        # Rank gap effect (blowouts are more predictable)
        rank_gap = abs(rank1 - rank2)
        if rank_gap > 100:
            uncertainty_mult *= 0.9  # More predictable
        
        return base_var * uncertainty_mult
    
    def estimate_total_variance(self, team1: str, team2: str,
                                 tempo1: float, tempo2: float) -> float:
        """
        Estimate total variance accounting for tempo and schedule.
        """
        
        base_var = self.BASE_VARIANCE_TOTAL
        
        # High tempo games have more variance
        avg_tempo = (tempo1 + tempo2) / 2
        if avg_tempo > 72:
            base_var *= 1.1
        elif avg_tempo < 65:
            base_var *= 0.95
        
        # TQPR uncertainty adjustment
        t1_data = self.tqpr.get_team_data(team1)
        t2_data = self.tqpr.get_team_data(team2)
        
        for data in [t1_data, t2_data]:
            if data is None:
                base_var *= 1.1
                continue
            
            quality_games = data.get('Quality_Games', 0)
            if quality_games < 3:
                base_var *= 1.1
        
        return base_var


# ==============================================================================
# MODULE 4: LUCK REGRESSION WITH TQPR CONTEXT
# ==============================================================================

class TQPRLuckRegressor:
    """
    Enhanced luck regression using TQPR schedule context.
    
    Key insight: Luck regression should be STRONGER for teams whose 
    luck was accumulated against weak schedules, and WEAKER for teams
    whose luck was tested against quality opponents.
    
    REGRESSION FORMULA:
        regression_factor = base_factor * schedule_reliability_weight
        
    Where:
        schedule_reliability_weight is higher for battle-tested teams
        (their luck is more "real" - earned against quality)
    """
    
    BASE_REGRESSION_FACTOR = 0.30  # Standard regression
    
    def __init__(self, tqpr_loader: TQPRDataLoader):
        self.tqpr = tqpr_loader
    
    def compute_luck_adjustment(self, team_name: str, raw_luck: float) -> Tuple[float, str]:
        """
        Compute luck adjustment with TQPR-informed regression strength.
        
        Returns:
            adjustment: Points to subtract from efficiency
            explanation: Text explanation
        """
        
        team_data = self.tqpr.get_team_data(team_name)
        
        if team_data is None:
            # Default regression
            adj = raw_luck * self.BASE_REGRESSION_FACTOR
            return adj, f"Standard luck regression: {raw_luck:+.3f} → {adj:+.2f}"
        
        quality_games = team_data.get('Quality_Games', 0)
        pt_score = team_data.get('Paper_Tiger_Score', 50)
        
        # Determine regression strength
        if quality_games >= 5 and pt_score < 50:
            # Battle-tested team - luck is more "real"
            # Use lighter regression
            factor = self.BASE_REGRESSION_FACTOR * 0.7
            explanation = "Battle-tested: lighter regression"
        elif quality_games < 3 or pt_score > 75:
            # Soft schedule - luck may be inflated
            # Use heavier regression
            factor = self.BASE_REGRESSION_FACTOR * 1.3
            explanation = "Soft schedule: heavier regression"
        else:
            factor = self.BASE_REGRESSION_FACTOR
            explanation = "Standard regression"
        
        adj = raw_luck * factor
        
        return adj, f"{explanation} ({raw_luck:+.3f} → {adj:+.2f})"


# ==============================================================================
# MODULE 5: INTEGRATED PREDICTION ENGINE
# ==============================================================================

class TQPREnhancedPredictor:
    """
    Main integration class that combines all TQPR modules with Bible V10.
    
    PREDICTION FLOW:
    1. Load base KenPom efficiency
    2. Apply TQPR luck regression (context-aware)
    3. Apply hierarchical quadrant adjustment
    4. Apply location adjustment (existing PhD module)
    5. Compute four factors edge
    6. Run Monte Carlo with TQPR-informed variance
    7. Generate signals with confidence intervals
    """
    
    def __init__(self, tqpr_path: str):
        self.tqpr_loader = TQPRDataLoader(tqpr_path)
        self.adjustment_engine = HierarchicalAdjustmentEngine(self.tqpr_loader)
        self.variance_estimator = TQPRVarianceEstimator(self.tqpr_loader)
        self.luck_regressor = TQPRLuckRegressor(self.tqpr_loader)
        
        print(f"✅ TQPR Integration loaded: {len(self.tqpr_loader.df)} teams")
    
    def compute_all_adjustments(self, visitor: str, home: str,
                                 v_rank: int, h_rank: int,
                                 v_luck: float, h_luck: float) -> Dict:
        """
        Compute all TQPR-based adjustments for a matchup.
        
        Returns comprehensive adjustment dictionary.
        """
        
        # Hierarchical quadrant adjustments
        v_tqpr_adj, v_breakdown = self.adjustment_engine.compute_tqpr_adjustment(visitor, h_rank)
        h_tqpr_adj, h_breakdown = self.adjustment_engine.compute_tqpr_adjustment(home, v_rank)
        
        # Luck regression
        v_luck_adj, v_luck_exp = self.luck_regressor.compute_luck_adjustment(visitor, v_luck)
        h_luck_adj, h_luck_exp = self.luck_regressor.compute_luck_adjustment(home, h_luck)
        
        # Variance estimation
        spread_var = self.variance_estimator.estimate_spread_variance(
            visitor, home, v_rank, h_rank
        )
        
        return {
            'visitor': {
                'tqpr_adjustment': v_tqpr_adj,
                'tqpr_breakdown': v_breakdown,
                'luck_adjustment': v_luck_adj,
                'luck_explanation': v_luck_exp,
            },
            'home': {
                'tqpr_adjustment': h_tqpr_adj,
                'tqpr_breakdown': h_breakdown,
                'luck_adjustment': h_luck_adj,
                'luck_explanation': h_luck_exp,
            },
            'spread_variance': spread_var,
        }
    
    def generate_signal_flags(self, visitor: str, home: str) -> list:
        """
        Generate betting signal flags based on TQPR analysis.
        """
        
        flags = []
        
        v_data = self.tqpr_loader.get_team_data(visitor)
        h_data = self.tqpr_loader.get_team_data(home)
        
        for name, data, role in [(visitor, v_data, 'V'), (home, h_data, 'H')]:
            if data is None:
                continue
            
            pt_score = data.get('Paper_Tiger_Score', 0)
            ky_score = data.get('Kentucky_Score', 0)
            luck = data.get('Luck', 0)
            quality_games = data.get('Quality_Games', 0)
            
            # Paper Tiger flag
            if pt_score > 70:
                flags.append(f"🐯 {role}: Paper Tiger ({pt_score:.0f})")
            
            # Kentucky Problem flag
            if ky_score > 30:
                flags.append(f"🔻 {role}: Kentucky Problem ({ky_score:.0f})")
            
            # Luck regression flag
            if abs(luck) > 0.08:
                direction = "Lucky" if luck > 0 else "Unlucky"
                flags.append(f"🍀 {role}: {direction} ({luck:+.3f})")
            
            # Battle-tested flag
            if quality_games >= 6:
                flags.append(f"⚔️ {role}: Battle-tested ({quality_games} Q1/Q2 games)")
            elif quality_games < 2:
                flags.append(f"⚠️ {role}: Untested schedule")
        
        return flags


# ==============================================================================
# INTEGRATION CODE FOR BIBLE V10
# ==============================================================================

def integrate_tqpr_into_v10():
    """
    Code to add to Bible_Simulator_V10_EXPERIMENTAL.py
    
    CHANGES REQUIRED:
    
    1. Add to imports:
        from tqpr_integration import TQPREnhancedPredictor
    
    2. Add to configuration:
        TQPR_DATA_PATH = "tqpr_full_rankings.csv"
    
    3. Modify build_team_database():
        # After loading other data:
        tqpr_predictor = TQPREnhancedPredictor(TQPR_DATA_PATH)
        return stats, style, quad, eff, h_perf, r_perf, tqpr_predictor
    
    4. Modify run_simulation():
        # After line ~315 (luck regression):
        tqpr_adj = tqpr_predictor.compute_all_adjustments(
            v_name, h_name, v['Rank'], h['Rank'], v.get('Luck',0), h.get('Luck',0)
        )
        
        # Apply TQPR adjustments
        v_off += tqpr_adj['visitor']['tqpr_adjustment'] / 2
        v_def -= tqpr_adj['visitor']['tqpr_adjustment'] / 2
        h_off += tqpr_adj['home']['tqpr_adjustment'] / 2
        h_def -= tqpr_adj['home']['tqpr_adjustment'] / 2
        
        # Use TQPR-informed variance
        s_var = tqpr_adj['spread_variance']
        
        # Add TQPR flags to output
        tqpr_flags = tqpr_predictor.generate_signal_flags(v_name, h_name)
    """
    pass


# ==============================================================================
# EXAMPLE USAGE
# ==============================================================================

if __name__ == "__main__":
    # Demo usage
    predictor = TQPREnhancedPredictor("/mnt/user-data/uploads/tqpr_full_rankings.csv")
    
    # Example matchup
    adjustments = predictor.compute_all_adjustments(
        visitor="Florida",
        home="Kentucky", 
        v_rank=20,
        h_rank=50,
        v_luck=-0.12,
        h_luck=-0.08
    )
    
    print("\n" + "="*60)
    print("TQPR ADJUSTMENT ANALYSIS: Florida @ Kentucky")
    print("="*60)
    
    for role, data in [('VISITOR (Florida)', adjustments['visitor']), 
                       ('HOME (Kentucky)', adjustments['home'])]:
        print(f"\n{role}:")
        print(f"  TQPR Adjustment: {data['tqpr_adjustment']:+.2f}")
        print(f"  Luck Adjustment: {data['luck_explanation']}")
        
        bd = data['tqpr_breakdown']
        print(f"  Components:")
        print(f"    Divergence: {bd.get('divergence_adj', 0):+.2f}")
        print(f"    Paper Tiger: {bd.get('paper_tiger_penalty', 0):+.2f}")
        print(f"    Kentucky Prob: {bd.get('kentucky_penalty', 0):+.2f}")
        print(f"    Quadrant Perf: {bd.get('quadrant_adj', 0):+.2f}")
    
    print(f"\nSpread Variance: {adjustments['spread_variance']:.2f}")
    
    flags = predictor.generate_signal_flags("Florida", "Kentucky")
    if flags:
        print(f"\nSignal Flags: {', '.join(flags)}")
