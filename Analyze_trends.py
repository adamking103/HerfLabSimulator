import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ==============================================================================
# CONFIGURATION
# ==============================================================================
HOME_FILE = 'team_home_performance_VALIDATED_2026.csv'
ROAD_FILE = 'team_road_performance_VALIDATED_2026.csv'

# Thresholds for "Betable" Trends
MIN_GAMES = 4        # Minimum games played in that setting to qualify
DIFF_THRESHOLD = 8.0 # How many points better they must be at home to qualify

# ==============================================================================
# LOAD AND MERGE DATA
# ==============================================================================
try:
    df_home = pd.read_csv(HOME_FILE)
    df_road = pd.read_csv(ROAD_FILE)
    print(f"✅ Loaded {len(df_home)} Home Profiles and {len(df_road)} Road Profiles.")
except FileNotFoundError:
    print("❌ Error: Could not find your validated CSV files. Make sure they are in the same folder.")
    exit()

# Merge on Team Name
# Suffixes _h (Home) and _r (Road) will be added automatically
# Example: 'Overall_NetEff' becomes 'Overall_NetEff_h' and 'Overall_NetEff_r'
df = pd.merge(df_home, df_road, on="Team", suffixes=('_h', '_r'))

# ==============================================================================
# CALCULATE THE "SPLIT" (Home Advantage Metric)
# ==============================================================================
# NetEff is already your efficiency margin (Points Per 100 Possessions better/worse than average)

# The "Jekyll & Hyde" Factor: How much better are they at home?
df['Home_Court_Advantage'] = df['Overall_NetEff_h'] - df['Overall_NetEff_r']

# Filter for Sample Size to remove noise (e.g., teams with only 1 road game)
# Using your actual column name: 'Total_Games'
df = df[ (df['Total_Games_h'] >= MIN_GAMES) & (df['Total_Games_r'] >= MIN_GAMES) ]

# ==============================================================================
# IDENTIFY THE "MONEY" TEAMS
# ==============================================================================

# 1. HOME FORTRESSES (Teams typically undervalued at home)
home_heroes = df[df['Home_Court_Advantage'] > DIFF_THRESHOLD].sort_values(by='Home_Court_Advantage', ascending=False)

# 2. ROAD WARRIORS (Teams that play just as well or better on road - rare!)
road_warriors = df[df['Home_Court_Advantage'] < 0].sort_values(by='Home_Court_Advantage', ascending=True)

# ==============================================================================
# REPORTING
# ==============================================================================
print("\n" + "="*80)
print("🏀 THE 'JEKYLL & HYDE' REPORT (2026 Season)")
print("="*80)

print(f"\n🏰 TOP {len(home_heroes.head(15))} HOME COURT HEROES (Bet ON at Home)")
print(f"   Criteria: Play > {DIFF_THRESHOLD} pts better per 100 poss at home")
print("-" * 80)
# Selecting specific columns for clean output
print(home_heroes[['Team', 'Overall_NetEff_h', 'Overall_NetEff_r', 'Home_Court_Advantage']].head(15).to_string(index=False))

print(f"\n🚗 TOP {len(road_warriors.head(15))} ROAD WARRIORS (Value on Road)")
print("   Criteria: Net Rating represents consistent travel performance")
print("-" * 80)
print(road_warriors[['Team', 'Overall_NetEff_h', 'Overall_NetEff_r', 'Home_Court_Advantage']].head(15).to_string(index=False))

# ==============================================================================
# VISUALIZATION SCATTER PLOT
# ==============================================================================
plt.figure(figsize=(12, 8))
sns.scatterplot(data=df, x='Overall_NetEff_r', y='Overall_NetEff_h', alpha=0.6)

# Add reference line (y=x) where Home = Road
# Teams ON this line play the same everywhere.
# Teams ABOVE this line are better at home.
plt.plot([-40, 40], [-40, 40], color='red', linestyle='--', label='Neutral (Same Home/Road)')

# Highlight Top 5 Home Heavy teams
for i in range(5):
    if i < len(home_heroes):
        team = home_heroes.iloc[i]
        # Offset the text slightly so it doesn't cover the dot
        plt.text(team['Overall_NetEff_r'], team['Overall_NetEff_h']+1, team['Team'], weight='bold', color='blue')

plt.title(f"Home vs. Road Efficiency (n={len(df)} Teams)")
plt.xlabel("Road Efficiency (NetEff)")
plt.ylabel("Home Efficiency (NetEff)")
plt.legend()
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()
