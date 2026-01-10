import pandas as pd

# Load the file
try:
    df = pd.read_csv('team_home_performance_VALIDATED_2026.csv')
    print("\n✅ FILE LOADED SUCCESSFULLY")
    print("="*40)
    print("HERE ARE YOUR EXACT COLUMN NAMES:")
    print("="*40)
    print(df.columns.tolist())
    print("="*40)
except FileNotFoundError:
    print("❌ Error: Could not find the CSV file.")
