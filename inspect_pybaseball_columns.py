#!/usr/bin/env python3
"""
Diagnostic script to check pybaseball field coverage for key columns
"""
from pybaseball import statcast
import pandas as pd

def inspect_columns():
    """
    Check field coverage for a single day sample
    """
    print("Pulling sample data from pybaseball for 2025-05-24...")
    
    # Pull one day of data
    df = statcast(start_dt="2025-05-24", end_dt="2025-05-24")
    
    print(f"Retrieved {len(df)} records")
    print("\n" + "="*50)
    print("FIELD COVERAGE ANALYSIS")
    print("="*50)
    
    # Key columns we need for complete sword swing analysis
    key_columns = [
        "release_speed",
        "release_spin_rate", 
        "spin_axis",
        "plate_x",
        "plate_z",
        "home_team",
        "away_team", 
        "pitch_name",
        "sz_top",
        "sz_bot",
        "bat_speed",
        "swing_path_tilt",
        "attack_angle",
        "intercept_ball_minus_batter_pos_y_inches"
    ]
    
    coverage = df[key_columns].notnull().mean()
    
    for col in key_columns:
        percentage = coverage[col] * 100
        status = "✅ GOOD" if percentage > 80 else "⚠️  SPARSE" if percentage > 20 else "❌ MISSING"
        print(f"{col:40} {percentage:6.1f}% {status}")
    
    print("\n" + "="*50)
    print("SAMPLE DATA PREVIEW")
    print("="*50)
    
    # Show a few complete records
    complete_records = df.dropna(subset=['release_speed', 'home_team', 'plate_x'])
    if len(complete_records) > 0:
        sample = complete_records.iloc[0]
        print(f"Sample record:")
        print(f"  Player: {sample.get('player_name', 'N/A')}")
        print(f"  Teams: {sample.get('away_team', 'N/A')} @ {sample.get('home_team', 'N/A')}")
        print(f"  Velocity: {sample.get('release_speed', 'N/A')} mph")
        print(f"  Spin Rate: {sample.get('release_spin_rate', 'N/A')} rpm")
        print(f"  Location: ({sample.get('plate_x', 'N/A')}, {sample.get('plate_z', 'N/A')})")
    else:
        print("No complete records found in sample")
    
    print(f"\nTotal columns available: {len(df.columns)}")
    print(f"Available columns: {list(df.columns[:10])}..." if len(df.columns) > 10 else f"Available columns: {list(df.columns)}")

if __name__ == "__main__":
    inspect_columns()