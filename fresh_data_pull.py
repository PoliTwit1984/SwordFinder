#!/usr/bin/env python3
"""
Pull fresh Statcast data with all sword swing fields from pybaseball
"""

from pybaseball import statcast
import pandas as pd
from datetime import datetime, timedelta

def pull_fresh_statcast_data():
    """
    Pull fresh data from pybaseball with all the sword swing fields
    """
    print("Pulling fresh Statcast data with sword swing fields...")
    
    # Get recent data - let's go back about 2 months to get plenty of games
    end_date = "2025-05-24"  # Most recent date in your original file
    start_date = "2025-03-28"  # Start from opening day
    
    print(f"Fetching data from {start_date} to {end_date}...")
    
    try:
        # Pull the data
        data = statcast(start_dt=start_date, end_dt=end_date)
        
        print(f"Successfully pulled {len(data)} records")
        print(f"Date range: {data['game_date'].min()} to {data['game_date'].max()}")
        
        # Check sword swing field availability
        sword_fields = ['bat_speed', 'swing_length', 'intercept_ball_minus_batter_pos_y_inches', 
                       'swing_path_tilt', 'attack_angle']
        
        print("\nSword swing field data:")
        for field in sword_fields:
            if field in data.columns:
                non_null = data[field].notna().sum()
                print(f"  {field}: {non_null:,}/{len(data):,} records")
        
        # Check swinging strikes
        swinging_strikes = data[data['description'].isin(['swinging_strike', 'swinging_strike_blocked'])]
        print(f"\nSwinging strikes found: {len(swinging_strikes):,}")
        
        if len(swinging_strikes) > 0:
            print("Swinging strikes with sword data:")
            for field in sword_fields:
                if field in swinging_strikes.columns:
                    has_data = swinging_strikes[field].notna().sum()
                    print(f"  {field}: {has_data:,}/{len(swinging_strikes):,}")
        
        # Save the fresh data
        output_file = "fresh_statcast_2025_complete.csv"
        data.to_csv(output_file, index=False)
        print(f"\nSaved complete dataset to {output_file}")
        
        return data
        
    except Exception as e:
        print(f"Error pulling data: {e}")
        return None

if __name__ == "__main__":
    data = pull_fresh_statcast_data()