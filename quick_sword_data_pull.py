#!/usr/bin/env python3
"""
Quick pull of recent data with sword swing fields to get SwordFinder working immediately
"""

from pybaseball import statcast
import pandas as pd

def pull_recent_sword_data():
    """
    Pull just the most recent month of data to get sword swings working quickly
    """
    print("Pulling recent data with sword swing fields...")
    
    # Get just May 2025 data for immediate testing
    start_date = "2025-05-01"
    end_date = "2025-05-24"
    
    print(f"Fetching data from {start_date} to {end_date}...")
    
    try:
        # Pull recent data
        data = statcast(start_dt=start_date, end_dt=end_date)
        
        print(f"✅ Successfully pulled {len(data):,} records")
        print(f"📅 Date range: {data['game_date'].min()} to {data['game_date'].max()}")
        
        # Check sword swing data
        sword_fields = ['bat_speed', 'swing_path_tilt', 'intercept_ball_minus_batter_pos_y_inches', 'attack_angle']
        
        swinging_strikes = data[data['description'].isin(['swinging_strike', 'swinging_strike_blocked'])]
        print(f"🏏 Swinging strikes: {len(swinging_strikes):,}")
        
        complete_sword_data = swinging_strikes.dropna(subset=sword_fields)
        print(f"⚔️ Complete sword swing records: {len(complete_sword_data):,}")
        
        # Save for immediate use
        output_file = "recent_statcast_with_swords.csv"
        data.to_csv(output_file, index=False)
        print(f"💾 Saved to {output_file}")
        
        return data
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    pull_recent_sword_data()