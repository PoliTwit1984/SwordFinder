#!/usr/bin/env python3
"""
Pull complete Statcast data from pybaseball for the entire 2025 season
This will get all available data with all fields for comprehensive analysis
"""

from pybaseball import statcast
import pandas as pd
from datetime import datetime, timedelta
import os

def pull_complete_statcast_data():
    """
    Pull all available 2025 Statcast data from pybaseball
    """
    print("Pulling complete 2025 Statcast data from pybaseball...")
    print("This will include all games and all available fields for comprehensive analysis")
    
    # Get all 2025 data - start from spring training through current
    # MLB season typically runs March through October
    start_date = "2025-02-15"  # Include spring training
    end_date = "2025-10-31"    # Include postseason
    
    print(f"Fetching data from {start_date} to {end_date}...")
    print("Note: This may take several minutes as it's a large dataset")
    
    try:
        # Pull all the data
        data = statcast(start_dt=start_date, end_dt=end_date)
        
        print(f"\n✅ Successfully pulled {len(data):,} total records")
        print(f"📅 Date range: {data['game_date'].min()} to {data['game_date'].max()}")
        print(f"📊 Total columns: {len(data.columns)}")
        
        # Show data distribution by month
        print("\n📈 Data distribution by month:")
        monthly_counts = data['game_date'].dt.to_period('M').value_counts().sort_index()
        for month, count in monthly_counts.items():
            print(f"  {month}: {count:,} pitches")
        
        # Check sword swing field availability
        sword_fields = ['bat_speed', 'swing_length', 'intercept_ball_minus_batter_pos_y_inches', 
                       'swing_path_tilt', 'attack_angle']
        
        print("\n⚔️ SWORD SWING DATA AVAILABILITY:")
        for field in sword_fields:
            if field in data.columns:
                non_null = data[field].notna().sum()
                percentage = (non_null / len(data)) * 100
                print(f"  ✅ {field}: {non_null:,}/{len(data):,} records ({percentage:.1f}%)")
            else:
                print(f"  ❌ {field}: NOT AVAILABLE")
        
        # Check swinging strikes
        swinging_strikes = data[data['description'].isin(['swinging_strike', 'swinging_strike_blocked'])]
        print(f"\n🏏 Total swinging strikes: {len(swinging_strikes):,}")
        
        if len(swinging_strikes) > 0:
            print("Swinging strikes with complete sword data:")
            complete_sword_data = swinging_strikes.dropna(subset=sword_fields)
            print(f"  Complete sword swing records: {len(complete_sword_data):,}/{len(swinging_strikes):,}")
            
            for field in sword_fields:
                if field in swinging_strikes.columns:
                    has_data = swinging_strikes[field].notna().sum()
                    percentage = (has_data / len(swinging_strikes)) * 100
                    print(f"  {field}: {has_data:,}/{len(swinging_strikes):,} ({percentage:.1f}%)")
        
        # Show some interesting stats
        print(f"\n📊 DATASET OVERVIEW:")
        print(f"  🏟️ Games covered: {data['game_pk'].nunique():,}")
        print(f"  👨‍⚾ Unique batters: {data['batter'].nunique():,}")
        print(f"  🤾‍♂️ Unique pitchers: {data['pitcher'].nunique():,}")
        print(f"  🏀 Different pitch types: {data['pitch_type'].nunique()}")
        
        # Save the complete dataset
        output_file = "complete_statcast_2025.csv"
        print(f"\n💾 Saving complete dataset to {output_file}...")
        data.to_csv(output_file, index=False)
        
        file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
        print(f"✅ Saved {file_size_mb:.1f} MB file with complete 2025 Statcast data")
        
        print(f"\n🎯 Ready for sword swing analysis and any future baseball analytics!")
        
        return data
        
    except Exception as e:
        print(f"❌ Error pulling data: {e}")
        print("This might be due to:")
        print("  - Network connectivity issues")
        print("  - pybaseball API limits")
        print("  - Date range too large")
        print("\nTry running again or reduce the date range")
        return None

if __name__ == "__main__":
    data = pull_complete_statcast_data()