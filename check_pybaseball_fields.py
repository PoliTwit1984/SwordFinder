#!/usr/bin/env python3
"""
Utility script to check what fields are available from pybaseball
and determine if we have the data needed for sword swing analysis
"""

from pybaseball import statcast
import pandas as pd

def check_available_fields():
    """
    Check what fields are available from pybaseball statcast data
    """
    print("Checking available fields from pybaseball...")
    
    # Get a small sample to check available columns
    try:
        # Get just one day of data to check columns
        sample_data = statcast(start_dt="2025-03-28", end_dt="2025-03-28")
        
        print(f"Total columns available: {len(sample_data.columns)}")
        print("\nAll available columns:")
        for i, col in enumerate(sample_data.columns):
            print(f"{i+1:2d}. {col}")
        
        # Check for sword swing specific fields
        sword_fields = [
            'bat_speed',
            'swing_length', 
            'intercept_ball_minus_batter_pos_y_inches',
            'swing_path_tilt',
            'attack_angle'
        ]
        
        print("\n" + "="*50)
        print("SWORD SWING FIELD AVAILABILITY:")
        print("="*50)
        
        for field in sword_fields:
            if field in sample_data.columns:
                # Check if the field has actual data
                non_null_count = sample_data[field].notna().sum()
                total_count = len(sample_data)
                print(f"✅ {field}: Available ({non_null_count}/{total_count} records have data)")
            else:
                print(f"❌ {field}: NOT AVAILABLE")
        
        # Check for swinging strikes with data
        swinging_strikes = sample_data[
            sample_data['description'].isin(['swinging_strike', 'swinging_strike_blocked'])
        ]
        
        print(f"\nSwinging strikes found: {len(swinging_strikes)}")
        
        if len(swinging_strikes) > 0:
            print("\nSwinging strike data sample:")
            for field in sword_fields:
                if field in swinging_strikes.columns:
                    has_data = swinging_strikes[field].notna().sum()
                    print(f"  {field}: {has_data}/{len(swinging_strikes)} swinging strikes have data")
        
        return sample_data
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

if __name__ == "__main__":
    data = check_available_fields()