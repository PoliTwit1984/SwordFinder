#!/usr/bin/env python3
"""
Test script for the database sword finder
"""
import os
from simple_db_swordfinder import SimpleDatabaseSwordFinder

# Set up database connection
os.environ['DATABASE_URL'] = "postgresql://swordfinder:swordfinder123@localhost:5432/swordfinder_db"

# Create finder instance
finder = SimpleDatabaseSwordFinder()

# Test with recent date from our data range
test_date = '2025-05-25'
print(f"Testing sword finder for date: {test_date}")

result = finder.find_sword_swings(test_date)

print(f"Success: {result['success']}")
print(f"Count: {result['count']}")
print(f"Error: {result.get('error', 'None')}")

if result['success'] and result['data']:
    print(f"\nFirst sword swing found:")
    swing = result['data'][0]
    print(f"Player: {swing['player_name']}")
    print(f"Pitch Type: {swing['pitch_type']}")
    print(f"Sword Score: {swing['sword_score']}")
    print(f"Bat Speed: {swing['bat_speed']}")
    print(f"Swing Path Tilt: {swing['swing_path_tilt']}")
    print(f"Intercept Y: {swing['intercept_y']}")
else:
    print("No sword swings found for this date")
