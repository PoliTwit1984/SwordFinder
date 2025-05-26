#!/usr/bin/env python3
"""
Fix missing pitch details in database by updating records with complete CSV data
"""
import pandas as pd
import os
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_missing_pitch_data():
    """
    Update database records that are missing pitch details like velocity, teams, etc.
    """
    # Connect to database
    database_url = os.environ.get('DATABASE_URL')
    engine = create_engine(database_url)
    
    logger.info("Loading complete CSV data...")
    df = pd.read_csv('complete_statcast_2025.csv')
    
    # Focus on records that have the missing data we need
    complete_records = df[
        (df['release_speed'].notna()) & 
        (df['home_team'].notna()) & 
        (df['away_team'].notna()) &
        (df['plate_x'].notna()) &
        (df['plate_z'].notna()) &
        (df['release_spin_rate'].notna()) &
        (df['bat_speed'].notna()) &
        (df['swing_path_tilt'].notna()) &
        (df['intercept_ball_minus_batter_pos_y_inches'].notna()) &
        (df['description'].isin(['swinging_strike', 'swinging_strike_blocked']))
    ]
    
    logger.info(f"Found {len(complete_records)} records with complete pitch data")
    
    # Update database records in batches
    with engine.connect() as conn:
        count = 0
        for _, row in complete_records.iterrows():
            try:
                update_query = text("""
                    UPDATE statcast_pitches 
                    SET release_speed = :release_speed,
                        home_team = :home_team,
                        away_team = :away_team,
                        plate_x = :plate_x,
                        plate_z = :plate_z,
                        release_spin_rate = :release_spin_rate,
                        pitch_name = :pitch_name,
                        at_bat_number = :at_bat_number,
                        pitch_number = :pitch_number,
                        pfx_x = :pfx_x,
                        pfx_z = :pfx_z,
                        sz_top = :sz_top,
                        sz_bot = :sz_bot,
                        sv_id = :sv_id
                    WHERE game_date = :game_date 
                    AND player_name = :player_name
                    AND pitch_type = :pitch_type
                    AND abs(bat_speed - :bat_speed) < 0.1
                    AND abs(swing_path_tilt - :swing_path_tilt) < 0.1
                """)
                
                result = conn.execute(update_query, {
                    'release_speed': float(row['release_speed']) if pd.notna(row['release_speed']) else None,
                    'home_team': str(row['home_team']) if pd.notna(row['home_team']) else None,
                    'away_team': str(row['away_team']) if pd.notna(row['away_team']) else None,
                    'plate_x': float(row['plate_x']) if pd.notna(row['plate_x']) else None,
                    'plate_z': float(row['plate_z']) if pd.notna(row['plate_z']) else None,
                    'release_spin_rate': float(row['release_spin_rate']) if pd.notna(row['release_spin_rate']) else None,
                    'pitch_name': str(row['pitch_name']) if pd.notna(row['pitch_name']) else None,
                    'at_bat_number': int(row['at_bat_number']) if pd.notna(row['at_bat_number']) else None,
                    'pitch_number': int(row['pitch_number']) if pd.notna(row['pitch_number']) else None,
                    'pfx_x': float(row['pfx_x']) if pd.notna(row['pfx_x']) else None,
                    'pfx_z': float(row['pfx_z']) if pd.notna(row['pfx_z']) else None,
                    'sz_top': float(row['sz_top']) if pd.notna(row['sz_top']) else None,
                    'sz_bot': float(row['sz_bot']) if pd.notna(row['sz_bot']) else None,
                    'sv_id': str(row['sv_id']) if pd.notna(row['sv_id']) else None,
                    'game_date': str(row['game_date']),
                    'player_name': str(row['player_name']),
                    'pitch_type': str(row['pitch_type']),
                    'bat_speed': float(row['bat_speed']),
                    'swing_path_tilt': float(row['swing_path_tilt'])
                })
                
                if result.rowcount > 0:
                    count += result.rowcount
                    if count % 100 == 0:
                        logger.info(f"Updated {count} records...")
                        
            except Exception as e:
                logger.warning(f"Error updating record: {e}")
                continue
        
        conn.commit()
        logger.info(f"Successfully updated {count} records with complete pitch data!")

if __name__ == "__main__":
    fix_missing_pitch_data()