#!/usr/bin/env python3
"""
Import complete Statcast data with all sword swing fields into PostgreSQL database
"""

import pandas as pd
import logging
from models import get_db, StatcastPitch, create_tables
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def import_complete_statcast_data(csv_path: str = 'complete_statcast_2025.csv'):
    """
    Import complete Statcast data from CSV file into database
    """
    logger.info(f"Starting import from {csv_path}")
    
    try:
        # Load CSV file
        logger.info("Loading CSV file...")
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} records from CSV")
        
        # Ensure tables exist
        create_tables()
        
        with get_db() as db:
            # Clear existing data
            logger.info("Clearing existing data...")
            db.execute(text("DELETE FROM sword_swings"))
            db.execute(text("DELETE FROM daily_results"))
            db.execute(text("DELETE FROM statcast_pitches"))
            db.commit()
            
            # Process in chunks for better memory management
            chunk_size = 1000
            total_chunks = (len(df) + chunk_size - 1) // chunk_size
            
            for i in range(0, len(df), chunk_size):
                chunk_num = (i // chunk_size) + 1
                logger.info(f"Processing chunk {chunk_num}/{total_chunks} ({chunk_size} records)")
                
                chunk = df.iloc[i:i + chunk_size]
                records = []
                
                for _, row in chunk.iterrows():
                    record = StatcastPitch(
                        # Game info
                        game_pk=safe_int(row.get('game_pk')),
                        game_date=safe_str(row.get('game_date')),
                        home_team=safe_str(row.get('home_team')),
                        away_team=safe_str(row.get('away_team')),
                        inning=safe_int(row.get('inning')),
                        
                        # At-bat info
                        at_bat_number=safe_int(row.get('at_bat_number')),
                        pitch_number=safe_int(row.get('pitch_number')),
                        balls=safe_int(row.get('balls')),
                        strikes=safe_int(row.get('strikes')),
                        
                        # Players
                        batter=safe_int(row.get('batter')),
                        pitcher=safe_int(row.get('pitcher')),
                        batter_name=safe_str(row.get('player_name')),  # This is the batter name in the CSV
                        pitcher_name=safe_str(row.get('pitcher_name')),
                        
                        # Pitch characteristics
                        pitch_type=safe_str(row.get('pitch_type')),
                        pitch_name=safe_str(row.get('pitch_name')),
                        release_speed=safe_float(row.get('release_speed')),
                        release_spin_rate=safe_float(row.get('release_spin_rate')),
                        release_extension=safe_float(row.get('release_extension')),
                        
                        # Pitch location
                        plate_x=safe_float(row.get('plate_x')),
                        plate_z=safe_float(row.get('plate_z')),
                        sz_top=safe_float(row.get('sz_top')),
                        sz_bot=safe_float(row.get('sz_bot')),
                        
                        # Pitch movement
                        pfx_x=safe_float(row.get('pfx_x')),
                        pfx_z=safe_float(row.get('pfx_z')),
                        effective_speed=safe_float(row.get('effective_speed')),
                        
                        # SWORD SWING FIELDS - These are the key ones!
                        bat_speed=safe_float(row.get('bat_speed')),
                        swing_path_tilt=safe_float(row.get('swing_path_tilt')),
                        attack_angle=safe_float(row.get('attack_angle')),
                        intercept_ball_minus_batter_pos_y_inches=safe_float(row.get('intercept_ball_minus_batter_pos_y_inches')),
                        
                        # Outcome
                        description=safe_str(row.get('description')),
                        events=safe_str(row.get('events')),
                        
                        # Play ID for video lookup
                        play_id=safe_str(row.get('sv_id'))  # sv_id is the play identifier
                    )
                    records.append(record)
                
                # Bulk insert
                db.add_all(records)
                db.commit()
                logger.info(f"Inserted {len(records)} records")
            
            # Verify the import
            total_count = db.query(StatcastPitch).count()
            logger.info(f"✅ Import completed successfully!")
            logger.info(f"Total records in database: {total_count:,}")
            
            # Check sword swing data availability
            sword_data_count = db.query(StatcastPitch).filter(
                StatcastPitch.bat_speed.isnot(None)
            ).count()
            logger.info(f"Records with sword swing data: {sword_data_count:,}")
            
            swinging_strikes = db.query(StatcastPitch).filter(
                StatcastPitch.description.in_(['swinging_strike', 'swinging_strike_blocked'])
            ).count()
            logger.info(f"Total swinging strikes: {swinging_strikes:,}")
            
            complete_sword_swings = db.query(StatcastPitch).filter(
                StatcastPitch.description.in_(['swinging_strike', 'swinging_strike_blocked']),
                StatcastPitch.bat_speed.isnot(None),
                StatcastPitch.swing_path_tilt.isnot(None),
                StatcastPitch.intercept_ball_minus_batter_pos_y_inches.isnot(None)
            ).count()
            logger.info(f"Swinging strikes with complete sword data: {complete_sword_swings:,}")
            
    except Exception as e:
        logger.error(f"Error during import: {str(e)}")
        raise

def safe_int(value):
    """Safely convert to int"""
    if pd.isna(value) or value == '':
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None

def safe_float(value):
    """Safely convert to float"""
    if pd.isna(value) or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_str(value):
    """Safely convert to string"""
    if pd.isna(value) or value == '':
        return None
    return str(value)

if __name__ == "__main__":
    import_complete_statcast_data()