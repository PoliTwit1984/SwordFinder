#!/usr/bin/env python3
"""
Import all Statcast data from CSV into PostgreSQL database
using the models defined in models_complete.py.
"""
import pandas as pd
import logging
import os
from datetime import datetime

# Use models and session from models_complete.py
from models_complete import StatcastPitch, get_db 

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the correct path to the CSV file at the top level of SwordFinder
CSV_FILE_PATH = '2025_full_statcast_05242025.csv'

def safe_int(value):
    """Safely convert to int"""
    try:
        if pd.isna(value):
            return None
        return int(float(value)) # float conversion handles cases like "10.0"
    except (ValueError, TypeError):
        return None

def safe_float(value):
    """Safely convert to float"""
    try:
        if pd.isna(value):
            return None
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_str(value):
    """Safely convert to string"""
    try:
        if pd.isna(value):
            return None
        return str(value).strip()
    except (ValueError, TypeError):
        return None

def import_statcast_data(csv_path: str = CSV_FILE_PATH):
    """
    Import all Statcast data from CSV file into database using models_complete.StatcastPitch
    """
    logger.info(f"Starting import from {csv_path} using models_complete.StatcastPitch")
    
    # Table creation is assumed to be handled by temp_update_schema.py or manually.
    logger.info("Assuming tables (statcast_pitches, sword_swings, etc.) already exist as per models_complete.py definition.")

    logger.info(f"Loading CSV file: {csv_path}...")
    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except FileNotFoundError:
        logger.error(f"CRITICAL ERROR: CSV file not found at {csv_path}. Please ensure the file exists in the SwordFinder directory.")
        return
    logger.info(f"Loaded {len(df)} records from CSV")
    
    with get_db() as db:
        try:
            logger.info("Clearing existing data from statcast_pitches table...")
            deleted_rows = db.query(StatcastPitch).delete()
            db.commit()
            logger.info(f"Deleted {deleted_rows} existing rows from statcast_pitches.")
            
            chunk_size = 1000
            total_chunks = (len(df) // chunk_size) + (1 if len(df) % chunk_size > 0 else 0)
            
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                current_chunk_num = (i // chunk_size) + 1
                logger.info(f"Processing chunk {current_chunk_num}/{total_chunks} ({len(chunk)} records)")
                
                pitch_objects = []
                for idx, row_data in chunk.iterrows():
                    # Map CSV columns to StatcastPitch model attributes
                    # Ensure all attributes in StatcastPitch model are covered if they exist in CSV
                    # Using .get() for safety, and safe_type conversion
                    pitch_data_args = {
                        # Core pitch identification & player IDs
                        'pitch_type': safe_str(row_data.get('pitch_type')),
                        'game_date': safe_str(row_data.get('game_date')),
                        'player_name': safe_str(row_data.get('player_name') or row_data.get('pitcher_name')), # Pitcher's name
                        'batter': safe_int(row_data.get('batter')),
                        'pitcher': safe_int(row_data.get('pitcher')),
                        'sv_id': safe_str(row_data.get('sv_id') or row_data.get('play_id')), # Video play ID
                        
                        # Pitch characteristics
                        'release_speed': safe_float(row_data.get('release_speed')),
                        'release_pos_x': safe_float(row_data.get('release_pos_x')),
                        'release_pos_z': safe_float(row_data.get('release_pos_z')),
                        'release_spin_rate': safe_float(row_data.get('release_spin_rate')),
                        'release_extension': safe_float(row_data.get('release_extension')),
                        'spin_axis': safe_float(row_data.get('spin_axis')),
                        'pfx_x': safe_float(row_data.get('pfx_x')),
                        'pfx_z': safe_float(row_data.get('pfx_z')),
                        'plate_x': safe_float(row_data.get('plate_x')),
                        'plate_z': safe_float(row_data.get('plate_z')),
                        'effective_speed': safe_float(row_data.get('effective_speed')),
                        'pitch_name': safe_str(row_data.get('pitch_name')), # Descriptive

                        # Zone and game context
                        'zone': safe_int(row_data.get('zone')),
                        'sz_top': safe_float(row_data.get('sz_top')),
                        'sz_bot': safe_float(row_data.get('sz_bot')),
                        'game_pk': safe_int(row_data.get('game_pk')),
                        'home_team': safe_str(row_data.get('home_team')),
                        'away_team': safe_str(row_data.get('away_team')),
                        'type': safe_str(row_data.get('type')), # S, B, X
                        'stand': safe_str(row_data.get('stand')),
                        'p_throws': safe_str(row_data.get('p_throws')),
                        'inning': safe_int(row_data.get('inning')),
                        'inning_topbot': safe_str(row_data.get('inning_topbot')),
                        'at_bat_number': safe_int(row_data.get('at_bat_number')),
                        'pitch_number': safe_int(row_data.get('pitch_number')),
                        'balls': safe_int(row_data.get('balls')),
                        'strikes': safe_int(row_data.get('strikes')),
                        'outs_when_up': safe_int(row_data.get('outs_when_up')),
                        
                        # Outcome
                        'description': safe_str(row_data.get('description')),
                        'events': safe_str(row_data.get('events')),
                        'des': safe_str(row_data.get('des')),
                        
                        # Hit data (often null for non-bip)
                        'bb_type': safe_str(row_data.get('bb_type')),
                        'hit_location': safe_int(row_data.get('hit_location')),
                        'hit_distance_sc': safe_float(row_data.get('hit_distance_sc')),
                        'launch_speed': safe_float(row_data.get('launch_speed')),
                        'launch_angle': safe_float(row_data.get('launch_angle')),

                        # Sword specific metrics from CSV
                        'bat_speed': safe_float(row_data.get('bat_speed')),
                        'swing_path_tilt': safe_float(row_data.get('swing_path_tilt')),
                        'attack_angle': safe_float(row_data.get('attack_angle')),
                        'intercept_ball_minus_batter_pos_y_inches': safe_float(row_data.get('intercept_ball_minus_batter_pos_y_inches')),
                        'swing_length': safe_float(row_data.get('swing_length')),
                        'intercept_ball_minus_batter_pos_x_inches': safe_float(row_data.get('intercept_ball_minus_batter_pos_x_inches')),
                        'attack_direction': safe_float(row_data.get('attack_direction')),
                        
                        # Add any other fields present in models_complete.StatcastPitch and your CSV
                        # This is a representative mapping, not exhaustive of all 118 fields.
                    }
                    # Filter out None values, as SQLAlchemy model might have defaults or constraints
                    pitch_data_args_cleaned = {k: v for k, v in pitch_data_args.items() if v is not None}
                    
                    pitch = StatcastPitch(**pitch_data_args_cleaned)
                    pitch_objects.append(pitch)
                
                db.add_all(pitch_objects)
                db.commit()
                logger.info(f"Committed chunk {current_chunk_num}/{total_chunks} ({len(pitch_objects)} records)")
            
            logger.info("Import completed successfully!")
            
            total_pitches = db.query(StatcastPitch).count()
            unique_games = db.query(StatcastPitch.game_pk).distinct().count()
            unique_dates = db.query(StatcastPitch.game_date).distinct().count()
            
            logger.info(f"Database summary:")
            logger.info(f"  Total pitches: {total_pitches:,}")
            logger.info(f"  Unique games: {unique_games:,}")
            logger.info(f"  Unique dates: {unique_dates:,}")
            
        except Exception as e:
            logger.error(f"Error during import processing chunk {current_chunk_num if 'current_chunk_num' in locals() else 'N/A'}: {e}")
            logger.error(traceback.format_exc())
            db.rollback()
            raise 

if __name__ == "__main__":
    # Ensure DATABASE_URL is set for standalone execution
    if not os.environ.get("DATABASE_URL"):
        logger.info("DATABASE_URL not set for standalone import_data.py, setting default.")
        os.environ["DATABASE_URL"] = "postgresql://swordfinder:swordfinder123@localhost:5432/swordfinder_db"
    import_statcast_data()
