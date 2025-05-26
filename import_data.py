#!/usr/bin/env python3
"""
Import all Statcast data from CSV into PostgreSQL database
"""
import pandas as pd
import logging
from sqlalchemy import create_engine
from models import StatcastPitch, create_tables, SessionLocal
import os
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def import_statcast_data(csv_path: str = 'attached_assets/statcast_2025.csv'):
    """
    Import all Statcast data from CSV file into database
    """
    logger.info(f"Starting import from {csv_path}")
    
    # Create tables if they don't exist
    create_tables()
    
    # Read CSV file
    logger.info("Loading CSV file...")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} records from CSV")
    
    # Get database session
    db = SessionLocal()
    
    try:
        # Clear existing data (optional - remove if you want to append)
        logger.info("Clearing existing data...")
        db.query(StatcastPitch).delete()
        db.commit()
        
        # Process data in chunks for better performance
        chunk_size = 1000
        total_chunks = len(df) // chunk_size + 1
        
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            logger.info(f"Processing chunk {i//chunk_size + 1}/{total_chunks} ({len(chunk)} records)")
            
            pitch_objects = []
            for _, row in chunk.iterrows():
                pitch = StatcastPitch(
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
                    
                    # Player info
                    batter=safe_int(row.get('batter')),
                    pitcher=safe_int(row.get('pitcher')),
                    batter_name=safe_str(row.get('batter_name')),
                    pitcher_name=safe_str(row.get('pitcher_name')),
                    
                    # Pitch data
                    pitch_type=safe_str(row.get('pitch_type')),
                    pitch_name=safe_str(row.get('pitch_name')),
                    release_speed=safe_float(row.get('release_speed')),
                    release_spin_rate=safe_float(row.get('release_spin_rate')),
                    release_extension=safe_float(row.get('release_extension')),
                    
                    # Location
                    plate_x=safe_float(row.get('plate_x')),
                    plate_z=safe_float(row.get('plate_z')),
                    sz_top=safe_float(row.get('sz_top')),
                    sz_bot=safe_float(row.get('sz_bot')),
                    
                    # Movement
                    pfx_x=safe_float(row.get('pfx_x')),
                    pfx_z=safe_float(row.get('pfx_z')),
                    effective_speed=safe_float(row.get('effective_speed')),
                    
                    # Swing data
                    bat_speed=safe_float(row.get('bat_speed')),
                    swing_path_tilt=safe_float(row.get('swing_path_tilt')),
                    attack_angle=safe_float(row.get('attack_angle')),
                    intercept_ball_minus_batter_pos_y_inches=safe_float(row.get('intercept_ball_minus_batter_pos_y_inches')),
                    
                    # Outcome
                    description=safe_str(row.get('description')),
                    events=safe_str(row.get('events')),
                    
                    # Video (will be populated later)
                    play_id=safe_str(row.get('play_id'))
                )
                pitch_objects.append(pitch)
            
            # Bulk insert
            db.add_all(pitch_objects)
            db.commit()
            
            logger.info(f"Inserted {len(pitch_objects)} records")
        
        logger.info("Import completed successfully!")
        
        # Print summary stats
        total_pitches = db.query(StatcastPitch).count()
        unique_games = db.query(StatcastPitch.game_pk).distinct().count()
        unique_dates = db.query(StatcastPitch.game_date).distinct().count()
        
        logger.info(f"Database summary:")
        logger.info(f"  Total pitches: {total_pitches:,}")
        logger.info(f"  Unique games: {unique_games:,}")
        logger.info(f"  Unique dates: {unique_dates:,}")
        
    except Exception as e:
        logger.error(f"Error during import: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def safe_int(value):
    """Safely convert to int"""
    try:
        if pd.isna(value):
            return None
        return int(float(value))
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

if __name__ == "__main__":
    import_statcast_data()