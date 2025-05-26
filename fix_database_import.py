#!/usr/bin/env python3
"""
Fix incomplete database import by properly mapping CSV fields to database
Focus on critical missing fields: teams, velocity, locations, spin rates
"""
import pandas as pd
import os
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_database_import():
    """
    Update database with missing fields from CSV in efficient batches
    """
    database_url = os.environ.get('DATABASE_URL')
    engine = create_engine(database_url)
    
    logger.info("Loading CSV data...")
    # Read CSV with proper data types
    df = pd.read_csv('complete_statcast_2025.csv', low_memory=False)
    logger.info(f"Loaded {len(df)} records from CSV")
    
    # Focus on records that have the critical missing data
    logger.info("Filtering for records with complete critical data...")
    
    with engine.connect() as conn:
        batch_size = 1000
        updated_count = 0
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: records {i} to {min(i+batch_size, len(df))}")
            
            for _, row in batch.iterrows():
                try:
                    # Build update for missing fields
                    update_fields = []
                    params = {
                        'game_date': str(row['game_date']),
                        'player_name': str(row['player_name']) if pd.notna(row['player_name']) else None,
                        'pitch_type': str(row['pitch_type']) if pd.notna(row['pitch_type']) else None,
                        'game_pk': int(row['game_pk']) if pd.notna(row['game_pk']) else None
                    }
                    
                    # Add fields that are missing in database
                    if pd.notna(row['release_speed']):
                        update_fields.append("release_speed = :release_speed")
                        params['release_speed'] = float(row['release_speed'])
                    
                    if pd.notna(row['home_team']):
                        update_fields.append("home_team = :home_team")
                        params['home_team'] = str(row['home_team'])
                    
                    if pd.notna(row['away_team']):
                        update_fields.append("away_team = :away_team")
                        params['away_team'] = str(row['away_team'])
                    
                    if pd.notna(row['plate_x']):
                        update_fields.append("plate_x = :plate_x")
                        params['plate_x'] = float(row['plate_x'])
                    
                    if pd.notna(row['plate_z']):
                        update_fields.append("plate_z = :plate_z")
                        params['plate_z'] = float(row['plate_z'])
                    
                    if pd.notna(row['release_spin_rate']):
                        update_fields.append("release_spin_rate = :release_spin_rate")
                        params['release_spin_rate'] = float(row['release_spin_rate'])
                    
                    if pd.notna(row['pitch_name']):
                        update_fields.append("pitch_name = :pitch_name")
                        params['pitch_name'] = str(row['pitch_name'])
                    
                    if pd.notna(row['sv_id']):
                        update_fields.append("sv_id = :sv_id")
                        params['sv_id'] = str(row['sv_id'])
                    
                    if pd.notna(row['at_bat_number']):
                        update_fields.append("at_bat_number = :at_bat_number")
                        params['at_bat_number'] = int(row['at_bat_number'])
                    
                    if pd.notna(row['pitch_number']):
                        update_fields.append("pitch_number = :pitch_number")
                        params['pitch_number'] = int(row['pitch_number'])
                    
                    # Only update if we have fields to update
                    if update_fields:
                        update_query = text(f"""
                            UPDATE statcast_pitches 
                            SET {', '.join(update_fields)}
                            WHERE game_date = :game_date 
                            AND game_pk = :game_pk
                            AND player_name = :player_name
                            AND pitch_type = :pitch_type
                        """)
                        
                        result = conn.execute(update_query, params)
                        if result.rowcount > 0:
                            updated_count += result.rowcount
                
                except Exception as e:
                    logger.warning(f"Error updating record: {e}")
                    continue
            
            # Commit after each batch
            conn.commit()
            logger.info(f"Updated {updated_count} records so far...")
        
        logger.info(f"Import fix complete! Updated {updated_count} total records with missing data")

if __name__ == "__main__":
    fix_database_import()