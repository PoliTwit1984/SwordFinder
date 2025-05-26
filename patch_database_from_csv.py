"""
Patch database with complete authentic MLB data from CSV file
Updates all missing fields across entire database using local CSV data
"""
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import time
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_float(value):
    """Safely convert to float"""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return float(value)
    except:
        return None

def safe_int(value):
    """Safely convert to int"""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return int(value)
    except:
        return None

def safe_str(value):
    """Safely convert to string"""
    if pd.isna(value) or value == '' or value is None:
        return None
    return str(value)

def patch_database_from_csv():
    """
    Update database with missing fields from complete CSV file
    """
    start_time = time.time()
    
    try:
        # Connect to database
        database_url = os.environ.get('DATABASE_URL')
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        logger.info("Loading CSV file...")
        df = pd.read_csv('complete_statcast_2025.csv')
        logger.info(f"Loaded {len(df)} records from CSV")
        
        # Prepare update query
        update_query = """
        UPDATE statcast_pitches 
        SET 
            home_team = COALESCE(home_team, %s),
            away_team = COALESCE(away_team, %s),
            release_speed = COALESCE(release_speed, %s),
            release_spin_rate = COALESCE(release_spin_rate, %s),
            spin_axis = COALESCE(spin_axis, %s),
            plate_x = COALESCE(plate_x, %s),
            plate_z = COALESCE(plate_z, %s),
            pitch_name = COALESCE(pitch_name, %s),
            pitch_type = COALESCE(pitch_type, %s),
            stand = COALESCE(stand, %s),
            p_throws = COALESCE(p_throws, %s),
            sz_top = COALESCE(sz_top, %s),
            sz_bot = COALESCE(sz_bot, %s),
            bat_speed = COALESCE(bat_speed, %s),
            swing_path_tilt = COALESCE(swing_path_tilt, %s),
            intercept_ball_minus_batter_pos_y_inches = COALESCE(intercept_ball_minus_batter_pos_y_inches, %s),
            player_name = COALESCE(player_name, %s),
            batter = COALESCE(batter, %s),
            pitcher = COALESCE(pitcher, %s),
            pfx_x = COALESCE(pfx_x, %s),
            pfx_z = COALESCE(pfx_z, %s),
            effective_speed = COALESCE(effective_speed, %s),
            release_extension = COALESCE(release_extension, %s),
            attack_angle = COALESCE(attack_angle, %s),
            swing_length = COALESCE(swing_length, %s)
        WHERE game_pk = %s 
        AND game_date = %s
        AND ((batter = %s AND pitcher = %s) OR (player_name = %s AND pitch_type = %s))
        """
        
        batch_size = 500
        total_updated = 0
        processed = 0
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            batch_data = []
            
            for _, row in batch.iterrows():
                # Prepare data for update
                update_data = (
                    safe_str(row.get('home_team')),
                    safe_str(row.get('away_team')),
                    safe_float(row.get('release_speed')),
                    safe_float(row.get('release_spin_rate')),
                    safe_float(row.get('spin_axis')),
                    safe_float(row.get('plate_x')),
                    safe_float(row.get('plate_z')),
                    safe_str(row.get('pitch_name')),
                    safe_str(row.get('pitch_type')),
                    safe_str(row.get('stand')),
                    safe_str(row.get('p_throws')),
                    safe_float(row.get('sz_top')),
                    safe_float(row.get('sz_bot')),
                    safe_float(row.get('bat_speed')),
                    safe_float(row.get('swing_path_tilt')),
                    safe_float(row.get('intercept_ball_minus_batter_pos_y_inches')),
                    safe_str(row.get('player_name')),
                    safe_int(row.get('batter')),
                    safe_int(row.get('pitcher')),
                    safe_float(row.get('pfx_x')),
                    safe_float(row.get('pfx_z')),
                    safe_float(row.get('effective_speed')),
                    safe_float(row.get('release_extension')),
                    safe_float(row.get('attack_angle')),
                    safe_float(row.get('swing_length')),
                    # WHERE conditions
                    safe_int(row.get('game_pk')),
                    safe_str(row.get('game_date')),
                    safe_int(row.get('batter')),
                    safe_int(row.get('pitcher')),
                    safe_str(row.get('player_name')),
                    safe_str(row.get('pitch_type'))
                )
                batch_data.append(update_data)
            
            # Execute batch update
            cursor.executemany(update_query, batch_data)
            batch_updated = cursor.rowcount
            total_updated += batch_updated
            processed += len(batch)
            
            conn.commit()
            
            elapsed = time.time() - start_time
            logger.info(f"Batch {i//batch_size + 1} complete. Processed {processed}/{len(df)} records. Updated {total_updated} total records. Elapsed: {elapsed:.1f}s")
        
        cursor.close()
        conn.close()
        
        elapsed = time.time() - start_time
        logger.info(f"CSV patch completed! Updated {total_updated} records in {elapsed:.1f} seconds")
        
        return {
            "success": True,
            "total_processed": len(df),
            "total_updated": total_updated,
            "elapsed_time": elapsed
        }
        
    except Exception as e:
        logger.error(f"Error during CSV patch: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

if __name__ == "__main__":
    result = patch_database_from_csv()
    print(f"Final result: {result}")