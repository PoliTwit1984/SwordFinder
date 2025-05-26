"""
Fast bulk import for complete authentic MLB dataset
Uses PostgreSQL COPY for maximum speed
"""
import pandas as pd
import psycopg2
import os
from io import StringIO
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fast_import_complete_data():
    """
    Fast bulk import using PostgreSQL COPY command
    """
    logger.info("Starting fast bulk import of complete authentic MLB dataset")
    
    # Load the complete dataset
    logger.info("Loading complete_statcast_2025.csv...")
    df = pd.read_csv('complete_statcast_2025.csv', low_memory=False)
    logger.info(f"Loaded {len(df):,} authentic MLB records")
    
    # Connect directly to PostgreSQL
    conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
    cursor = conn.cursor()
    
    # Clear existing data
    logger.info("Clearing existing data...")
    cursor.execute("TRUNCATE TABLE statcast_pitches RESTART IDENTITY CASCADE")
    conn.commit()
    
    # Prepare data for bulk import
    logger.info("Preparing data for bulk import...")
    
    # Select and order columns to match database structure
    columns_to_import = [
        'pitch_type', 'game_date', 'release_speed', 'release_pos_x', 'release_pos_z',
        'player_name', 'batter', 'pitcher', 'events', 'description',
        'bat_speed', 'swing_length', 'swing_path_tilt', 
        'intercept_ball_minus_batter_pos_x_inches', 'intercept_ball_minus_batter_pos_y_inches',
        'attack_angle', 'attack_direction', 'sv_id', 'game_pk', 'inning',
        'at_bat_number', 'pitch_number', 'pitch_name', 'zone', 'balls', 'strikes',
        'plate_x', 'plate_z', 'launch_speed', 'launch_angle', 'effective_speed',
        'release_spin_rate', 'release_extension', 'pfx_x', 'pfx_z',
        'home_team', 'away_team', 'stand', 'p_throws', 'type', 'hit_location',
        'bb_type', 'game_year', 'sz_top', 'sz_bot'
    ]
    
    # Create subset with available columns
    available_columns = [col for col in columns_to_import if col in df.columns]
    df_subset = df[available_columns].copy()
    
    # Convert to string buffer for COPY
    logger.info("Converting to CSV format for bulk import...")
    output = StringIO()
    df_subset.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
    output.seek(0)
    
    # Build column list for COPY command
    db_columns = [
        'pitch_type', 'game_date', 'release_speed', 'release_pos_x', 'release_pos_z',
        'player_name', 'batter', 'pitcher', 'events', 'description',
        'bat_speed', 'swing_length', 'swing_path_tilt', 
        'intercept_ball_minus_batter_pos_x_inches', 'intercept_ball_minus_batter_pos_y_inches',
        'attack_angle', 'attack_direction', 'sv_id', 'game_pk', 'inning',
        'at_bat_number', 'pitch_number', 'pitch_name', 'zone', 'balls', 'strikes',
        'plate_x', 'plate_z', 'launch_speed', 'launch_angle', 'effective_speed',
        'release_spin_rate', 'release_extension', 'pfx_x', 'pfx_z',
        'home_team', 'away_team', 'stand', 'p_throws', 'type', 'hit_location',
        'bb_type', 'game_year', 'sz_top', 'sz_bot'
    ]
    
    # Filter to only columns that exist in both data and database
    final_columns = [col for col in db_columns if col in available_columns]
    
    logger.info(f"Importing {len(final_columns)} columns for {len(df_subset):,} records...")
    
    # Execute bulk COPY
    copy_sql = f"COPY statcast_pitches ({','.join(final_columns)}) FROM STDIN WITH CSV DELIMITER E'\\t' NULL '\\N'"
    cursor.copy_expert(copy_sql, output)
    
    # Commit the transaction
    conn.commit()
    
    # Verify import
    cursor.execute("SELECT COUNT(*) FROM statcast_pitches")
    total_count = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT COUNT(*) FROM statcast_pitches 
        WHERE description IN ('swinging_strike', 'swinging_strike_blocked')
        AND bat_speed IS NOT NULL
    """)
    sword_count = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    logger.info(f"✅ BULK IMPORT COMPLETE!")
    logger.info(f"✅ Total records imported: {total_count:,}")
    logger.info(f"✅ Complete sword swings: {sword_count:,}")
    
    return total_count, sword_count

if __name__ == "__main__":
    fast_import_complete_data()