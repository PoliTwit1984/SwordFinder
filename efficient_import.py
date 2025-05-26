"""
Ultra-efficient import using direct SQL COPY for maximum speed
"""
import pandas as pd
import psycopg2
import os
from io import StringIO
import logging

logging.basicConfig(level=logging.INFO)

def ultra_fast_import():
    """Import using PostgreSQL COPY - fastest possible method"""
    print("🚀 Starting ultra-fast import of complete authentic MLB dataset")
    
    # Connect directly to PostgreSQL
    conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
    cursor = conn.cursor()
    
    # Clear existing data first
    print("Clearing existing data...")
    cursor.execute("TRUNCATE TABLE statcast_pitches RESTART IDENTITY CASCADE")
    conn.commit()
    
    # Load and prepare data
    print("Loading 226,833 authentic MLB records...")
    df = pd.read_csv('complete_statcast_2025.csv', low_memory=False)
    print(f"Loaded {len(df):,} authentic records")
    
    # Select key columns for sword swing analysis
    key_columns = [
        'pitch_type', 'game_date', 'player_name', 'batter', 'pitcher', 
        'description', 'bat_speed', 'swing_path_tilt', 
        'intercept_ball_minus_batter_pos_y_inches', 'attack_angle',
        'sv_id', 'game_pk', 'inning', 'balls', 'strikes',
        'plate_x', 'plate_z', 'launch_speed', 'launch_angle'
    ]
    
    # Filter to only available columns
    available_cols = [col for col in key_columns if col in df.columns]
    df_subset = df[available_cols]
    
    print(f"Preparing {len(available_cols)} columns for COPY...")
    
    # Convert to CSV string for COPY
    output = StringIO()
    df_subset.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
    output.seek(0)
    
    # Execute COPY command
    copy_sql = f"COPY statcast_pitches ({','.join(available_cols)}) FROM STDIN WITH CSV DELIMITER E'\\t' NULL '\\N'"
    print("Executing bulk COPY...")
    cursor.copy_expert(copy_sql, output)
    
    # Commit transaction
    conn.commit()
    
    # Verify results
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
    
    print(f"✅ ULTRA-FAST IMPORT COMPLETE!")
    print(f"✅ Total authentic MLB records: {total_count:,}")
    print(f"✅ Complete sword swings ready: {sword_count:,}")
    
    return total_count, sword_count

if __name__ == "__main__":
    ultra_fast_import()