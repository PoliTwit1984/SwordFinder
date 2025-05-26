"""
Chunked import for complete authentic MLB dataset
Imports data in manageable chunks to avoid timeouts
"""
import pandas as pd
from models_complete import get_db, StatcastPitch
from sqlalchemy import text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_float(value):
    try:
        return float(value) if pd.notna(value) and value != '' else None
    except:
        return None

def safe_int(value):
    try:
        return int(value) if pd.notna(value) and value != '' else None
    except:
        return None

def safe_str(value):
    try:
        return str(value)[:500] if pd.notna(value) and value != '' else None
    except:
        return None

def import_chunk(start_idx, chunk_size=10000):
    """Import a specific chunk of data"""
    logger.info(f"Loading CSV chunk starting at index {start_idx:,}")
    
    # Load only the chunk we need
    df = pd.read_csv('complete_statcast_2025.csv', 
                     low_memory=False, 
                     skiprows=range(1, start_idx + 1),  # Skip rows before our chunk
                     nrows=chunk_size)
    
    if len(df) == 0:
        return 0  # No more data
    
    logger.info(f"Processing {len(df):,} records...")
    
    with get_db() as db:
        batch_records = []
        for _, row in df.iterrows():
            pitch = StatcastPitch(
                pitch_type=safe_str(row.get('pitch_type')),
                game_date=safe_str(row.get('game_date')),
                release_speed=safe_float(row.get('release_speed')),
                release_pos_x=safe_float(row.get('release_pos_x')),
                release_pos_z=safe_float(row.get('release_pos_z')),
                player_name=safe_str(row.get('player_name')),
                batter=safe_int(row.get('batter')),
                pitcher=safe_int(row.get('pitcher')),
                events=safe_str(row.get('events')),
                description=safe_str(row.get('description')),
                bat_speed=safe_float(row.get('bat_speed')),
                swing_length=safe_float(row.get('swing_length')),
                swing_path_tilt=safe_float(row.get('swing_path_tilt')),
                intercept_ball_minus_batter_pos_y_inches=safe_float(row.get('intercept_ball_minus_batter_pos_y_inches')),
                attack_angle=safe_float(row.get('attack_angle')),
                sv_id=safe_str(row.get('sv_id')),
                game_pk=safe_int(row.get('game_pk')),
                inning=safe_int(row.get('inning')),
                at_bat_number=safe_int(row.get('at_bat_number')),
                pitch_number=safe_int(row.get('pitch_number')),
                pitch_name=safe_str(row.get('pitch_name')),
                zone=safe_int(row.get('zone')),
                balls=safe_int(row.get('balls')),
                strikes=safe_int(row.get('strikes')),
                plate_x=safe_float(row.get('plate_x')),
                plate_z=safe_float(row.get('plate_z')),
                launch_speed=safe_float(row.get('launch_speed')),
                launch_angle=safe_float(row.get('launch_angle')),
                effective_speed=safe_float(row.get('effective_speed')),
                release_spin_rate=safe_float(row.get('release_spin_rate')),
                pfx_x=safe_float(row.get('pfx_x')),
                pfx_z=safe_float(row.get('pfx_z')),
                home_team=safe_str(row.get('home_team')),
                away_team=safe_str(row.get('away_team'))
            )
            batch_records.append(pitch)
        
        db.add_all(batch_records)
        db.commit()
        
        # Get current totals
        total_count = db.execute(text('SELECT COUNT(*) FROM statcast_pitches')).scalar()
        sword_count = db.execute(text("""
            SELECT COUNT(*) FROM statcast_pitches 
            WHERE description IN ('swinging_strike', 'swinging_strike_blocked')
            AND bat_speed IS NOT NULL
        """)).scalar()
        
        logger.info(f"Chunk complete. Total records: {total_count:,}, Sword swings: {sword_count:,}")
        return len(batch_records)

def run_chunked_import():
    """Run the complete chunked import"""
    logger.info("Starting chunked import of complete authentic MLB dataset")
    
    # Get current progress
    with get_db() as db:
        current_count = db.execute(text('SELECT COUNT(*) FROM statcast_pitches')).scalar()
    
    logger.info(f"Current database records: {current_count:,}")
    
    # Load CSV to get total size
    logger.info("Checking dataset size...")
    total_rows = sum(1 for line in open('complete_statcast_2025.csv')) - 1  # Subtract header
    logger.info(f"Total authentic MLB records to import: {total_rows:,}")
    
    chunk_size = 10000
    start_idx = current_count
    
    while start_idx < total_rows:
        imported = import_chunk(start_idx, chunk_size)
        if imported == 0:
            break
        start_idx += imported
        
        # Progress update
        progress = (start_idx / total_rows) * 100
        logger.info(f"Progress: {progress:.1f}% ({start_idx:,}/{total_rows:,})")
    
    # Final verification
    with get_db() as db:
        final_total = db.execute(text('SELECT COUNT(*) FROM statcast_pitches')).scalar()
        final_sword_count = db.execute(text("""
            SELECT COUNT(*) FROM statcast_pitches 
            WHERE description IN ('swinging_strike', 'swinging_strike_blocked')
            AND bat_speed IS NOT NULL
        """)).scalar()
        
        logger.info(f"✅ IMPORT COMPLETE!")
        logger.info(f"✅ Total records: {final_total:,}")
        logger.info(f"✅ Complete sword swings: {final_sword_count:,}")

if __name__ == "__main__":
    run_chunked_import()