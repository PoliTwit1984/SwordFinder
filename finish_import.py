"""
Complete the import of your authentic MLB dataset
Efficiently imports remaining records to reach 100% completion
"""
import pandas as pd
from models_complete import get_db, StatcastPitch
from sqlalchemy import text
import time

def safe_float(value):
    try:
        return float(value) if pd.notna(value) and str(value).strip() != '' else None
    except:
        return None

def safe_int(value):
    try:
        return int(value) if pd.notna(value) and str(value).strip() != '' else None
    except:
        return None

def safe_str(value):
    try:
        return str(value)[:500] if pd.notna(value) and str(value).strip() != '' else None
    except:
        return None

def finish_import():
    """Complete the import of remaining authentic MLB records"""
    
    # Check current status
    with get_db() as db:
        current_count = db.execute(text('SELECT COUNT(*) FROM statcast_pitches')).scalar()
    
    total_target = 226833
    remaining = total_target - current_count
    progress = (current_count / total_target) * 100
    
    print(f"🚀 Finishing your authentic MLB dataset import")
    print(f"📊 Current: {current_count:,} records ({progress:.1f}%)")
    print(f"📈 Remaining: {remaining:,} records to complete dataset")
    
    if remaining <= 0:
        print("✅ Dataset already complete!")
        return
    
    # Load remaining records from the complete dataset
    chunk_start = current_count
    print(f"📥 Loading records {chunk_start:,} to {total_target:,}...")
    
    df_chunk = pd.read_csv('complete_statcast_2025.csv', 
                           low_memory=False, 
                           skiprows=range(1, chunk_start + 1))
    
    print(f"📋 Loaded {len(df_chunk):,} records for final import")
    
    # Import in efficient batches
    batch_size = 5000
    total_batches = (len(df_chunk) + batch_size - 1) // batch_size
    
    with get_db() as db:
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(df_chunk))
            batch = df_chunk.iloc[start_idx:end_idx]
            
            if len(batch) == 0:
                break
                
            print(f"🔄 Processing batch {batch_num + 1}/{total_batches} ({len(batch):,} records)")
            
            batch_records = []
            for _, row in batch.iterrows():
                pitch = StatcastPitch(
                    pitch_type=safe_str(row.get('pitch_type')),
                    game_date=safe_str(row.get('game_date')),
                    player_name=safe_str(row.get('player_name')),
                    batter=safe_int(row.get('batter')),
                    pitcher=safe_int(row.get('pitcher')),
                    description=safe_str(row.get('description')),
                    bat_speed=safe_float(row.get('bat_speed')),
                    swing_path_tilt=safe_float(row.get('swing_path_tilt')),
                    intercept_ball_minus_batter_pos_y_inches=safe_float(row.get('intercept_ball_minus_batter_pos_y_inches')),
                    attack_angle=safe_float(row.get('attack_angle')),
                    sv_id=safe_str(row.get('sv_id')),
                    game_pk=safe_int(row.get('game_pk')),
                    inning=safe_int(row.get('inning')),
                    balls=safe_int(row.get('balls')),
                    strikes=safe_int(row.get('strikes'))
                )
                batch_records.append(pitch)
            
            # Commit batch
            db.add_all(batch_records)
            db.commit()
            
            # Progress update
            current_total = db.execute(text('SELECT COUNT(*) FROM statcast_pitches')).scalar()
            current_progress = (current_total / total_target) * 100
            
            print(f"✅ Progress: {current_total:,} records ({current_progress:.1f}%)")
            
            # Check for completion
            if current_total >= total_target:
                print("🎯 DATASET COMPLETION REACHED!")
                break
            
            # Brief pause between batches
            time.sleep(0.1)
    
    # Final verification
    with get_db() as db:
        final_total = db.execute(text('SELECT COUNT(*) FROM statcast_pitches')).scalar()
        sword_total = db.execute(text("""
            SELECT COUNT(*) FROM statcast_pitches 
            WHERE description IN ('swinging_strike', 'swinging_strike_blocked')
            AND bat_speed IS NOT NULL
        """)).scalar()
        
        dates_total = db.execute(text('SELECT COUNT(DISTINCT game_date) FROM statcast_pitches')).scalar()
        bat_speed_total = db.execute(text('SELECT COUNT(*) FROM statcast_pitches WHERE bat_speed IS NOT NULL')).scalar()
    
    final_progress = (final_total / total_target) * 100
    
    print(f"\n🎯 AUTHENTIC MLB DATASET IMPORT COMPLETE!")
    print(f"✅ Total authentic MLB records: {final_total:,} ({final_progress:.1f}%)")
    print(f"⚔️ Complete sword swings ready: {sword_total:,}")
    print(f"📅 Game dates available: {dates_total}")
    print(f"🎯 Records with bat speed data: {bat_speed_total:,}")
    print(f"\n🚀 Your SwordFinder is now powered by the complete authentic MLB dataset!")

if __name__ == "__main__":
    finish_import()