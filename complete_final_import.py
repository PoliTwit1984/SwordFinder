"""
Complete the final import of authentic MLB dataset
Process remaining records in small batches to avoid timeouts
"""
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from datetime import datetime

def complete_final_import():
    """Complete the import with smaller batch sizes"""
    
    # Database connection
    DATABASE_URL = os.environ.get('DATABASE_URL')
    engine = create_engine(DATABASE_URL)
    
    # Check current count
    with engine.connect() as conn:
        from sqlalchemy import text
        result = conn.execute(text("SELECT COUNT(*) FROM statcast_pitches"))
        current_count = result.scalar()
    
    print(f"🚀 Completing authentic MLB dataset import")
    print(f"📊 Current: {current_count:,} records")
    
    # Load remaining data
    print(f"📥 Loading remaining records from {current_count:,}...")
    
    try:
        # Read CSV in chunks to handle memory efficiently
        csv_path = 'complete_statcast_2025.csv'
        chunk_size = 2000  # Smaller chunks for stability
        
        # Skip already imported records
        df_iterator = pd.read_csv(csv_path, chunksize=chunk_size, skiprows=range(1, current_count + 1))
        
        batch_num = 1
        total_added = 0
        
        for chunk_df in df_iterator:
            if len(chunk_df) == 0:
                break
                
            print(f"🔄 Processing batch {batch_num} ({len(chunk_df):,} records)")
            
            # Process this chunk
            processed_records = []
            
            for _, row in chunk_df.iterrows():
                try:
                    record = {
                        'pitch_type': str(row.get('pitch_type', '')) if pd.notna(row.get('pitch_type')) else None,
                        'game_date': str(row.get('game_date', '')) if pd.notna(row.get('game_date')) else None,
                        'release_speed': float(row.get('release_speed')) if pd.notna(row.get('release_speed')) else None,
                        'player_name': str(row.get('player_name', '')) if pd.notna(row.get('player_name')) else None,
                        'batter': int(row.get('batter')) if pd.notna(row.get('batter')) else None,
                        'pitcher': int(row.get('pitcher')) if pd.notna(row.get('pitcher')) else None,
                        'events': str(row.get('events', '')) if pd.notna(row.get('events')) else None,
                        'description': str(row.get('description', '')) if pd.notna(row.get('description')) else None,
                        'game_pk': int(row.get('game_pk')) if pd.notna(row.get('game_pk')) else None,
                        'sv_id': str(row.get('sv_id', '')) if pd.notna(row.get('sv_id')) else None,
                        'bat_speed': float(row.get('bat_speed')) if pd.notna(row.get('bat_speed')) else None,
                        'attack_angle': float(row.get('attack_angle')) if pd.notna(row.get('attack_angle')) else None,
                        'swing_path_tilt': float(row.get('swing_path_tilt')) if pd.notna(row.get('swing_path_tilt')) else None,
                        'intercept_ball_minus_batter_pos_y_inches': float(row.get('intercept_ball_minus_batter_pos_y_inches')) if pd.notna(row.get('intercept_ball_minus_batter_pos_y_inches')) else None,
                        'zone': int(row.get('zone')) if pd.notna(row.get('zone')) else None,
                        'inning': int(row.get('inning')) if pd.notna(row.get('inning')) else None,
                        'at_bat_number': int(row.get('at_bat_number')) if pd.notna(row.get('at_bat_number')) else None,
                        'pitch_number': int(row.get('pitch_number')) if pd.notna(row.get('pitch_number')) else None,
                    }
                    
                    processed_records.append(record)
                    
                except Exception as e:
                    print(f"⚠️  Skipping problematic record: {e}")
                    continue
            
            # Insert batch
            if processed_records:
                try:
                    df_batch = pd.DataFrame(processed_records)
                    df_batch.to_sql('statcast_pitches', engine, if_exists='append', index=False, method='multi')
                    
                    total_added += len(processed_records)
                    new_total = current_count + total_added
                    percentage = (new_total / 226833) * 100
                    
                    print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Progress: {new_total:,} records ({percentage:.1f}%) - {226833 - new_total:,} remaining")
                    
                except Exception as e:
                    print(f"❌ Error inserting batch {batch_num}: {e}")
                    continue
            
            batch_num += 1
            
            # Stop if we've completed the dataset
            if current_count + total_added >= 226833:
                print(f"🎉 DATASET COMPLETE! {current_count + total_added:,} authentic MLB records imported!")
                break
                
    except Exception as e:
        print(f"❌ Import error: {e}")
        print(f"📊 Progress saved: {current_count + total_added:,} records")
    
    # Final count
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM statcast_pitches"))
        final_count = result.scalar()
    
    print(f"🏁 Final result: {final_count:,} authentic MLB records in database")
    
    if final_count >= 226833:
        print("🎉 CONGRATULATIONS! Your complete authentic MLB dataset is ready!")
    else:
        remaining = 226833 - final_count
        print(f"📈 Progress made! {remaining:,} records remaining to complete dataset")

if __name__ == "__main__":
    complete_final_import()