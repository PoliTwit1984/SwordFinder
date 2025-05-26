"""
Complete Database Update from CSV
Updates entire database with authentic MLB data from CSV file
"""
import pandas as pd
import psycopg2
import os
import time

def update_database_from_csv():
    """Update entire database with authentic MLB data from CSV"""
    
    print("🗡️ Starting complete database update with authentic MLB data...")
    start_time = time.time()
    
    # Connect to database
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    cursor = conn.cursor()
    
    # Load complete CSV data
    print("📊 Loading complete CSV file...")
    df = pd.read_csv('complete_statcast_2025.csv', low_memory=False)
    print(f"✅ Loaded {len(df)} records from CSV")
    
    # Update query for essential fields
    update_query = """
    UPDATE statcast_pitches 
    SET 
        home_team = COALESCE(home_team, %s),
        away_team = COALESCE(away_team, %s),
        release_speed = COALESCE(release_speed, %s),
        release_spin_rate = COALESCE(release_spin_rate, %s),
        pitch_name = COALESCE(pitch_name, %s),
        plate_x = COALESCE(plate_x, %s),
        plate_z = COALESCE(plate_z, %s),
        stand = COALESCE(stand, %s),
        p_throws = COALESCE(p_throws, %s)
    WHERE game_pk = %s AND game_date = %s
    """
    
    batch_size = 1000
    total_updated = 0
    
    print(f"🔄 Processing {len(df)} records in batches of {batch_size}...")
    
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        batch_updated = 0
        
        for _, row in batch.iterrows():
            try:
                # Prepare safe values
                def safe_val(val):
                    return None if pd.isna(val) or val == '' else val
                
                cursor.execute(update_query, (
                    safe_val(row.get('home_team')),
                    safe_val(row.get('away_team')),
                    safe_val(row.get('release_speed')),
                    safe_val(row.get('release_spin_rate')),
                    safe_val(row.get('pitch_name')),
                    safe_val(row.get('plate_x')),
                    safe_val(row.get('plate_z')),
                    safe_val(row.get('stand')),
                    safe_val(row.get('p_throws')),
                    int(row.get('game_pk')) if not pd.isna(row.get('game_pk')) else None,
                    safe_val(row.get('game_date'))
                ))
                batch_updated += cursor.rowcount
                
            except Exception as e:
                continue
        
        total_updated += batch_updated
        conn.commit()
        
        elapsed = time.time() - start_time
        rate = total_updated / elapsed if elapsed > 0 else 0
        
        print(f"✅ Batch {i//batch_size + 1}: Updated {batch_updated} records | Total: {total_updated} | Rate: {rate:.1f}/sec")
    
    cursor.close()
    conn.close()
    
    elapsed = time.time() - start_time
    print(f"🎉 Database update complete!")
    print(f"📈 Total records updated: {total_updated}")
    print(f"⏱️  Total time: {elapsed:.1f} seconds")
    print(f"🚀 Rate: {total_updated/elapsed:.1f} records/second")
    
    return total_updated

if __name__ == "__main__":
    update_database_from_csv()