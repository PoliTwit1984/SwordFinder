"""
Efficient batch update - processes one date at a time for reliable progress
"""
import pandas as pd
import psycopg2
import os
import sys

def update_single_date(target_date):
    """Update a single date with authentic MLB data"""
    
    print(f"🗡️ Updating {target_date} with authentic MLB data...")
    
    # Connect to database
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    cursor = conn.cursor()
    
    # Load CSV data for this date
    df = pd.read_csv('complete_statcast_2025.csv', low_memory=False)
    date_data = df[df['game_date'] == target_date]
    
    if len(date_data) == 0:
        print(f"❌ No CSV data found for {target_date}")
        return 0
    
    print(f"📊 Found {len(date_data)} records for {target_date}")
    
    updated = 0
    batch_size = 100
    
    for i in range(0, len(date_data), batch_size):
        batch = date_data.iloc[i:i+batch_size]
        batch_updated = 0
        
        for _, row in batch.iterrows():
            try:
                cursor.execute("""
                    UPDATE statcast_pitches 
                    SET 
                        home_team = COALESCE(home_team, %s),
                        away_team = COALESCE(away_team, %s),
                        release_speed = COALESCE(release_speed, %s),
                        release_spin_rate = COALESCE(release_spin_rate, %s),
                        pitch_name = COALESCE(pitch_name, %s)
                    WHERE game_pk = %s AND at_bat_number = %s AND pitch_number = %s
                """, (
                    str(row.get('home_team')) if pd.notna(row.get('home_team')) else None,
                    str(row.get('away_team')) if pd.notna(row.get('away_team')) else None,
                    float(row.get('release_speed')) if pd.notna(row.get('release_speed')) else None,
                    float(row.get('release_spin_rate')) if pd.notna(row.get('release_spin_rate')) else None,
                    str(row.get('pitch_name')) if pd.notna(row.get('pitch_name')) else None,
                    int(row.get('game_pk')) if pd.notna(row.get('game_pk')) else None,
                    int(row.get('at_bat_number')) if pd.notna(row.get('at_bat_number')) else None,
                    int(row.get('pitch_number')) if pd.notna(row.get('pitch_number')) else None
                ))
                batch_updated += cursor.rowcount
            except Exception:
                continue
        
        updated += batch_updated
        conn.commit()
        print(f"  Batch {i//batch_size + 1}: Updated {batch_updated} | Total: {updated}")
    
    cursor.close()
    conn.close()
    
    print(f"✅ {target_date} complete! Updated {updated} records")
    return updated

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2025-03-31"
    update_single_date(target_date)