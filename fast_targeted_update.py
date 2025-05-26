"""
Fast targeted database update - focuses on essential missing fields
Updates records that are missing critical data for sword swing analysis
"""
import pandas as pd
import psycopg2
import os
import time

def fast_targeted_update():
    """Update database focusing on records missing essential data"""
    
    print("🗡️ Starting fast targeted update for sword swing analysis...")
    start_time = time.time()
    
    # Connect to database
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    cursor = conn.cursor()
    
    # Find records missing essential data
    print("🔍 Finding records missing essential data...")
    cursor.execute("""
        SELECT game_pk, game_date, COUNT(*) 
        FROM statcast_pitches 
        WHERE home_team IS NULL OR release_speed IS NULL
        GROUP BY game_pk, game_date 
        ORDER BY game_date DESC
        LIMIT 20
    """)
    missing_games = cursor.fetchall()
    
    print(f"📊 Found {len(missing_games)} games needing updates")
    
    # Load CSV data
    print("📊 Loading CSV data...")
    df = pd.read_csv('complete_statcast_2025.csv', low_memory=False)
    
    total_updated = 0
    
    for game_pk, game_date, count in missing_games:
        print(f"🔄 Updating game {game_pk} on {game_date} ({count} pitches)...")
        
        # Get data for this specific game
        game_data = df[(df['game_pk'] == game_pk) & (df['game_date'] == game_date)]
        
        if len(game_data) == 0:
            print(f"⚠️  No CSV data found for game {game_pk}")
            continue
            
        game_updated = 0
        
        for _, row in game_data.iterrows():
            try:
                cursor.execute("""
                    UPDATE statcast_pitches 
                    SET 
                        home_team = COALESCE(home_team, %s),
                        away_team = COALESCE(away_team, %s),
                        release_speed = COALESCE(release_speed, %s),
                        release_spin_rate = COALESCE(release_spin_rate, %s),
                        pitch_name = COALESCE(pitch_name, %s)
                    WHERE game_pk = %s AND game_date = %s AND at_bat_number = %s AND pitch_number = %s
                """, (
                    str(row.get('home_team')) if pd.notna(row.get('home_team')) else None,
                    str(row.get('away_team')) if pd.notna(row.get('away_team')) else None,
                    float(row.get('release_speed')) if pd.notna(row.get('release_speed')) else None,
                    float(row.get('release_spin_rate')) if pd.notna(row.get('release_spin_rate')) else None,
                    str(row.get('pitch_name')) if pd.notna(row.get('pitch_name')) else None,
                    int(row.get('game_pk')),
                    str(row.get('game_date')),
                    int(row.get('at_bat_number')) if pd.notna(row.get('at_bat_number')) else None,
                    int(row.get('pitch_number')) if pd.notna(row.get('pitch_number')) else None
                ))
                game_updated += cursor.rowcount
                
            except Exception as e:
                continue
        
        conn.commit()
        total_updated += game_updated
        
        elapsed = time.time() - start_time
        print(f"✅ Game {game_pk}: Updated {game_updated} records | Total: {total_updated} | Time: {elapsed:.1f}s")
    
    cursor.close()
    conn.close()
    
    print(f"🎉 Fast update complete! Updated {total_updated} records")
    return total_updated

if __name__ == "__main__":
    fast_targeted_update()