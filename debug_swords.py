import os
from sqlalchemy import create_engine, text

db_url = os.environ.get('DATABASE_URL')
engine = create_engine(db_url)

with engine.connect() as conn:
    print("=== DEBUGGING SWORD SWINGS FOR 2025-05-24 ===\n")
    
    # 1. Check if we have any data for this date
    result = conn.execute(text("""
        SELECT COUNT(*) 
        FROM statcast_pitches 
        WHERE game_date = '2025-05-24'
    """))
    print(f"1. Total pitches on 2025-05-24: {result.scalar()}")
    
    # 2. Check strikeouts
    result = conn.execute(text("""
        SELECT COUNT(*) 
        FROM statcast_pitches 
        WHERE game_date = '2025-05-24'
        AND events = 'strikeout'
    """))
    print(f"2. Pitches with strikeout event: {result.scalar()}")
    
    # 3. Check strikeout at-bats (distinct)
    result = conn.execute(text("""
        SELECT COUNT(DISTINCT CONCAT(game_pk, '-', at_bat_number))
        FROM statcast_pitches 
        WHERE game_date = '2025-05-24'
        AND events = 'strikeout'
    """))
    print(f"3. Unique strikeout at-bats: {result.scalar()}")
    
    # 4. Check swinging strikes
    result = conn.execute(text("""
        SELECT COUNT(*) 
        FROM statcast_pitches 
        WHERE game_date = '2025-05-24'
        AND description IN ('swinging_strike', 'swinging_strike_blocked')
    """))
    print(f"4. Total swinging strikes: {result.scalar()}")
    
    # 5. Check swinging strikes with bat speed
    result = conn.execute(text("""
        SELECT COUNT(*) 
        FROM statcast_pitches 
        WHERE game_date = '2025-05-24'
        AND description IN ('swinging_strike', 'swinging_strike_blocked')
        AND bat_speed IS NOT NULL
    """))
    print(f"5. Swinging strikes with bat speed data: {result.scalar()}")
    
    # 6. Try the CTE query manually
    result = conn.execute(text("""
        WITH strikeout_at_bats AS (
            SELECT DISTINCT game_pk, at_bat_number
            FROM statcast_pitches
            WHERE game_date = '2025-05-24'
            AND events = 'strikeout'
        )
        SELECT COUNT(*)
        FROM statcast_pitches sp
        JOIN strikeout_at_bats sa 
            ON sp.game_pk = sa.game_pk 
            AND sp.at_bat_number = sa.at_bat_number
        WHERE sp.game_date = '2025-05-24'
    """))
    print(f"6. Total pitches in strikeout at-bats: {result.scalar()}")
    
    # 7. Check swinging strikes in strikeout at-bats
    result = conn.execute(text("""
        WITH strikeout_at_bats AS (
            SELECT DISTINCT game_pk, at_bat_number
            FROM statcast_pitches
            WHERE game_date = '2025-05-24'
            AND events = 'strikeout'
        )
        SELECT COUNT(*)
        FROM statcast_pitches sp
        JOIN strikeout_at_bats sa 
            ON sp.game_pk = sa.game_pk 
            AND sp.at_bat_number = sa.at_bat_number
        WHERE sp.game_date = '2025-05-24'
        AND sp.description IN ('swinging_strike', 'swinging_strike_blocked')
    """))
    print(f"7. Swinging strikes in strikeout at-bats: {result.scalar()}")
    
    # 8. Check with bat speed data
    result = conn.execute(text("""
        WITH strikeout_at_bats AS (
            SELECT DISTINCT game_pk, at_bat_number
            FROM statcast_pitches
            WHERE game_date = '2025-05-24'
            AND events = 'strikeout'
        )
        SELECT COUNT(*)
        FROM statcast_pitches sp
        JOIN strikeout_at_bats sa 
            ON sp.game_pk = sa.game_pk 
            AND sp.at_bat_number = sa.at_bat_number
        WHERE sp.game_date = '2025-05-24'
        AND sp.description IN ('swinging_strike', 'swinging_strike_blocked')
        AND sp.bat_speed IS NOT NULL
    """))
    print(f"8. Swinging strikes in strikeout at-bats WITH bat speed: {result.scalar()}")
    
    # 9. Let's see what we actually have
    print("\n=== SAMPLE DATA ===")
    result = conn.execute(text("""
        WITH strikeout_at_bats AS (
            SELECT DISTINCT game_pk, at_bat_number
            FROM statcast_pitches
            WHERE game_date = '2025-05-24'
            AND events = 'strikeout'
        )
        SELECT sp.player_name, sp.bat_speed, sp.swing_path_tilt, 
               sp.intercept_ball_minus_batter_pos_y_inches,
               sp.game_pk, sp.at_bat_number, sp.description
        FROM statcast_pitches sp
        JOIN strikeout_at_bats sa 
            ON sp.game_pk = sa.game_pk 
            AND sp.at_bat_number = sa.at_bat_number
        WHERE sp.game_date = '2025-05-24'
        AND sp.description IN ('swinging_strike', 'swinging_strike_blocked')
        AND sp.bat_speed IS NOT NULL
        LIMIT 10
    """))
    
    print("\nSwinging strikes in strikeout at-bats with bat speed:")
    for row in result:
        print(f"  {row[0]}: bat_speed={row[1]}, tilt={row[2]}, intercept={row[3]}")
        print(f"    game_pk={row[4]}, at_bat={row[5]}, desc={row[6]}")
