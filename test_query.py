import os
from sqlalchemy import create_engine, text
import logging

# Configure logging for this script
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_url = os.environ.get('DATABASE_URL')
logger.info(f"test_query.py: DATABASE_URL = {db_url}")
if not db_url:
    logger.error("DATABASE_URL environment variable not set or empty in test_query.py!")
    # Attempt to use a default if not set, for standalone execution
    db_url = "postgresql://swordfinder:swordfinder123@localhost:5432/swordfinder_db"
    logger.info(f"test_query.py: Using default DATABASE_URL = {db_url}")


engine = create_engine(db_url)

with engine.connect() as conn:
    # First, let's see what dates have data
    result = conn.execute(text("""
        SELECT game_date, COUNT(*) as count
        FROM statcast_pitches
        WHERE game_date >= '2025-05-20' AND game_date <= '2025-05-25'
        GROUP BY game_date
        ORDER BY game_date
    """))
    print("Dates with data:")
    for row in result:
        print(f"  {row[0]}: {row[1]} pitches")
    
    # Check 5/24 specifically
    print("\n=== May 24, 2025 Analysis ===")
    
    # Run diagnostic queries
    # 1. Total pitches on that date
    result = conn.execute(text("""
        SELECT COUNT(*) FROM statcast_pitches WHERE game_date = '2025-05-24'
    """))
    print(f"\n1. Total pitches on 2025-05-24: {result.scalar()}")
    
    # 2. How many swinging_strikes
    result = conn.execute(text("""
        SELECT COUNT(*) FROM statcast_pitches 
        WHERE game_date = '2025-05-24' 
        AND description IN ('swinging_strike', 'swinging_strike_blocked')
    """))
    print(f"2. Swinging strikes: {result.scalar()}")
    
    # 3. How many have bat speed + tilt + intercept
    result = conn.execute(text("""
        SELECT COUNT(*) FROM statcast_pitches 
        WHERE game_date = '2025-05-24' 
        AND description IN ('swinging_strike', 'swinging_strike_blocked')
        AND bat_speed IS NOT NULL
        AND swing_path_tilt IS NOT NULL
        AND intercept_ball_minus_batter_pos_y_inches IS NOT NULL
    """))
    print(f"3. With bat speed + tilt + intercept: {result.scalar()}")
    
    # 4. How many are in strikeout at-bats
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
        AND sp.swing_path_tilt IS NOT NULL
        AND sp.intercept_ball_minus_batter_pos_y_inches IS NOT NULL
    """))
    print(f"4. In strikeout at-bats with all data: {result.scalar()}")
    
    # 5. Check a sample without filters
    result = conn.execute(text("""
        SELECT player_name, bat_speed, swing_path_tilt, 
               intercept_ball_minus_batter_pos_y_inches, events
        FROM statcast_pitches
        WHERE game_date = '2025-05-24'
        AND description IN ('swinging_strike', 'swinging_strike_blocked')
        AND bat_speed IS NOT NULL
        ORDER BY bat_speed ASC
        LIMIT 5
    """))
    
    print("\n5. Lowest bat speed swinging strikes (any at-bat):")
    for row in result:
        print(f"  Batter: {row[0]}") # This is player_name (pitcher) in this context
        print(f"    Bat speed: {row[1]}, Tilt: {row[2]}, Intercept: {row[3]}")
        print(f"    Event outcome: {row[4]}")

    # Fetch specific pitch for Logan Davidson (game_pk=777784, at_bat_number=103, pitch_number=2)
    print("\n=== Logan Davidson Pitch Analysis (Old Top Sword Candidate) ===")
    davidson_pitch_query = text("""
        SELECT game_pk, at_bat_number, pitch_number, player_name, batter, description, events,
               bat_speed, swing_path_tilt, intercept_ball_minus_batter_pos_y_inches,
               plate_x, plate_z, sz_top, sz_bot, sv_id
        FROM statcast_pitches
        WHERE game_pk = :game_pk AND at_bat_number = :at_bat_number AND pitch_number = :pitch_number
    """)
    davidson_result = conn.execute(davidson_pitch_query, {
        "game_pk": 777784,
        "at_bat_number": 103,
        "pitch_number": 2
    })
    davidson_row = davidson_result.fetchone()

    if davidson_row:
        print("Logan Davidson's Pitch Data (game_pk=777784, at_bat=103, pitch=2):")
        print(f"  Game PK: {davidson_row[0]}, At Bat: {davidson_row[1]}, Pitch #: {davidson_row[2]}")
        print(f"  Pitcher Name (player_name): {davidson_row[3]}") # player_name is pitcher
        print(f"  Batter ID: {davidson_row[4]}")
        print(f"  Description: {davidson_row[5]}")
        print(f"  Events: {davidson_row[6]}")
        print(f"  Bat Speed: {davidson_row[7]}")
        print(f"  Swing Path Tilt: {davidson_row[8]}")
        print(f"  Intercept Y: {davidson_row[9]}")
        print(f"  Plate X: {davidson_row[10]}")
        print(f"  Plate Z: {davidson_row[11]}")
        print(f"  SZ Top: {davidson_row[12]}")
        print(f"  SZ Bot: {davidson_row[13]}")
        print(f"  SV ID (play_id for video): {davidson_row[14]}")
    else:
        print("Logan Davidson's pitch (game_pk=777784, at_bat=103, pitch=2) not found in database.")

    # Check some SwordSwing records
    print("\n=== Sample SwordSwing Records ===")
    sword_swings_query = text("""
        SELECT ss.id, ss.pitch_id, ss.sword_score, ss.raw_sword_metric, sp.game_date, sp.player_name, sp.batter
        FROM sword_swings ss
        JOIN statcast_pitches sp ON ss.pitch_id = sp.id
        ORDER BY sp.game_date DESC, ss.sword_score DESC
        LIMIT 5
    """)
    sword_swings_result = conn.execute(sword_swings_query)
    sword_swing_rows = sword_swings_result.fetchall()

    if sword_swing_rows:
        for row in sword_swing_rows:
            print(f"  SwordSwing ID: {row[0]}, Pitch ID: {row[1]}, Scaled Score: {row[2]}, Raw Metric: {row[3]}")
            print(f"    Date: {row[4]}, Pitcher: {row[5]}, Batter ID: {row[6]}")
    else:
        print("No records found in sword_swings table.")

    # Find top 5 highest raw_sword_metric scores and their video_url
    print("\n=== Top 5 All-Time Swords by Raw Metric (from sword_swings table) ===")
    top_raw_swords_query = text("""
        SELECT 
            ss.raw_sword_metric, 
            ss.video_url, 
            ss.sword_score, 
            sp.game_date, 
            sp.player_name as pitcher_name, 
            sp.batter as batter_id, 
            sp.description as pitch_description,
            sp.pitch_name as descriptive_pitch_name,
            sp.release_speed
        FROM sword_swings ss
        JOIN statcast_pitches sp ON ss.pitch_id = sp.id
        WHERE ss.raw_sword_metric IS NOT NULL -- Keep this, as we are ordering by it
        -- Removed 'AND ss.video_url IS NOT NULL' to show top raw scores even if video_url isn't populated yet
        ORDER BY ss.raw_sword_metric DESC
        LIMIT 5
    """)
    top_raw_swords_result = conn.execute(top_raw_swords_query)
    top_raw_sword_rows = top_raw_swords_result.fetchall()

    if top_raw_sword_rows:
        for i, row in enumerate(top_raw_sword_rows):
            print(f"\n#{i+1} Raw Metric: {row[0]:.4f} (Scaled Score: {row[2]})")
            print(f"  Video URL: {row[1]}")
            print(f"  Date: {row[3]}, Pitcher: {row[4]}, Batter ID: {row[5]}")
            print(f"  Pitch: {row[7]} ({row[8]} mph), Desc: {row[6]}")
    else:
        print("No sword_swings records found with raw_sword_metric and video_url to display top 5.")
