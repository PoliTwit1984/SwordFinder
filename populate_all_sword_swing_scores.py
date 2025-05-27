import logging
import os
import time
import traceback # Added for full traceback logging
from datetime import datetime # Added import for datetime
from sqlalchemy import text, distinct
from models_complete import get_db, SwordSwing, StatcastPitch
from simple_db_swordfinder import SimpleDatabaseSwordFinder 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not os.environ.get("DATABASE_URL"):
    logger.info("DATABASE_URL not set for populate_all_sword_swing_scores.py, setting default.")
    os.environ["DATABASE_URL"] = "postgresql://swordfinder:swordfinder123@localhost:5432/swordfinder_db"

temp_finder_instance = SimpleDatabaseSwordFinder()

def get_all_game_dates(db_session):
    """Fetches all distinct game_dates from statcast_pitches."""
    dates_query = db_session.query(distinct(StatcastPitch.game_date)).order_by(StatcastPitch.game_date)
    dates_result = dates_query.all()
    return [date_row[0] for date_row in dates_result if date_row[0] is not None]

def process_date(date_str, db_session):
    """
    Processes a single date: finds swords, calculates scores, and updates/creates SwordSwing records.
    """
    logger.info(f"Processing date: {date_str}")
    
    query_sql = text("""
        WITH final_pitches_of_strikeout_at_bats AS (
            SELECT DISTINCT ON (game_pk, at_bat_number)
                   id, player_name, pitch_type, bat_speed, 
                   swing_path_tilt, attack_angle,
                   intercept_ball_minus_batter_pos_y_inches as intercept_y,
                   sv_id as play_id_col,
                   game_pk, description, events,
                   release_speed, launch_speed, launch_angle,
                   home_team, away_team, inning, inning_topbot,
                   at_bat_number, pitch_number, balls, strikes,
                   plate_x, plate_z, sz_top, sz_bot,
                   release_spin_rate, pfx_x, pfx_z,
                   pitch_name, batter, pitcher
            FROM statcast_pitches
            WHERE game_date = :date
              AND events = 'strikeout'
            ORDER BY game_pk, at_bat_number, pitch_number DESC
        )
        SELECT fp.id as statcast_pitch_id, 
               fp.player_name, fp.pitch_type, fp.bat_speed, 
               fp.swing_path_tilt, fp.attack_angle,
               fp.intercept_y,
               fp.play_id_col as play_id, 
               fp.game_pk, fp.description, fp.events,
               fp.release_speed, fp.launch_speed, fp.launch_angle,
               fp.home_team, fp.away_team, fp.inning, fp.inning_topbot,
               fp.at_bat_number, fp.pitch_number, fp.balls, fp.strikes,
               fp.plate_x, fp.plate_z, fp.sz_top, fp.sz_bot,
               fp.release_spin_rate, fp.pfx_x, fp.pfx_z,
               fp.pitch_name, fp.batter, fp.pitcher
        FROM final_pitches_of_strikeout_at_bats fp
        WHERE fp.description IN ('swinging_strike', 'swinging_strike_blocked')
          AND fp.bat_speed IS NOT NULL 
          AND fp.swing_path_tilt IS NOT NULL 
          AND fp.intercept_y IS NOT NULL
    """)

    all_candidate_rows = db_session.execute(query_sql, {"date": date_str}).fetchall()
    logger.info(f"Date {date_str}: Fetched {len(all_candidate_rows)} sword candidates from SQL.")

    if not all_candidate_rows:
        return 0

    updated_for_date_count = 0
    for row in all_candidate_rows:
        statcast_pitch_id = row[0]
        bat_speed = row[3] or 0
        swing_path_tilt = row[4] or 0
        intercept_y = row[6] or 0
        
        plate_x = row[22]
        plate_z = row[23]
        sz_top = row[24]
        sz_bot = row[25]

        dynamic_zone_penalty_factor = temp_finder_instance._calculate_dynamic_zone_penalty(
            plate_x, plate_z, sz_top, sz_bot
        )

        bat_speed_comp_norm = (60 - bat_speed) / 60 if bat_speed <= 60 else 0
        tilt_comp_norm = swing_path_tilt / 60 if swing_path_tilt <= 60 else 1.0
        intercept_comp_norm = intercept_y / 50 if intercept_y <= 50 else 1.0
        
        raw_sword_metric = (
            0.35 * bat_speed_comp_norm +
            0.25 * tilt_comp_norm +
            0.25 * intercept_comp_norm +
            0.15 * dynamic_zone_penalty_factor 
        )
        
        scaled_sword_score = raw_sword_metric * 50 + 50

        sword_swing_record = db_session.query(SwordSwing).filter(SwordSwing.pitch_id == statcast_pitch_id).first()
        if not sword_swing_record:
            sword_swing_record = SwordSwing(pitch_id=statcast_pitch_id, is_sword_swing=True)
            db_session.add(sword_swing_record)
            logger.debug(f"Date {date_str}: Creating new SwordSwing for pitch_id {statcast_pitch_id}")
        
        sword_swing_record.raw_sword_metric = round(raw_sword_metric, 4)
        sword_swing_record.sword_score = round(scaled_sword_score, 1)
        sword_swing_record.updated_at = datetime.utcnow() # Consider timezone.utc if using Python 3.11+
        updated_for_date_count +=1
    
    try:
        db_session.commit()
        logger.info(f"Date {date_str}: Committed {updated_for_date_count} SwordSwing updates/creations.")
    except Exception as e:
        db_session.rollback()
        logger.error(f"Date {date_str}: Error committing SwordSwing updates: {e}")
        logger.error(traceback.format_exc())
        return 0
        
    return updated_for_date_count

def run_population(test_date=None):
    logger.info("Starting comprehensive population of raw_sword_metric in sword_swings table...")
    overall_start_time = time.time()
    total_records_updated_across_all_dates = 0

    if test_date:
        logger.info(f"TEST MODE: Processing only for specified date: {test_date}")
        game_dates = [test_date]
    else:
        with get_db() as db_session_outer:
            game_dates = get_all_game_dates(db_session_outer)
            logger.info(f"Found {len(game_dates)} distinct game dates to process.")

    for date_str in game_dates:
        with get_db() as db_session_for_date:
            updated_count_for_this_date = process_date(date_str, db_session_for_date)
            total_records_updated_across_all_dates += updated_count_for_this_date
            if not test_date: 
                time.sleep(0.1) 

    overall_end_time = time.time()
    logger.info("Comprehensive population process completed.")
    logger.info(f"Total SwordSwing records created/updated: {total_records_updated_across_all_dates}")
    logger.info(f"Total time taken: {overall_end_time - overall_start_time:.2f} seconds")

if __name__ == "__main__":
    # To run for all dates: run_population()
    # To test for a single date: run_population(test_date="YYYY-MM-DD")
    logger.info("Running population script for ALL dates...")
    run_population()
