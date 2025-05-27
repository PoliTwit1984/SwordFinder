import logging
import os
import time
from sqlalchemy import text
from models_complete import get_db, SwordSwing, StatcastPitch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_raw_metric_for_backfill(bat_speed, swing_path_tilt, intercept_y):
    """
    Calculates the raw sword metric using a fixed zone penalty of 1.0.
    This is the sum of weighted normalized components, before 50-100 scaling.
    """
    if bat_speed is None or swing_path_tilt is None or intercept_y is None:
        return None

    bat_speed_val = bat_speed or 0
    tilt_val = swing_path_tilt or 0
    intercept_val = intercept_y or 0

    bat_speed_comp_norm = (60 - bat_speed_val) / 60 if bat_speed_val <= 60 else 0
    tilt_comp_norm = tilt_val / 60 if tilt_val <= 60 else 1.0
    intercept_comp_norm = intercept_val / 50 if intercept_val <= 50 else 1.0
    
    zone_penalty_factor = 1.0 # Fixed for this backfill as per prompt's formula structure

    raw_metric = (
        0.35 * bat_speed_comp_norm +
        0.25 * tilt_comp_norm +
        0.25 * intercept_comp_norm +
        0.15 * zone_penalty_factor 
    )
    return raw_metric

def backfill_raw_sword_metrics(batch_size=100):
    logger.info("Starting backfill of raw_sword_metric for SwordSwing records...")
    start_time = time.time()
    updated_count = 0
    processed_count = 0

    with get_db() as db_session:
        # Query for SwordSwing records needing backfill, joining with StatcastPitch for metrics
        # This query fetches all at once, then processes in Python batches.
        # For very large tables, consider server-side batching with OFFSET/LIMIT.
        query = db_session.query(SwordSwing, StatcastPitch).\
            join(StatcastPitch, SwordSwing.pitch_id == StatcastPitch.id).\
            filter(
                SwordSwing.raw_sword_metric == None, # Check for NULL
                StatcastPitch.events == 'strikeout',
                StatcastPitch.description.in_(['swinging_strike', 'swinging_strike_blocked']),
                StatcastPitch.bat_speed != None,
                StatcastPitch.swing_path_tilt != None,
                StatcastPitch.intercept_ball_minus_batter_pos_y_inches != None
            )
        
        records_to_update = query.all()
        total_to_process = len(records_to_update)
        logger.info(f"Found {total_to_process} SwordSwing records to backfill.")

        if total_to_process == 0:
            logger.info("No records need backfilling.")
            return

        for i in range(0, total_to_process, batch_size):
            batch = records_to_update[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(total_to_process + batch_size - 1)//batch_size} ({len(batch)} records)")
            
            for sword_swing_record, statcast_pitch_record in batch:
                processed_count += 1
                raw_metric = calculate_raw_metric_for_backfill(
                    statcast_pitch_record.bat_speed,
                    statcast_pitch_record.swing_path_tilt,
                    statcast_pitch_record.intercept_ball_minus_batter_pos_y_inches # This is intercept_y
                )

                if raw_metric is not None:
                    sword_swing_record.raw_sword_metric = raw_metric
                    # The scaled sword_score might also need recalculation if it was based on a different formula before
                    # For now, only populating raw_sword_metric as requested.
                    # If the prompt's formula "score = (...)*50+50" was meant for raw_sword_metric,
                    # then raw_metric should be: raw_metric * 50 + 50.
                    # Sticking to unscaled sum of components for 'raw_sword_metric'.
                    updated_count += 1
                else:
                    logger.warning(f"Could not calculate raw_metric for SwordSwing ID {sword_swing_record.id}, pitch_id {sword_swing_record.pitch_id}. Skipping.")

            try:
                db_session.commit()
                logger.info(f"Committed batch. Total updated so far: {updated_count}")
            except Exception as e:
                db_session.rollback()
                logger.error(f"Error committing batch: {e}")
                # Decide if to stop or continue with next batch
                # For now, let's stop on commit error to investigate
                break 
        
    end_time = time.time()
    logger.info(f"Backfill process completed.")
    logger.info(f"Total records processed: {processed_count}")
    logger.info(f"Total records updated with raw_sword_metric: {updated_count}")
    logger.info(f"Time taken: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    if not os.environ.get("DATABASE_URL"):
        logger.info("DATABASE_URL not set for standalone script, setting default.")
        os.environ["DATABASE_URL"] = "postgresql://swordfinder:swordfinder123@localhost:5432/swordfinder_db"
    backfill_raw_sword_metrics()
