import logging
import os
import time
import traceback
from sqlalchemy import text
from models_complete import get_db, SwordSwing, StatcastPitch
from video_downloader import get_video_url_from_sporty_page, download_sword_clip
from app import get_best_video_url # For play_id lookup if sv_id is missing, though app.py's lookup is more complex
                                 # Re-implementing a focused play_id lookup here might be better.
import requests # For MLB Stats API lookup
from datetime import datetime


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not os.environ.get("DATABASE_URL"):
    logger.info("DATABASE_URL not set, setting default.")
    os.environ["DATABASE_URL"] = "postgresql://swordfinder:swordfinder123@localhost:5432/swordfinder_db"

def lookup_play_id_for_pitch(db_session, statcast_pitch_id):
    """
    Looks up the sv_id (video play_id) from statcast_pitches.
    If not found, attempts to find it using MLB Stats API via game_pk, at_bat_number, pitch_number.
    """
    pitch_record = db_session.query(StatcastPitch).filter(StatcastPitch.id == statcast_pitch_id).first()
    if not pitch_record:
        logger.warning(f"No StatcastPitch record found for id {statcast_pitch_id}")
        return None

    if pitch_record.sv_id and pitch_record.sv_id.strip():
        logger.info(f"Found sv_id '{pitch_record.sv_id}' in database for pitch_id {statcast_pitch_id}")
        return pitch_record.sv_id

    # sv_id is missing, try MLB Stats API lookup
    logger.info(f"sv_id missing for pitch_id {statcast_pitch_id}. Attempting MLB Stats API lookup...")
    if not (pitch_record.game_pk and pitch_record.at_bat_number and pitch_record.pitch_number and pitch_record.inning):
        logger.warning(f"Missing game_pk, at_bat_number, pitch_number, or inning for pitch_id {statcast_pitch_id}. Cannot lookup play_id via API.")
        return None

    try:
        mlb_api_url = f"https://statsapi.mlb.com/api/v1.1/game/{pitch_record.game_pk}/feed/live"
        response = requests.get(mlb_api_url, timeout=10)
        response.raise_for_status()
        game_data = response.json()
        all_plays = game_data.get('liveData', {}).get('plays', {}).get('allPlays', [])
        
        current_at_bat_number_in_inning = 0
        for play_idx, play in enumerate(all_plays):
            if play.get('about', {}).get('inning') == pitch_record.inning:
                current_at_bat_number_in_inning +=1 # This is a simplification, MLB at_bat_number is game-global
                # A more robust lookup would match on game_pk and at_bat_number directly if possible,
                # or use a more complex matching logic as in app.py's get_play_id route.
                # For this script, we'll assume at_bat_number from statcast_pitches is reliable.
                
                if play.get('about',{}).get('atBatIndex') +1 == pitch_record.at_bat_number: # atBatIndex is 0-indexed
                     for event in play.get('playEvents', []):
                        if event.get('pitchNumber') == pitch_record.pitch_number:
                            api_play_id = event.get('playId') or event.get('uuid') # Prefer UUIDs
                            if api_play_id:
                                logger.info(f"MLB API: Found play_id '{api_play_id}' for pitch_id {statcast_pitch_id}")
                                # Optionally update statcast_pitches.sv_id here
                                pitch_record.sv_id = api_play_id
                                db_session.commit()
                                return api_play_id
        logger.warning(f"MLB API: Could not find matching play_id for pitch_id {statcast_pitch_id}")
        return None
    except Exception as e:
        logger.error(f"Error looking up play_id via MLB API for pitch_id {statcast_pitch_id}: {e}")
        return None


def process_top_swords_videos():
    logger.info("Processing videos for top 5 all-time swords...")
    start_time = time.time()
    updated_records = 0

    with get_db() as db_session:
        top_swords_query = db_session.query(SwordSwing.id, SwordSwing.pitch_id, StatcastPitch.sv_id).\
            join(StatcastPitch, SwordSwing.pitch_id == StatcastPitch.id).\
            filter(SwordSwing.raw_sword_metric != None).\
            order_by(SwordSwing.raw_sword_metric.desc()).\
            limit(5)
        
        top_sword_candidates = top_swords_query.all()

        if not top_sword_candidates:
            logger.info("No top swords found to process videos for.")
            return

        logger.info(f"Found {len(top_sword_candidates)} top swords to process for video information.")

        for sword_swing_db_id, pitch_db_id, sv_id_from_db in top_sword_candidates:
            logger.info(f"Processing SwordSwing ID: {sword_swing_db_id}, Pitch ID: {pitch_db_id}")
            
            video_play_id = sv_id_from_db
            if not video_play_id or not video_play_id.strip():
                logger.info(f"sv_id is missing or empty for pitch_id {pitch_db_id}. Attempting API lookup.")
                video_play_id = lookup_play_id_for_pitch(db_session, pitch_db_id)

            if not video_play_id or not video_play_id.strip():
                logger.warning(f"Still no valid video_play_id for SwordSwing ID {sword_swing_db_id} after lookup. Skipping video processing.")
                continue

            # Fetch the SwordSwing record to update it
            sword_swing_record = db_session.query(SwordSwing).filter(SwordSwing.id == sword_swing_db_id).first()
            if not sword_swing_record: # Should not happen if we just queried it
                logger.error(f"Consistency error: SwordSwing record ID {sword_swing_db_id} not found for update.")
                continue

            # Construct Savant page URL
            savant_page_url = f"https://baseballsavant.mlb.com/sporty-videos?playId={video_play_id.strip()}&videoType=AWAY"
            sword_swing_record.video_url = savant_page_url
            logger.info(f"Set video_url (Savant page) for SwordSwing ID {sword_swing_db_id}: {savant_page_url}")

            # Get direct MP4 download URL and download the clip
            direct_mp4_url = get_video_url_from_sporty_page(video_play_id)
            if direct_mp4_url:
                sword_swing_record.download_url = direct_mp4_url
                logger.info(f"Found direct MP4 URL for SwordSwing ID {sword_swing_db_id}: {direct_mp4_url}")
                
                download_result = download_sword_clip(video_play_id, direct_mp4_url)
                if download_result:
                    sword_swing_record.local_mp4_path = download_result['path']
                    sword_swing_record.mp4_file_size = download_result['file_size']
                    sword_swing_record.mp4_downloaded = True
                    logger.info(f"Video downloaded for SwordSwing ID {sword_swing_db_id} to {download_result['path']}")
                else:
                    logger.warning(f"Failed to download video for SwordSwing ID {sword_swing_db_id} using URL: {direct_mp4_url}")
                    sword_swing_record.mp4_downloaded = False
            else:
                logger.warning(f"No direct MP4 URL found for SwordSwing ID {sword_swing_db_id} (play_id: {video_play_id})")
                sword_swing_record.download_url = None
                sword_swing_record.mp4_downloaded = False
            
            sword_swing_record.updated_at = datetime.utcnow()
            updated_records +=1
            
            try:
                db_session.commit() # Commit after each sword processing
            except Exception as e:
                db_session.rollback()
                logger.error(f"Error committing update for SwordSwing ID {sword_swing_db_id}: {e}")
                logger.error(traceback.format_exc())


    end_time = time.time()
    logger.info("Top swords video processing completed.")
    logger.info(f"Total SwordSwing records updated with video info: {updated_records}")
    logger.info(f"Time taken: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    process_top_swords_videos()
