"""
Simplified database-powered SwordFinder using your complete authentic MLB dataset
"""
import logging
import os
from datetime import datetime
from typing import List, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

class SimpleDatabaseSwordFinder:
    """
    Simplified SwordFinder that directly queries your 226,833 authentic MLB records
    """
    
    def __init__(self):
        self.engine = create_engine(os.environ.get('DATABASE_URL'))
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
    def find_sword_swings(self, date_str: str) -> Dict:
        """
        Find sword swings from your authentic MLB database
        """
        logger.info(f"Finding sword swings for {date_str} from authentic MLB data")
        
        try:
            # Query your authentic MLB data for sword swing candidates
            query = text("""
                SELECT player_name, pitch_type, bat_speed, 
                       swing_path_tilt, attack_angle,
                       intercept_ball_minus_batter_pos_y_inches as intercept_y,
                       sv_id as play_id, game_pk, description,
                       release_speed, launch_speed, launch_angle
                FROM statcast_pitches 
                WHERE game_date = :date
                AND description IN ('swinging_strike', 'swinging_strike_blocked')
                AND bat_speed IS NOT NULL 
                AND bat_speed < 60
                AND swing_path_tilt IS NOT NULL 
                AND swing_path_tilt > 30
                AND intercept_ball_minus_batter_pos_y_inches IS NOT NULL
                AND intercept_ball_minus_batter_pos_y_inches > 14
                ORDER BY bat_speed ASC, swing_path_tilt DESC, intercept_ball_minus_batter_pos_y_inches DESC
                LIMIT 5
            """)
            
            result = self.session.execute(query, {"date": date_str})
            rows = result.fetchall()
            
            sword_swings = []
            for row in rows:
                # Calculate sword score using your proven formula
                bat_speed = row[2] or 0
                swing_path_tilt = row[3] or 0
                intercept_y = row[5] or 0
                
                # Sword scoring formula (normalized to 50-100 scale)
                sword_score = (
                    0.35 * ((60 - bat_speed) / 60) +
                    0.25 * (swing_path_tilt / 60) +
                    0.25 * (intercept_y / 50) +
                    0.15 * 1.0  # zone penalty factor
                ) * 50 + 50  # Scale to 50-100
                
                sword_swing = {
                    "player_name": row[0],
                    "pitch_type": row[1],
                    "bat_speed": round(bat_speed, 1),
                    "swing_path_tilt": round(swing_path_tilt, 1),
                    "attack_angle": round(row[4] or 0, 1),
                    "intercept_y": round(intercept_y, 1),
                    "sword_score": round(sword_score, 1),
                    "play_id": row[6],
                    "game_pk": row[7],
                    "description": row[8],
                    "video_url": f"https://baseballsavant.mlb.com/sporty-videos?playId={row[6]}&videoType=AWAY" if row[6] else None,
                    "release_speed": round(row[9] or 0, 1),
                    "launch_speed": round(row[10] or 0, 1),
                    "launch_angle": round(row[11] or 0, 1)
                }
                
                sword_swings.append(sword_swing)
            
            logger.info(f"Found {len(sword_swings)} authentic sword swings for {date_str}")
            
            return {
                "success": True,
                "data": sword_swings,
                "count": len(sword_swings),
                "date": date_str,
                "source": "authentic_mlb_database"
            }
            
        except Exception as e:
            logger.error(f"Error querying authentic MLB data: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "count": 0,
                "date": date_str
            }