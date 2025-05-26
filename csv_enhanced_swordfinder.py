"""
Enhanced SwordFinder that combines database speed with CSV completeness
Uses database for filtering, CSV for complete pitch details
"""
import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, List

logger = logging.getLogger(__name__)

class CSVEnhancedSwordFinder:
    """
    SwordFinder that gets complete pitch details from your CSV file
    """
    def __init__(self):
        database_url = os.environ.get('DATABASE_URL')
        self.engine = create_engine(database_url)
        self.session = self.engine.connect()
        
        # Load your complete CSV data once
        logger.info("Loading complete CSV data for detailed pitch information...")
        self.df = pd.read_csv('complete_statcast_2025.csv', low_memory=False)
        logger.info(f"Loaded {len(self.df)} complete MLB records from CSV")

    def find_sword_swings(self, date_str: str) -> Dict:
        """
        Find sword swings using database for speed, CSV for complete details
        """
        logger.info(f"Finding sword swings for {date_str} with complete pitch details")
        
        try:
            # First, get sword swing candidates from database (fast)
            query = text("""
                SELECT player_name, pitch_type, bat_speed, 
                       swing_path_tilt, attack_angle,
                       intercept_ball_minus_batter_pos_y_inches as intercept_y,
                       game_pk, description
                FROM statcast_pitches 
                WHERE game_date = :date
                AND description IN ('swinging_strike', 'swinging_strike_blocked')
                AND bat_speed IS NOT NULL 
                AND bat_speed < 60
                AND swing_path_tilt IS NOT NULL 
                AND swing_path_tilt > 30
                AND intercept_ball_minus_batter_pos_y_inches IS NOT NULL
                AND intercept_ball_minus_batter_pos_y_inches > 14
                AND player_name IS NOT NULL
                ORDER BY bat_speed ASC, swing_path_tilt DESC, intercept_ball_minus_batter_pos_y_inches DESC
                LIMIT 10
            """)
            
            result = self.session.execute(query, {"date": date_str})
            rows = result.fetchall()
            
            # Now enhance each result with complete CSV data
            sword_swings = []
            for row in rows:
                # Find matching record in CSV for complete details
                csv_match = self.df[
                    (self.df['game_date'] == date_str) &
                    (self.df['player_name'] == row[0]) &
                    (self.df['pitch_type'] == row[1]) &
                    (abs(self.df['bat_speed'].astype(float) - float(row[2])) < 0.1)
                ]
                
                if len(csv_match) > 0:
                    csv_row = csv_match.iloc[0]
                    
                    # Calculate sword score
                    bat_speed = row[2] or 0
                    swing_path_tilt = row[3] or 0
                    intercept_y = row[5] or 0
                    
                    sword_score = (
                        0.35 * ((60 - bat_speed) / 60) +
                        0.25 * (swing_path_tilt / 60) +
                        0.25 * (intercept_y / 50) +
                        0.15 * 1.0
                    ) * 50 + 50
                    
                    # Build complete sword swing with CSV details
                    sword_swing = {
                        "player_name": str(csv_row['player_name']),
                        "pitch_type": str(csv_row['pitch_type']),
                        "pitch_name": str(csv_row.get('pitch_name', csv_row['pitch_type'])),
                        "bat_speed": round(float(csv_row['bat_speed']), 1),
                        "swing_path_tilt": round(float(csv_row['swing_path_tilt']), 1),
                        "attack_angle": round(float(csv_row['attack_angle']), 1),
                        "intercept_y": round(float(csv_row['intercept_ball_minus_batter_pos_y_inches']), 1),
                        "sword_score": round(sword_score, 1),
                        "game_pk": int(csv_row['game_pk']),
                        "description": str(csv_row['description']),
                        "events": str(csv_row.get('events', '')),
                        "release_speed": round(float(csv_row['release_speed']) if pd.notna(csv_row['release_speed']) else 0, 1),
                        "home_team": str(csv_row['home_team']) if pd.notna(csv_row['home_team']) else 'Unknown',
                        "away_team": str(csv_row['away_team']) if pd.notna(csv_row['away_team']) else 'Unknown',
                        "inning": int(csv_row['inning']) if pd.notna(csv_row['inning']) else 0,
                        "inning_topbot": str(csv_row['inning_topbot']) if pd.notna(csv_row['inning_topbot']) else 'Unknown',
                        "at_bat_number": int(csv_row['at_bat_number']) if pd.notna(csv_row['at_bat_number']) else 0,
                        "pitch_number": int(csv_row['pitch_number']) if pd.notna(csv_row['pitch_number']) else 0,
                        "balls": int(csv_row['balls']) if pd.notna(csv_row['balls']) else 0,
                        "strikes": int(csv_row['strikes']) if pd.notna(csv_row['strikes']) else 0,
                        "plate_x": round(float(csv_row['plate_x']) if pd.notna(csv_row['plate_x']) else 0, 2),
                        "plate_z": round(float(csv_row['plate_z']) if pd.notna(csv_row['plate_z']) else 0, 2),
                        "sz_top": round(float(csv_row['sz_top']) if pd.notna(csv_row['sz_top']) else 0, 2),
                        "sz_bot": round(float(csv_row['sz_bot']) if pd.notna(csv_row['sz_bot']) else 0, 2),
                        "release_spin_rate": round(float(csv_row['release_spin_rate']) if pd.notna(csv_row['release_spin_rate']) else 0, 0),
                        "pfx_x": round(float(csv_row['pfx_x']) if pd.notna(csv_row['pfx_x']) else 0, 2),
                        "pfx_z": round(float(csv_row['pfx_z']) if pd.notna(csv_row['pfx_z']) else 0, 2),
                        "play_id": str(csv_row['sv_id']) if pd.notna(csv_row['sv_id']) else None,
                        "video_url": f"https://baseballsavant.mlb.com/sporty-videos?playId={csv_row['sv_id']}&videoType=AWAY" if pd.notna(csv_row['sv_id']) else None
                    }
                    
                    sword_swings.append(sword_swing)
                    
                    if len(sword_swings) >= 5:
                        break
            
            logger.info(f"Found {len(sword_swings)} complete sword swings for {date_str}")
            
            return {
                "success": True,
                "date": date_str,
                "count": len(sword_swings),
                "data": sword_swings
            }
            
        except Exception as e:
            logger.error(f"Error finding sword swings: {e}")
            return {
                "success": False,
                "error": str(e),
                "date": date_str,
                "count": 0,
                "data": []
            }