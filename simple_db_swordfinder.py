"""
Simplified database-powered SwordFinder using your complete authentic MLB dataset
"""
import logging
import os
from datetime import datetime
from typing import List, Dict
import requests  # Added for MLB Stats API
import traceback # Added for logging full tracebacks
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)
# Attempt to ensure DEBUG messages from this specific logger are processed
logger.setLevel(logging.DEBUG) 
# If Flask's root logger or handlers are restrictive, this might not be enough
# but it's a common way to enable more verbose logging for a specific module.
# A handler might also be needed if no default handler is catching DEBUG for __main__ context.
if not logger.handlers:
    # Add a basic handler if none are configured, to ensure output goes somewhere (e.g., console)
    # This is more relevant if simple_db_swordfinder.py were run standalone.
    # In a Flask app, Flask usually configures logging.
    # However, let's add it to be safe for debugging.
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class SimpleDatabaseSwordFinder:
    """
    Simplified SwordFinder that directly queries your 226,833 authentic MLB records
    """
    
    def __init__(self):
        # Get database URL - local database doesn't need SSL
        db_url = os.environ.get('DATABASE_URL')
        logger.info(f"SimpleDatabaseSwordFinder __init__: DATABASE_URL = {db_url}")
        if not db_url:
            logger.error("DATABASE_URL environment variable not set or empty in SimpleDatabaseSwordFinder!")
            # Potentially raise an error or use a default, but logging is key for now
        
        self.engine = create_engine(db_url)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
    def find_sword_swings(self, date_str: str) -> Dict:
        """
        Find sword swings from your authentic MLB database
        """
        logger.info(f"Finding sword swings for {date_str} from authentic MLB data")
        
        try:
            # Refined query to select only the final pitch of at-bats that were strikeouts
            # and were swinging strikes.
            query = text("""
                WITH final_pitches_of_strikeout_at_bats AS (
                    SELECT DISTINCT ON (game_pk, at_bat_number)
                           id, player_name, pitch_type, bat_speed, 
                           swing_path_tilt, attack_angle,
                           intercept_ball_minus_batter_pos_y_inches as intercept_y,
                           sv_id as play_id_col, -- aliased to avoid conflict with play_id function if any
                           game_pk, description, events,
                           release_speed, launch_speed, launch_angle,
                           home_team, away_team, inning, inning_topbot,
                           at_bat_number, pitch_number, balls, strikes,
                           plate_x, plate_z, sz_top, sz_bot,
                           release_spin_rate, pfx_x, pfx_z,
                           pitch_name, batter, pitcher
                    FROM statcast_pitches
                    WHERE game_date = :date
                      AND events = 'strikeout' -- Ensure the at-bat itself was a strikeout
                    ORDER BY game_pk, at_bat_number, pitch_number DESC -- Gets the last pitch of the AB
                )
                SELECT fp.statcast_pitch_id, -- Renamed id to statcast_pitch_id for clarity
                       fp.player_name, fp.pitch_type, fp.bat_speed, 
                       fp.swing_path_tilt, fp.attack_angle,
                       fp.intercept_y, -- Already aliased in CTE
                       fp.play_id_col as play_id, -- Use the aliased name
                       fp.game_pk, fp.description, fp.events,
                       fp.release_speed, fp.launch_speed, fp.launch_angle,
                       fp.home_team, fp.away_team, fp.inning, fp.inning_topbot,
                       fp.at_bat_number, fp.pitch_number, fp.balls, fp.strikes,
                       fp.plate_x, fp.plate_z, fp.sz_top, fp.sz_bot,
                       fp.release_spin_rate, fp.pfx_x, fp.pfx_z,
                       fp.pitch_name, fp.batter, fp.pitcher
                FROM ( -- Subselect to rename id to statcast_pitch_id before outer select
                    SELECT *, id as statcast_pitch_id 
                    FROM final_pitches_of_strikeout_at_bats
                ) fp
                WHERE fp.description IN ('swinging_strike', 'swinging_strike_blocked') -- Final pitch must be a swinging strike
                  AND fp.bat_speed IS NOT NULL 
                  AND fp.swing_path_tilt IS NOT NULL 
                  AND fp.intercept_y IS NOT NULL -- Corrected to use the alias from CTE
                  -- AND fp.player_name IS NOT NULL -- This was already commented out, keep as is
            """)
            
            # Execute query using the class session or a direct connection
            # Reverting to self.session as the direct connection was for debugging a different issue
            result = self.session.execute(query, {"date": date_str})
            all_candidate_rows = result.fetchall()
            
            logger.info(f"Fetched {len(all_candidate_rows)} sword candidates from SQL for date {date_str}")

            if not all_candidate_rows:
                return {
                    "success": True, "data": [], "count": 0, "date": date_str,
                    "source": "authentic_mlb_database_no_candidates"
                }

            all_scored_swords = []
            
            # Corrected Column indices (32 columns total, 0-31):
            # sp.id (0), sp.player_name (1), sp.pitch_type (2), sp.bat_speed (3), 
            # sp.swing_path_tilt (4), sp.attack_angle (5), 
            # sp.intercept_ball_minus_batter_pos_y_inches (6), sp.sv_id (7), 
            # sp.game_pk (8), sp.description (9), sp.events (10),
            # sp.release_speed (11), sp.launch_speed (12), sp.launch_angle (13),
            # sp.home_team (14), sp.away_team (15), sp.inning (16), sp.inning_topbot (17),
            # sp.at_bat_number (18), sp.pitch_number (19), sp.balls (20), sp.strikes (21),
            # sp.plate_x (22), sp.plate_z (23), sp.sz_top (24), sp.sz_bot (25),
            # sp.release_spin_rate (26), sp.pfx_x (27), sp.pfx_z (28),
            # sp.pitch_name (29), sp.batter (30), sp.pitcher (31)

            for row in all_candidate_rows:
                bat_speed = row[3] or 0
                swing_path_tilt = row[4] or 0
                intercept_y = row[6] or 0
                
                # Use correct indices for plate location data
                plate_x = row[22] 
                plate_z = row[23]
                sz_top = row[24]
                sz_bot = row[25]

                # Calculate dynamic zone penalty factor
                dynamic_zone_penalty_factor = self._calculate_dynamic_zone_penalty(
                    plate_x, plate_z, sz_top, sz_bot
                )

                # Calculate raw sword metric components (normalized 0-1 where higher is "better" for sword)
                bat_speed_comp_norm = (60 - bat_speed) / 60 if bat_speed <= 60 else 0 # Cap at 60mph for calc
                tilt_comp_norm = swing_path_tilt / 60 if swing_path_tilt <= 60 else 1.0 # Cap at 60 deg
                intercept_comp_norm = intercept_y / 50 if intercept_y <= 50 else 1.0 # Cap at 50 inches
                
                # Raw sword metric (sum of weighted normalized components)
                raw_sword_metric = (
                    0.35 * bat_speed_comp_norm +
                    0.25 * tilt_comp_norm +
                    0.25 * intercept_comp_norm +
                    0.15 * dynamic_zone_penalty_factor 
                )
                
                # Store raw metric and other details for sorting and final processing
                # This dictionary is temporary, will be rebuilt for the final top 5
                all_scored_swords.append({
                    "row_data": row, # Keep raw row data for now
                    "raw_sword_metric": raw_sword_metric,
                    # Store components if needed for debugging, e.g.:
                    # "bat_speed_val": bat_speed, "tilt_val": swing_path_tilt, "intercept_y_val": intercept_y,
                    # "zone_penalty_factor": dynamic_zone_penalty_factor
                })

            # Sort all candidates by raw_sword_metric (descending)
            all_scored_swords.sort(key=lambda x: x["raw_sword_metric"], reverse=True)

            # Determine min and max raw scores for daily normalization (from all candidates)
            if not all_scored_swords: # Should be caught by earlier check but good practice
                 min_raw_daily = 0.0
                 max_raw_daily = 1.0 # Avoid division by zero if only one or no swords
            else:
                raw_metrics_for_norm = [s["raw_sword_metric"] for s in all_scored_swords]
                min_raw_daily = min(raw_metrics_for_norm)
                max_raw_daily = max(raw_metrics_for_norm)

            # Select top 5
            top_5_swords_processed = []
            for scored_sword_info in all_scored_swords[:5]:
                row = scored_sword_info["row_data"]
                raw_metric = scored_sword_info["raw_sword_metric"]

                # Universal scaled score (50-100)
                sword_score_scaled_universal = raw_metric * 50 + 50
                
                # Daily normalized score (50-100)
                sword_score_scaled_daily_normalized = 75.0 # Default if max=min
                if max_raw_daily > min_raw_daily:
                    sword_score_scaled_daily_normalized = 50 + ((raw_metric - min_raw_daily) / (max_raw_daily - min_raw_daily)) * 50
                elif all_scored_swords: # If all scores are the same, they are all "top"
                    sword_score_scaled_daily_normalized = 100.0


                # Re-map fields based on new SQL column order
                # Indices based on the corrected list above (32 columns, 0-31)
                statcast_pitch_db_id = row[0] 
                current_play_id_value = row[7] 

                batter_mlbam_id = row[30] 
                pitcher_mlbam_id = row[31]
                
                # Batter name lookup is now done in app.py, so we pass IDs
                # Video URL construction is also now done in app.py

                # Data for app.py to process (batter name, video url, etc.)
                # This dictionary structure should match what app.py expects after this refactor
                processed_sword_dict = {
                    "statcast_pitch_db_id": statcast_pitch_db_id,
                    "pitcher_name": row[1], 
                    "batter_id": batter_mlbam_id, # Pass ID for app.py to lookup name
                    "pitcher_id": pitcher_mlbam_id,
                    "pitch_type": row[2], 
                    "pitch_name": row[29] if row[29] else row[2],
                    "bat_speed": round(row[3] or 0, 1), # Use direct row access for clarity
                    "swing_path_tilt": round(row[4] or 0, 1),
                    "attack_angle": round(row[5] or 0, 1),
                    "intercept_y": round(row[6] or 0, 1),
                    
                    "raw_sword_metric": round(raw_metric, 4), # For reference/debugging
                    "sword_score": round(sword_score_scaled_universal, 1), # Universal score
                    "daily_normalized_score": round(sword_score_scaled_daily_normalized, 1), # Daily UX score
                    
                    "play_id": current_play_id_value, # sv_id from DB (can be None)
                    "game_pk": row[8],
                    "description": row[9],
                    "events": row[10],
                    "release_speed": round(row[11], 1) if row[11] is not None else None,
                    "launch_speed": round(row[12], 1) if row[12] is not None else None,
                    "launch_angle": round(row[13], 1) if row[13] is not None else None,
                    "home_team": row[14],
                    "away_team": row[15],
                    "inning": row[16],
                    "inning_topbot": row[17],
                    "at_bat_number": row[18],
                    "pitch_number": row[19],
                    "balls": row[20],
                    "strikes": row[21],
                    # Use corrected indices for plate_x, plate_z, sz_top, sz_bot
                    "plate_x": round(row[22], 2) if row[22] is not None else None,
                    "plate_z": round(row[23], 2) if row[23] is not None else None,
                    "sz_top": round(row[24], 2) if row[24] is not None else None,
                    "sz_bot": round(row[25], 2) if row[25] is not None else None,
                    "release_spin_rate": round(row[26], 0) if row[26] is not None else None,
                    "pfx_x": round(row[27], 2) if row[27] is not None else None, 
                    "pfx_z": round(row[28], 2) if row[28] is not None else None,
                    # video_url will be constructed in app.py
                }
                top_5_swords_processed.append(processed_sword_dict)
            
            logger.info(f"Processed top {len(top_5_swords_processed)} sword swings for {date_str} after Python scoring.")
            
            return {
                "success": True,
                "data": top_5_swords_processed,
                "count": len(top_5_swords_processed),
                "date": date_str,
                "source": "authentic_mlb_database_python_scored"
            }
            
        except Exception as e:
            logger.error(f"Error in find_sword_swings: {e}")
            logger.error(traceback.format_exc()) # Log full traceback
            return {
                "success": False, "error": str(e), "data": [], "count": 0, "date": date_str
            }

    def _calculate_dynamic_zone_penalty(self, plate_x, plate_z, sz_top, sz_bot):
        """
        Calculates a dynamic zone penalty factor based on Luna's formula.
        Returns a factor (e.g., 1.0 for neutral, >1.0 for pitches out of zone).
        """
        if plate_x is None or plate_z is None or sz_top is None or sz_bot is None:
            return 1.0  # Neutral if location data is missing

        # Approximate half plate width in feet (17 inches / 2 = 8.5 inches = ~0.708 feet)
        # Luna used 0.83, which is closer to 10 inches. Let's stick to Luna's.
        plate_width_half_feet = 0.83 

        out_x_feet = max(abs(plate_x) - plate_width_half_feet, 0)
        
        out_z_feet = 0
        if plate_z < sz_bot: # Pitch is below the strike zone
            out_z_feet = sz_bot - plate_z
        elif plate_z > sz_top: # Pitch is above the strike zone
            out_z_feet = plate_z - sz_top
            
        # Total distance out of typical hitting zone in feet
        total_out_of_zone_feet = out_x_feet + out_z_feet
        penalty_inches = total_out_of_zone_feet * 12 # Convert to inches
        
        # Scale the penalty. Luna: "scaled = min(penalty / 18, 2)".
        # This means a pitch 18 inches out of zone gets a bonus multiplier of 2.
        # The factor returned is 1 (base) + scaled_bonus.
        scaled_bonus = min(penalty_inches / 18.0, 2.0) 
        
        dynamic_factor = 1.0 + scaled_bonus
        # logger.debug(
        #    f"Zone Penalty: px={plate_x}, pz={plate_z}, top={sz_top}, bot={sz_bot} -> "
        #    f"out_x_ft={out_x_feet:.2f}, out_z_ft={out_z_feet:.2f}, inches_out={penalty_inches:.2f}, "
        #    f"scaled_bonus={scaled_bonus:.2f}, factor={dynamic_factor:.2f}"
        # )
        return dynamic_factor
