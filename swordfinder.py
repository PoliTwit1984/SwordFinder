import logging
import pandas as pd
import numpy as np
import requests
from datetime import datetime
import traceback

logger = logging.getLogger(__name__)

class SwordFinder:
    """
    Core logic for identifying and scoring sword swings from Statcast data
    """
    
    def __init__(self):
        self.weight_bat_speed = 0.35
        self.weight_swing_tilt = 0.25
        self.weight_intercept_y = 0.25
        self.weight_zone_penalty = 0.15
        
    def find_sword_swings(self, date_str):
        """
        Main method to find and score sword swings for a given date
        
        Args:
            date_str (str): Date in YYYY-MM-DD format
            
        Returns:
            list: Top 5 sword swings with scores
        """
        try:
            # Import pybaseball here to handle potential import issues
            try:
                from pybaseball import statcast
            except ImportError as e:
                logger.error(f"Failed to import pybaseball: {e}")
                raise Exception("pybaseball library not available. Please install it.")
            
            logger.info(f"Fetching Statcast data for {date_str}")
            
            # Fetch Statcast data for the given date
            try:
                data = statcast(start_dt=date_str, end_dt=date_str)
            except Exception as e:
                logger.error(f"Failed to fetch Statcast data: {e}")
                raise Exception(f"Failed to fetch Statcast data: {str(e)}")
            
            if data is None or len(data) == 0:
                logger.warning(f"No Statcast data found for {date_str}")
                return []
            
            logger.info(f"Retrieved {len(data)} total pitches from Statcast")
            
            # Apply sword swing filters
            sword_candidates = self._apply_sword_filters(data)
            
            if len(sword_candidates) == 0:
                logger.info("No sword swings found matching criteria")
                return []
            
            logger.info(f"Found {len(sword_candidates)} sword swing candidates")
            
            # Calculate sword scores
            scored_swings = self._calculate_sword_scores(sword_candidates)
            
            # Return top 5 sword swings
            top_swings = scored_swings.head(5)
            
            # Fetch playIds for each sword swing
            results_with_playids = self._add_play_ids(top_swings)
            
            return self._format_results(results_with_playids)
            
        except Exception as e:
            logger.error(f"Error in find_sword_swings: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def _apply_sword_filters(self, data):
        """
        Apply filtering criteria to identify sword swing candidates
        
        Criteria:
        - swinging_strike or swinging_strike_blocked
        - bat_speed < 60
        - intercept_ball_minus_batter_pos_y_inches > 14
        - swing_path_tilt > 30
        """
        logger.info("Applying sword swing filters")
        
        # Create a copy to avoid modifying original data
        filtered_data = data.copy()
        
        # Filter 1: Swing outcome (swinging_strike or swinging_strike_blocked)
        swing_outcomes = ['swinging_strike', 'swinging_strike_blocked']
        filtered_data = filtered_data[filtered_data['description'].isin(swing_outcomes)]
        logger.info(f"After swing outcome filter: {len(filtered_data)} swings")
        
        if len(filtered_data) == 0:
            return pd.DataFrame()
        
        # Filter 2: Bat speed < 60 mph
        # Handle missing bat_speed values
        filtered_data = filtered_data.dropna(subset=['bat_speed'])
        filtered_data = filtered_data[filtered_data['bat_speed'] < 60]
        logger.info(f"After bat speed filter: {len(filtered_data)} swings")
        
        if len(filtered_data) == 0:
            return pd.DataFrame()
        
        # Filter 3: Intercept Y > 14 inches
        # Handle missing intercept values
        intercept_col = 'intercept_ball_minus_batter_pos_y_inches'
        if intercept_col in filtered_data.columns:
            filtered_data = filtered_data.dropna(subset=[intercept_col])
            filtered_data = filtered_data[filtered_data[intercept_col] > 14]
            logger.info(f"After intercept Y filter: {len(filtered_data)} swings")
        else:
            logger.warning(f"Column {intercept_col} not found in data")
            return pd.DataFrame()
        
        if len(filtered_data) == 0:
            return pd.DataFrame()
        
        # Filter 4: Swing path tilt > 30 degrees
        if 'swing_path_tilt' in filtered_data.columns:
            filtered_data = filtered_data.dropna(subset=['swing_path_tilt'])
            filtered_data = filtered_data[filtered_data['swing_path_tilt'] > 30]
            logger.info(f"After swing path tilt filter: {len(filtered_data)} swings")
        else:
            logger.warning("Column 'swing_path_tilt' not found in data")
            return pd.DataFrame()
        
        return filtered_data
    
    def _calculate_sword_scores(self, data):
        """
        Calculate weighted sword scores for each swing
        
        Formula:
        sword_score = (
            0.35 * (60 - bat_speed)/60 +
            0.25 * swing_path_tilt/60 +
            0.25 * intercept_y/50 +
            0.15 * zone_penalty
        )
        """
        logger.info("Calculating sword scores")
        
        scored_data = data.copy()
        
        # Calculate individual components
        bat_speed_component = self.weight_bat_speed * (60 - scored_data['bat_speed']) / 60
        tilt_component = self.weight_swing_tilt * scored_data['swing_path_tilt'] / 60
        intercept_component = self.weight_intercept_y * scored_data['intercept_ball_minus_batter_pos_y_inches'] / 50
        
        # Calculate zone penalty (higher penalty for strikes in zone)
        zone_penalty = self._calculate_zone_penalty(scored_data)
        zone_component = self.weight_zone_penalty * zone_penalty
        
        # Calculate raw sword score
        raw_score = bat_speed_component + tilt_component + intercept_component + zone_component
        
        # Normalize to 50-100 scale
        min_score = raw_score.min()
        max_score = raw_score.max()
        
        if max_score == min_score:
            normalized_score = pd.Series([75.0] * len(raw_score), index=raw_score.index)
        else:
            normalized_score = 50 + (raw_score - min_score) / (max_score - min_score) * 50
        
        scored_data['sword_score'] = normalized_score
        
        # Sort by sword score descending
        scored_data = scored_data.sort_values('sword_score', ascending=False)
        
        return scored_data
    
    def _calculate_zone_penalty(self, data):
        """
        Calculate zone penalty based on strike zone location
        Higher penalty for pitches in the strike zone (easier to hit)
        """
        if 'zone' in data.columns:
            # Zone 1-9 are in strike zone, zone 11-14 are outside
            zone_penalty = np.where(data['zone'].between(1, 9), 0.3, 0.7)
        else:
            # Default penalty if zone data not available
            zone_penalty = np.full(len(data), 0.5)
        
        return pd.Series(zone_penalty, index=data.index)
    
    def _add_play_ids(self, data):
        """
        Fetch playIds for each sword swing using MLB Stats API
        """
        logger.info("Fetching playIds for sword swings")
        
        # Create a copy to avoid modifying original data
        data_with_playids = data.copy()
        
        # Group by game_pk to minimize API calls
        unique_games = data['game_pk'].unique()
        
        for game_pk in unique_games:
            try:
                # Fetch game data from MLB Stats API
                mlb_api_url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
                response = requests.get(mlb_api_url, timeout=10)
                response.raise_for_status()
                game_data = response.json()
                
                # Parse plays data
                all_plays = game_data['liveData']['plays']['allPlays']
                
                # For each sword swing in this game, find its playId
                game_swings = data[data['game_pk'] == game_pk]
                
                for idx, swing in game_swings.iterrows():
                    inning = swing['inning']
                    pitch_number = swing['pitch_number']
                    
                    # Search for matching pitch in game data
                    play_id = self._find_play_id_for_pitch(all_plays, inning, pitch_number)
                    
                    if play_id:
                        data_with_playids.loc[idx, 'play_id'] = play_id
                        logger.debug(f"Found playId {play_id} for game {game_pk}, inning {inning}, pitch {pitch_number}")
                    else:
                        logger.warning(f"No playId found for game {game_pk}, inning {inning}, pitch {pitch_number}")
                        
            except Exception as e:
                logger.warning(f"Failed to fetch playIds for game {game_pk}: {str(e)}")
                continue
        
        return data_with_playids
    
    def _find_play_id_for_pitch(self, all_plays, target_inning, target_pitch_number):
        """
        Find the playId for a specific pitch within game play data
        """
        for play in all_plays:
            play_about = play.get('about', {})
            play_inning = play_about.get('inning')
            
            # Only check plays from the target inning
            if play_inning == target_inning:
                play_events = play.get('playEvents', [])
                
                for event in play_events:
                    event_pitch_number = event.get('pitchNumber')
                    
                    if event_pitch_number == target_pitch_number:
                        # Look for UUID playId in the event
                        uuid_play_id = (
                            event.get('playId') or
                            event.get('uuid') or
                            event.get('guid') or
                            event.get('playGuid')
                        )
                        
                        if uuid_play_id:
                            return str(uuid_play_id)
        
        return None
    
    def _format_results(self, data):
        """
        Format the results for JSON response
        """
        results = []
        
        for _, row in data.iterrows():
            play_id = self._safe_get(row, 'play_id', '')
            video_url = f"https://baseballsavant.mlb.com/sporty-videos?playId={play_id}" if play_id else ""
            
            result = {
                "player_name": self._safe_get(row, 'player_name', 'Unknown Player'),
                "pitch_type": self._safe_get(row, 'pitch_type', 'Unknown'),
                "bat_speed": round(float(row['bat_speed']), 1),
                "intercept_y": round(float(row['intercept_ball_minus_batter_pos_y_inches']), 1),
                "swing_path_tilt": round(float(row['swing_path_tilt']), 1),
                "sword_score": round(float(row['sword_score']), 1),
                "play_id": play_id,
                "video_url": video_url,
                "game_pk": int(row['game_pk']) if pd.notna(row.get('game_pk')) else None,
                "inning": int(row['inning']) if pd.notna(row.get('inning')) else None,
                "pitch_number": int(row['pitch_number']) if pd.notna(row.get('pitch_number')) else None
            }
            results.append(result)
        
        return results
    
    def _safe_get(self, row, column, default):
        """
        Safely get a value from a pandas row with a default fallback
        """
        try:
            value = row.get(column, default)
            if pd.isna(value) or value == '':
                return default
            return str(value)
        except:
            return default
