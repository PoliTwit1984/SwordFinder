#!/usr/bin/env python3
"""
Import complete authentic MLB Statcast data with all 118 fields into PostgreSQL database
"""

import pandas as pd
import logging
from models_complete import get_db, StatcastPitch, create_tables
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def import_complete_authentic_data(csv_path: str = 'complete_statcast_2025.csv'):
    """
    Import complete authentic MLB Statcast data from CSV file into database
    """
    logger.info(f"Starting import of authentic MLB data from {csv_path}")
    
    try:
        # Load CSV file
        logger.info("Loading authentic MLB dataset...")
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"Loaded {len(df):,} authentic MLB records")
        
        # Check for sword swing data
        sword_fields = ['bat_speed', 'swing_path_tilt', 'intercept_ball_minus_batter_pos_y_inches', 'attack_angle']
        swinging_strikes = df[df['description'].isin(['swinging_strike', 'swinging_strike_blocked'])]
        complete_sword_data = swinging_strikes.dropna(subset=sword_fields)
        logger.info(f"Found {len(complete_sword_data):,} complete sword swings in authentic data!")
        
        # Ensure tables exist
        create_tables()
        
        with get_db() as db:
            # Clear existing data
            logger.info("Clearing existing data...")
            db.execute(text("DELETE FROM sword_swings"))
            db.execute(text("DELETE FROM daily_results"))
            db.execute(text("DELETE FROM statcast_pitches"))
            db.commit()
            
            # Process in chunks for better memory management
            chunk_size = 1000
            total_chunks = (len(df) + chunk_size - 1) // chunk_size
            
            for i in range(0, len(df), chunk_size):
                chunk_num = (i // chunk_size) + 1
                logger.info(f"Processing chunk {chunk_num}/{total_chunks} ({chunk_size} records)")
                
                chunk = df.iloc[i:i + chunk_size]
                records = []
                
                for _, row in chunk.iterrows():
                    record = StatcastPitch(
                        # Core pitch identification
                        pitch_type=safe_str(row.get('pitch_type')),
                        game_date=safe_str(row.get('game_date')),
                        release_speed=safe_float(row.get('release_speed')),
                        release_pos_x=safe_float(row.get('release_pos_x')),
                        release_pos_z=safe_float(row.get('release_pos_z')),
                        player_name=safe_str(row.get('player_name')),
                        batter=safe_int(row.get('batter')),
                        pitcher=safe_int(row.get('pitcher')),
                        events=safe_str(row.get('events')),
                        description=safe_str(row.get('description')),
                        
                        # Spin and break (legacy)
                        spin_dir=safe_float(row.get('spin_dir')),
                        spin_rate_deprecated=safe_float(row.get('spin_rate_deprecated')),
                        break_angle_deprecated=safe_float(row.get('break_angle_deprecated')),
                        break_length_deprecated=safe_float(row.get('break_length_deprecated')),
                        
                        # Zone and game context
                        zone=safe_int(row.get('zone')),
                        des=safe_str(row.get('des')),
                        game_type=safe_str(row.get('game_type')),
                        stand=safe_str(row.get('stand')),
                        p_throws=safe_str(row.get('p_throws')),
                        home_team=safe_str(row.get('home_team')),
                        away_team=safe_str(row.get('away_team')),
                        type=safe_str(row.get('type')),
                        hit_location=safe_int(row.get('hit_location')),
                        bb_type=safe_str(row.get('bb_type')),
                        balls=safe_int(row.get('balls')),
                        strikes=safe_int(row.get('strikes')),
                        game_year=safe_int(row.get('game_year')),
                        
                        # Pitch movement and location
                        pfx_x=safe_float(row.get('pfx_x')),
                        pfx_z=safe_float(row.get('pfx_z')),
                        plate_x=safe_float(row.get('plate_x')),
                        plate_z=safe_float(row.get('plate_z')),
                        
                        # Baserunners
                        on_3b=safe_int(row.get('on_3b')),
                        on_2b=safe_int(row.get('on_2b')),
                        on_1b=safe_int(row.get('on_1b')),
                        outs_when_up=safe_int(row.get('outs_when_up')),
                        inning=safe_int(row.get('inning')),
                        inning_topbot=safe_str(row.get('inning_topbot')),
                        
                        # Hit coordinates
                        hc_x=safe_float(row.get('hc_x')),
                        hc_y=safe_float(row.get('hc_y')),
                        
                        # Deprecated timestamp fields
                        tfs_deprecated=safe_str(row.get('tfs_deprecated')),
                        tfs_zulu_deprecated=safe_str(row.get('tfs_zulu_deprecated')),
                        
                        # Officials and identifiers
                        umpire=safe_int(row.get('umpire')),
                        sv_id=safe_str(row.get('sv_id')),  # This is the play ID for videos
                        
                        # Physics vectors
                        vx0=safe_float(row.get('vx0')),
                        vy0=safe_float(row.get('vy0')),
                        vz0=safe_float(row.get('vz0')),
                        ax=safe_float(row.get('ax')),
                        ay=safe_float(row.get('ay')),
                        az=safe_float(row.get('az')),
                        
                        # Strike zone dimensions
                        sz_top=safe_float(row.get('sz_top')),
                        sz_bot=safe_float(row.get('sz_bot')),
                        
                        # Hit outcome data
                        hit_distance_sc=safe_float(row.get('hit_distance_sc')),
                        launch_speed=safe_float(row.get('launch_speed')),
                        launch_angle=safe_float(row.get('launch_angle')),
                        effective_speed=safe_float(row.get('effective_speed')),
                        release_spin_rate=safe_float(row.get('release_spin_rate')),
                        release_extension=safe_float(row.get('release_extension')),
                        game_pk=safe_int(row.get('game_pk')),
                        
                        # Defensive positions
                        fielder_2=safe_int(row.get('fielder_2')),
                        fielder_3=safe_int(row.get('fielder_3')),
                        fielder_4=safe_int(row.get('fielder_4')),
                        fielder_5=safe_int(row.get('fielder_5')),
                        fielder_6=safe_int(row.get('fielder_6')),
                        fielder_7=safe_int(row.get('fielder_7')),
                        fielder_8=safe_int(row.get('fielder_8')),
                        fielder_9=safe_int(row.get('fielder_9')),
                        release_pos_y=safe_float(row.get('release_pos_y')),
                        
                        # Expected outcome statistics
                        estimated_ba_using_speedangle=safe_float(row.get('estimated_ba_using_speedangle')),
                        estimated_woba_using_speedangle=safe_float(row.get('estimated_woba_using_speedangle')),
                        woba_value=safe_float(row.get('woba_value')),
                        woba_denom=safe_int(row.get('woba_denom')),
                        babip_value=safe_float(row.get('babip_value')),
                        iso_value=safe_float(row.get('iso_value')),
                        launch_speed_angle=safe_int(row.get('launch_speed_angle')),
                        
                        # At-bat sequencing
                        at_bat_number=safe_int(row.get('at_bat_number')),
                        pitch_number=safe_int(row.get('pitch_number')),
                        pitch_name=safe_str(row.get('pitch_name')),
                        
                        # Score state
                        home_score=safe_int(row.get('home_score')),
                        away_score=safe_int(row.get('away_score')),
                        bat_score=safe_int(row.get('bat_score')),
                        fld_score=safe_int(row.get('fld_score')),
                        post_away_score=safe_int(row.get('post_away_score')),
                        post_home_score=safe_int(row.get('post_home_score')),
                        post_bat_score=safe_int(row.get('post_bat_score')),
                        post_fld_score=safe_int(row.get('post_fld_score')),
                        
                        # Defensive alignment
                        if_fielding_alignment=safe_str(row.get('if_fielding_alignment')),
                        of_fielding_alignment=safe_str(row.get('of_fielding_alignment')),
                        
                        # Advanced pitch metrics
                        spin_axis=safe_float(row.get('spin_axis')),
                        delta_home_win_exp=safe_float(row.get('delta_home_win_exp')),
                        delta_run_exp=safe_float(row.get('delta_run_exp')),
                        
                        # ⚔️ SWORD SWING METRICS ⚔️
                        bat_speed=safe_float(row.get('bat_speed')),
                        swing_length=safe_float(row.get('swing_length')),
                        
                        # More expected stats
                        estimated_slg_using_speedangle=safe_float(row.get('estimated_slg_using_speedangle')),
                        delta_pitcher_run_exp=safe_float(row.get('delta_pitcher_run_exp')),
                        hyper_speed=safe_float(row.get('hyper_speed')),
                        home_score_diff=safe_int(row.get('home_score_diff')),
                        bat_score_diff=safe_int(row.get('bat_score_diff')),
                        home_win_exp=safe_float(row.get('home_win_exp')),
                        bat_win_exp=safe_float(row.get('bat_win_exp')),
                        
                        # Player age data
                        age_pit_legacy=safe_float(row.get('age_pit_legacy')),
                        age_bat_legacy=safe_float(row.get('age_bat_legacy')),
                        age_pit=safe_float(row.get('age_pit')),
                        age_bat=safe_float(row.get('age_bat')),
                        
                        # Game context metrics
                        n_thruorder_pitcher=safe_int(row.get('n_thruorder_pitcher')),
                        n_priorpa_thisgame_player_at_bat=safe_int(row.get('n_priorpa_thisgame_player_at_bat')),
                        pitcher_days_since_prev_game=safe_int(row.get('pitcher_days_since_prev_game')),
                        batter_days_since_prev_game=safe_int(row.get('batter_days_since_prev_game')),
                        pitcher_days_until_next_game=safe_int(row.get('pitcher_days_until_next_game')),
                        batter_days_until_next_game=safe_int(row.get('batter_days_until_next_game')),
                        
                        # Advanced break measurements
                        api_break_z_with_gravity=safe_float(row.get('api_break_z_with_gravity')),
                        api_break_x_arm=safe_float(row.get('api_break_x_arm')),
                        api_break_x_batter_in=safe_float(row.get('api_break_x_batter_in')),
                        arm_angle=safe_float(row.get('arm_angle')),
                        
                        # ⚔️ MORE CRITICAL SWORD SWING METRICS ⚔️
                        attack_angle=safe_float(row.get('attack_angle')),
                        attack_direction=safe_float(row.get('attack_direction')),
                        swing_path_tilt=safe_float(row.get('swing_path_tilt')),
                        intercept_ball_minus_batter_pos_x_inches=safe_float(row.get('intercept_ball_minus_batter_pos_x_inches')),
                        intercept_ball_minus_batter_pos_y_inches=safe_float(row.get('intercept_ball_minus_batter_pos_y_inches'))
                    )
                    records.append(record)
                
                # Bulk insert
                db.add_all(records)
                db.commit()
                logger.info(f"Inserted {len(records)} authentic MLB records")
            
            # Verify the import
            total_count = db.query(StatcastPitch).count()
            logger.info(f"✅ Import completed successfully!")
            logger.info(f"Total authentic MLB records in database: {total_count:,}")
            
            # Check sword swing data availability
            sword_data_count = db.query(StatcastPitch).filter(
                StatcastPitch.bat_speed.isnot(None)
            ).count()
            logger.info(f"Records with authentic sword swing data: {sword_data_count:,}")
            
            swinging_strikes = db.query(StatcastPitch).filter(
                StatcastPitch.description.in_(['swinging_strike', 'swinging_strike_blocked'])
            ).count()
            logger.info(f"Total swinging strikes: {swinging_strikes:,}")
            
            complete_sword_swings = db.query(StatcastPitch).filter(
                StatcastPitch.description.in_(['swinging_strike', 'swinging_strike_blocked']),
                StatcastPitch.bat_speed.isnot(None),
                StatcastPitch.swing_path_tilt.isnot(None),
                StatcastPitch.intercept_ball_minus_batter_pos_y_inches.isnot(None)
            ).count()
            logger.info(f"Complete authentic sword swings ready for analysis: {complete_sword_swings:,}")
            
    except Exception as e:
        logger.error(f"Error during authentic data import: {str(e)}")
        raise

def safe_int(value):
    """Safely convert to int"""
    if pd.isna(value) or value == '' or value == 'game_date':
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None

def safe_float(value):
    """Safely convert to float"""
    if pd.isna(value) or value == '' or value == 'game_date':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_str(value):
    """Safely convert to string"""
    if pd.isna(value) or value == '' or value == 'game_date':
        return None
    return str(value)

if __name__ == "__main__":
    import_complete_authentic_data()