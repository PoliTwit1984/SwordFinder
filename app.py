import os
import logging
import requests
import threading
import time as time_module
import pandas as pd
from flask import Flask, request, jsonify, render_template, redirect, url_for, render_template_string
from datetime import datetime, timedelta
import traceback
from bs4 import BeautifulSoup
from pybaseball import statcast
from sqlalchemy import create_engine, text
from simple_db_swordfinder import SimpleDatabaseSwordFinder
from models_complete import create_tables

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Create the Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")

# Initialize database tables
create_tables()

# Initialize database-powered sword finder with your authentic MLB data
db_sword_finder = SimpleDatabaseSwordFinder()  # Uses your 226,833 authentic records

# Global status tracking for patch process
patch_status = {
    "status": "Idle",
    "rows_scanned": 0,
    "rows_updated": 0,
    "current_processing": "",
    "start_time": None,
    "elapsed_time": 0,
    "error_message": "",
    "total_expected": 0
}

@app.before_request
def force_https():
    """Redirect all HTTP traffic to HTTPS"""
    if (request.headers.get('X-Forwarded-Proto', 'http') == 'http' and 
        not app.debug and 
        'localhost' not in request.host and 
        '127.0.0.1' not in request.host):
        return redirect(request.url.replace('http://', 'https://'), code=301)

def get_video_url_from_sporty_page(play_id, max_retries=3):
    """
    Extract the direct MP4 download URL from a Baseball Savant sporty-videos page
    Based on proven solution from GitHub
    
    Args:
        play_id (str): The UUID playId for the pitch
        max_retries (int): Number of retry attempts
        
    Returns:
        str: Direct MP4 URL if found, None otherwise
    """
    attempt = 0
    while attempt < max_retries:
        try:
            page_url = f"https://baseballsavant.mlb.com/sporty-videos?playId={play_id}"
            logger.debug(f"Extracting MP4 from: {page_url} (attempt {attempt + 1})")
            
            response = requests.get(page_url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            video_container = soup.find('div', class_='video-box')
            
            if video_container:
                video_tag = video_container.find('video')
                if video_tag:
                    source_tag = video_tag.find('source', {'type': 'video/mp4'})
                    if source_tag and source_tag.get('src'):
                        mp4_url = source_tag.get('src')
                        logger.info(f"Found MP4 URL for playId {play_id}: {mp4_url}")
                        return mp4_url
            
            logger.warning(f"No video URL found for playId {play_id} on attempt {attempt + 1}")
            attempt += 1
            if attempt < max_retries:
                import time
                time.sleep(2)  # Wait before retry
                
        except Exception as e:
            logger.warning(f"Error extracting MP4 from sporty page for playId {play_id} on attempt {attempt + 1}: {str(e)}")
            attempt += 1
            if attempt < max_retries:
                import time
                time.sleep(2)
    
    return None

def get_best_video_url(play_id):
    """
    Try different video types in order and return the best available video URL
    
    Args:
        play_id (str): The UUID playId for the pitch
        
    Returns:
        dict: Contains playId, video_type, and video_url if successful, None otherwise
    """
    video_types = ["HOME", "AWAY", "NETWORK"]
    
    for video_type in video_types:
        video_url = f"https://baseballsavant.mlb.com/sporty-videos?playId={play_id}&videoType={video_type}"
        
        try:
            logger.debug(f"Checking video URL: {video_url}")
            response = requests.get(video_url, timeout=10)
            
            if response.status_code == 200:
                logger.info(f"Found working video: {video_type} for playId {play_id}")
                return {
                    "playId": play_id,
                    "video_type": video_type,
                    "video_url": video_url
                }
            else:
                logger.debug(f"Video type {video_type} returned status {response.status_code}")
                
        except requests.RequestException as e:
            logger.warning(f"Error checking video type {video_type}: {str(e)}")
            continue
    
    logger.warning(f"No working video found for playId {play_id}")
    return None

@app.route('/')
def home():
    """Serve the main SwordFinder application"""
    return render_template('home.html')

@app.route('/docs')
def docs():
    """Serve the API documentation and testing interface"""
    return render_template('docs.html')

@app.route('/swords', methods=['POST'])
def find_swords():
    """
    Main API endpoint to find sword swings for a given date
    
    Expected JSON payload:
    {
        "date": "YYYY-MM-DD"
    }
    
    Returns:
    {
        "success": true,
        "data": [
            {
                "player_name": "Name",
                "pitch_type": "SL",
                "bat_speed": 45.8,
                "intercept_y": 23.2,
                "sword_score": 96.4,
                "play_id": "...",
                "game_pk": ...
            }
        ],
        "count": 5,
        "date": "2024-01-01"
    }
    """
    try:
        # Validate request content type
        if not request.is_json:
            return jsonify({
                "success": False,
                "error": "Content-Type must be application/json"
            }), 400
        
        data = request.get_json()
        
        # Validate required fields
        if not data or 'date' not in data:
            return jsonify({
                "success": False,
                "error": "Missing required field: 'date'"
            }), 400
        
        date_str = data['date']
        
        # Validate date format
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({
                "success": False,
                "error": "Invalid date format. Use YYYY-MM-DD"
            }), 400
        
        # Check if date is not in the future
        if date_obj.date() > datetime.now().date():
            return jsonify({
                "success": False,
                "error": "Cannot analyze future dates"
            }), 400
        
        logger.info(f"Processing sword swing analysis for date: {date_str}")
        
        # Use database-powered version with complete authentic MLB data
        result = db_sword_finder.find_sword_swings(date_str)
        sword_swings = result.get('data', [])
        
        return jsonify({
            "success": True,
            "data": sword_swings,
            "count": len(sword_swings),
            "date": date_str
        })
        
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        logger.error(traceback.format_exc())
        
        return jsonify({
            "success": False,
            "error": f"Internal server error: {str(e)}"
        }), 500

@app.route('/playid', methods=['POST'])
def get_play_id():
    """
    Look up playId for a specific pitch using MLB Stats API
    
    Expected JSON payload:
    {
        "game_pk": 777788,
        "pitch_number": 4,
        "inning": 2
    }
    
    Returns:
    {
        "success": true,
        "playId": "0c0bea6e-cfce-326c-b224-4840f872c7c8",
        "video_url": "https://baseballsavant.mlb.com/sporty-videos?playId=...&videoType=AWAY"
    }
    """
    try:
        # Validate request content type
        if not request.is_json:
            return jsonify({
                "success": False,
                "error": "Content-Type must be application/json"
            }), 400
        
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['game_pk', 'pitch_number', 'inning']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    "success": False,
                    "error": f"Missing required field: '{field}'"
                }), 400
        
        game_pk = data['game_pk']
        pitch_number = data['pitch_number']
        inning = data['inning']
        
        logger.info(f"Looking up playId for game {game_pk}, inning {inning}, pitch {pitch_number}")
        
        # Call MLB Stats API
        mlb_api_url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
        
        try:
            response = requests.get(mlb_api_url, timeout=10)
            response.raise_for_status()
            game_data = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch game data from MLB API: {e}")
            return jsonify({
                "success": False,
                "error": f"Failed to fetch game data: {str(e)}"
            }), 500
        
        # Parse liveData.plays.allPlays
        try:
            all_plays = game_data['liveData']['plays']['allPlays']
            # Debug: log the structure of the first play
            if len(all_plays) > 0:
                first_play = all_plays[0]
                logger.debug(f"First play keys: {list(first_play.keys())}")
                logger.debug(f"First play sample: {dict(list(first_play.items())[:5])}")
        except KeyError as e:
            logger.error(f"Unexpected MLB API response structure: {e}")
            return jsonify({
                "success": False,
                "error": "Unexpected game data structure from MLB API"
            }), 500
        
        # Search for matching pitch
        logger.debug(f"Searching through {len(all_plays)} plays")
        
        for play_idx, play in enumerate(all_plays):
            # Look for the correct UUID playId - might be nested differently
            play_id = play.get('playId')
            uuid_play_id = play.get('playGuid') or play.get('uuid') or play.get('guid')
            play_events = play.get('playEvents', [])
            play_about = play.get('about', {})
            play_inning = play_about.get('inning')
            
            logger.debug(f"Play {play_idx}: playId={play_id}, uuidPlayId={uuid_play_id}, inning={play_inning}")
            
            # If we find events, also log their structure for debugging
            if play_idx < 3 and len(play_events) > 0:  # Log first few plays for debugging
                logger.debug(f"Play {play_idx} structure: {list(play.keys())}")
                if play_events:
                    logger.debug(f"First event structure: {list(play_events[0].keys())}")
            
            # Only check plays from the target inning for efficiency
            if play_inning == inning:
                logger.debug(f"Checking play {play_idx} in inning {play_inning} with {len(play_events)} events")
                
                for event_idx, event in enumerate(play_events):
                    # Check if this event matches our criteria
                    event_pitch_number = event.get('pitchNumber')
                    
                    logger.debug(f"Event {event_idx}: play_inning={play_inning}, pitchNumber={event_pitch_number}")
                    
                    # Match based on play inning and event pitch number
                    if play_inning == inning and event_pitch_number == pitch_number:
                        # Optional: verify it's a swinging strike
                        description = event.get('details', {}).get('description', '')
                        
                        # Look for the correct UUID playId in various possible locations
                        uuid_play_id = (
                            play.get('playId') or 
                            event.get('playId') or
                            event.get('uuid') or
                            event.get('guid') or
                            event.get('playGuid') or
                            play.get('about', {}).get('playId') or
                            event.get('about', {}).get('playId')
                        )
                        
                        logger.info(f"Found matching pitch: numericId={play_id}, uuidPlayId={uuid_play_id}, description={description}")
                        logger.debug(f"Event keys for debugging: {list(event.keys())}")
                        
                        # Use the UUID if found, otherwise fall back to numeric
                        final_play_id = uuid_play_id if uuid_play_id else play_id
                        
                        # Get the best available video URL and type
                        video_info = get_best_video_url(final_play_id)
                        
                        response_data = {
                            "success": True,
                            "playId": final_play_id,
                            "numeric_id": play_id,
                            "uuid_id": uuid_play_id,
                            "description": description,
                            "game_pk": game_pk,
                            "inning": inning,
                            "pitch_number": pitch_number
                        }
                        
                        if video_info:
                            response_data.update({
                                "video_type": video_info["video_type"],
                                "video_url": video_info["video_url"]
                            })
                            
                            # Extract the direct MP4 download URL
                            download_url = get_video_url_from_sporty_page(final_play_id)
                            response_data["download_url"] = download_url
                        else:
                            # Try without video type as fallback
                            fallback_url = f"https://baseballsavant.mlb.com/sporty-videos?playId={final_play_id}"
                            download_url = get_video_url_from_sporty_page(final_play_id)
                            
                            response_data.update({
                                "video_type": None,
                                "video_url": fallback_url,
                                "download_url": download_url,
                                "note": "Using fallback video URL"
                            })
                        
                        return jsonify(response_data)
        
        # No matching pitch found
        logger.warning(f"No matching pitch found for game {game_pk}, inning {inning}, pitch {pitch_number}")
        return jsonify({
            "success": False,
            "error": f"No pitch found for game {game_pk}, inning {inning}, pitch number {pitch_number}"
        }), 404
        
    except Exception as e:
        logger.error(f"Error in get_play_id: {str(e)}")
        logger.error(traceback.format_exc())
        
        return jsonify({
            "success": False,
            "error": f"Internal server error: {str(e)}"
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "SwordFinder API",
        "version": "1.0.0"
    })

@app.route('/patch-monitor', methods=['GET'])
def patch_monitor():
    """Database patch control center - integrated version"""
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>🗡️ Database Patch Control</title>
        <meta http-equiv="refresh" content="3">
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; background: #1a1a1a; color: #fff; }
            .container { max-width: 800px; margin: 0 auto; }
            .status-card { background: #2d2d2d; padding: 20px; border-radius: 10px; margin: 20px 0; }
            .status-running { border-left: 5px solid #4CAF50; }
            .status-idle { border-left: 5px solid #FFC107; }
            .status-error { border-left: 5px solid #F44336; }
            .status-completed { border-left: 5px solid #2196F3; }
            .progress-bar { background: #444; height: 20px; border-radius: 10px; overflow: hidden; }
            .progress-fill { background: #4CAF50; height: 100%; transition: width 0.3s; }
            .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
            .stat { background: #333; padding: 15px; border-radius: 8px; text-align: center; }
            .stat-value { font-size: 2em; font-weight: bold; color: #4CAF50; }
            .button { background: #4CAF50; color: white; padding: 12px 24px; border: none; border-radius: 5px; cursor: pointer; font-size: 16px; }
            .button:disabled { background: #666; cursor: not-allowed; }
            .log { background: #1e1e1e; padding: 15px; border-radius: 8px; font-family: monospace; font-size: 14px; max-height: 300px; overflow-y: auto; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🗡️ SwordFinder Database Patch Monitor</h1>
            
            <div class="status-card status-{{ status.status.lower() }}">
                <h2>Status: {{ status.status }}</h2>
                <p><strong>Current Task:</strong> {{ status.current_processing or "Ready to patch database" }}</p>
                <p><strong>Elapsed Time:</strong> {{ status.elapsed_time }}s</p>
                
                {% if status.total_expected > 0 %}
                <div class="progress-bar">
                    <div class="progress-fill" style="width: {{ (status.rows_scanned / status.total_expected * 100) if status.total_expected > 0 else 0 }}%"></div>
                </div>
                <p>{{ status.rows_scanned }} / {{ status.total_expected }} rows processed</p>
                {% endif %}
            </div>

            <div class="stats">
                <div class="stat">
                    <div class="stat-value">{{ status.rows_scanned }}</div>
                    <div>Rows Scanned</div>
                </div>
                <div class="stat">
                    <div class="stat-value">{{ status.rows_updated }}</div>
                    <div>Rows Updated</div>
                </div>
                <div class="stat">
                    <div class="stat-value">{{ "%.1f"|format(status.rows_updated / status.rows_scanned * 100) if status.rows_scanned > 0 else 0 }}%</div>
                    <div>Update Rate</div>
                </div>
            </div>

            <div style="margin: 30px 0;">
                <form action="/start-patch" method="post" style="display: inline;">
                    <button type="submit" class="button" {{ "disabled" if status.status == "Running" else "" }}>
                        Start Database Patch
                    </button>
                </form>
            </div>

            {% if status.error_message %}
            <div class="status-card status-error">
                <h3>Error Details</h3>
                <div class="log">{{ status.error_message }}</div>
            </div>
            {% endif %}

            <div class="status-card">
                <h3>Process Information</h3>
                <div class="log">
                    <div>Missing Data to Fix:</div>
                    <div>• Team names: 0 records have team data (need BOS, BAL, etc.)</div>
                    <div>• Velocities: Limited records have pitch speed data</div>
                    <div>• Spin rates: 0 records have spin rate data</div>
                    <div>• Locations: Limited plate position data</div>
                    <div><br>Patch will pull fresh pybaseball data and update missing fields.</div>
                </div>
            </div>
        </div>
    </body>
    </html>
    """, status=patch_status, now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

@app.route('/start-patch', methods=['POST'])
def start_patch():
    """Start the database patching process"""
    global patch_status
    
    if patch_status["status"] == "Running":
        return jsonify({"success": False, "error": "Patch already running"}), 400
    
    # Reset status
    patch_status.update({
        "status": "Running",
        "rows_scanned": 0,
        "rows_updated": 0,
        "current_processing": "Initializing...",
        "start_time": time_module.time(),
        "error_message": ""
    })
    
    # Start patch in background thread
    thread = threading.Thread(target=run_patch_process)
    thread.daemon = True
    thread.start()
    
    return redirect(url_for('patch_monitor'))

def run_patch_process():
    """Main patching process - runs in background"""
    global patch_status
    
    try:
        database_url = os.environ.get('DATABASE_URL')
        engine = create_engine(database_url)
        
        # Define date range to patch (recent dates first)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)  # Last 7 days
        
        patch_status["current_processing"] = f"Pulling Statcast data from {start_date} to {end_date}"
        logger.info(f"Starting patch: pulling data from {start_date} to {end_date}")
        
        # Pull fresh data from pybaseball
        df = statcast(start_dt=str(start_date), end_dt=str(end_date))
        patch_status["total_expected"] = len(df)
        logger.info(f"Retrieved {len(df)} records from pybaseball")
        
        with engine.connect() as conn:
            batch_size = 500
            updated_count = 0
            
            for i in range(0, len(df), batch_size):
                if patch_status["status"] != "Running":  # Allow stopping
                    break
                    
                batch = df.iloc[i:i+batch_size]
                patch_status["current_processing"] = f"Processing batch {i//batch_size + 1} (rows {i}-{min(i+batch_size, len(df))})"
                
                for idx, row in batch.iterrows():
                    patch_status["rows_scanned"] = idx + 1
                    
                    # Build update query for missing fields
                    update_fields = []
                    params = {
                        'game_pk': int(row['game_pk']) if pd.notna(row['game_pk']) else None,
                        'player_name': str(row['player_name']) if pd.notna(row['player_name']) else None,
                        'pitch_type': str(row['pitch_type']) if pd.notna(row['pitch_type']) else None
                    }
                    
                    # Add fields that might be missing
                    if pd.notna(row['release_speed']):
                        update_fields.append("release_speed = COALESCE(release_speed, :release_speed)")
                        params['release_speed'] = float(row['release_speed'])
                    
                    if pd.notna(row['home_team']):
                        update_fields.append("home_team = COALESCE(home_team, :home_team)")
                        params['home_team'] = str(row['home_team'])
                    
                    if pd.notna(row['away_team']):
                        update_fields.append("away_team = COALESCE(away_team, :away_team)")
                        params['away_team'] = str(row['away_team'])
                    
                    if pd.notna(row['release_spin_rate']):
                        update_fields.append("release_spin_rate = COALESCE(release_spin_rate, :release_spin_rate)")
                        params['release_spin_rate'] = float(row['release_spin_rate'])
                    
                    if pd.notna(row['plate_x']):
                        update_fields.append("plate_x = COALESCE(plate_x, :plate_x)")
                        params['plate_x'] = float(row['plate_x'])
                    
                    if pd.notna(row['plate_z']):
                        update_fields.append("plate_z = COALESCE(plate_z, :plate_z)")
                        params['plate_z'] = float(row['plate_z'])
                    
                    if pd.notna(row['pitch_name']):
                        update_fields.append("pitch_name = COALESCE(pitch_name, :pitch_name)")
                        params['pitch_name'] = str(row['pitch_name'])
                    
                    # Only update if we have fields and valid keys
                    if update_fields and all(params[k] is not None for k in ['game_pk', 'player_name', 'pitch_type']):
                        update_query = text(f"""
                            UPDATE statcast_pitches 
                            SET {', '.join(update_fields)}
                            WHERE game_pk = :game_pk
                            AND player_name = :player_name
                            AND pitch_type = :pitch_type
                        """)
                        
                        result = conn.execute(update_query, params)
                        if result.rowcount > 0:
                            updated_count += result.rowcount
                            patch_status["rows_updated"] = updated_count
                
                # Commit after each batch
                conn.commit()
                logger.info(f"Batch {i//batch_size + 1} complete. Updated {updated_count} records so far.")
        
        patch_status["status"] = "Completed"
        patch_status["current_processing"] = f"Patch completed! Updated {updated_count} records"
        logger.info(f"Patch process completed successfully. Updated {updated_count} total records.")
        
    except Exception as e:
        patch_status["status"] = "Error"
        patch_status["error_message"] = str(e)
        patch_status["current_processing"] = "Error occurred during patching"
        logger.error(f"Patch process failed: {e}")
        logger.error(traceback.format_exc())

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "success": False,
        "error": "Endpoint not found"
    }), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        "success": False,
        "error": "Method not allowed"
    }), 405

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
