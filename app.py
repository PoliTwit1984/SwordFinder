import os
import logging
import requests
from flask import Flask, request, jsonify, render_template
from datetime import datetime
import traceback
from bs4 import BeautifulSoup
from swordfinder import SwordFinder

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Create the Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")

# Initialize SwordFinder
sword_finder = SwordFinder()

def extract_mp4_from_sporty_page(play_id, video_type=None):
    """
    Extract the direct MP4 download URL from a Baseball Savant sporty-videos page
    
    Args:
        play_id (str): The UUID playId for the pitch
        video_type (str): Optional video type (HOME, AWAY, NETWORK)
        
    Returns:
        str: Direct MP4 URL if found, None otherwise
    """
    try:
        # Build the page URL
        if video_type:
            page_url = f"https://baseballsavant.mlb.com/sporty-videos?playId={play_id}&videoType={video_type}"
        else:
            page_url = f"https://baseballsavant.mlb.com/sporty-videos?playId={play_id}"
        
        logger.debug(f"Extracting MP4 from: {page_url}")
        
        # Load the page
        response = requests.get(page_url, timeout=15)
        response.raise_for_status()
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the video-box div and then the video tag
        video_box = soup.find('div', class_='video-box')
        if not video_box:
            logger.warning(f"No video-box div found for playId {play_id}")
            return None
        
        video_tag = video_box.find('video')
        if not video_tag:
            logger.warning(f"No video tag found in video-box for playId {play_id}")
            return None
        
        # Find the MP4 source
        mp4_source = video_tag.find('source', type='video/mp4')
        if not mp4_source:
            logger.warning(f"No MP4 source found for playId {play_id}")
            return None
        
        mp4_url = mp4_source.get('src')
        if mp4_url:
            logger.info(f"Found MP4 URL for playId {play_id}: {mp4_url}")
            return mp4_url
        else:
            logger.warning(f"MP4 source tag has no src attribute for playId {play_id}")
            return None
            
    except Exception as e:
        logger.error(f"Error extracting MP4 from sporty page for playId {play_id}: {str(e)}")
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
def index():
    """Serve the documentation and testing interface"""
    return render_template('index.html')

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
        
        # Find sword swings
        sword_swings = sword_finder.find_sword_swings(date_str)
        
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
                            download_url = extract_mp4_from_sporty_page(final_play_id, video_info["video_type"])
                            response_data["download_url"] = download_url
                        else:
                            # Try without video type as fallback
                            fallback_url = f"https://baseballsavant.mlb.com/sporty-videos?playId={final_play_id}"
                            download_url = extract_mp4_from_sporty_page(final_play_id)
                            
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
