import os
import logging
from flask import Flask, request, jsonify, render_template
from datetime import datetime
import traceback
from swordfinder import SwordFinder

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Create the Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")

# Initialize SwordFinder
sword_finder = SwordFinder()

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
